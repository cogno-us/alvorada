from __future__ import annotations

import functools
import json
import subprocess
import sys
import os
import sqlite3
from pathlib import Path

import pytest

from experiments.governed_message_transport import (
    AcceptedGaxRecipientAdapter,
    LocalDurableTransport,
    Route,
    TrustedRouteTable,
)
from experiments.odex_gax_imx_reference.gax_ref_runtime import (
    GAX_ARTIFACT_EXPORT_VERSION,
    LocalRegistry,
    TransactionalExchangeStore,
    load_executor_runtime,
    make_message,
    parse_time,
    run_exchange,
    runtime_proposal_model,
)
from experiments.odex_gax_imx_reference.synthetic_fixture import (
    build_synthetic_resolver,
    synthetic_refund_policy,
)

EVAL = "2026-08-08T01:00:00Z"


def load_env(name: str) -> dict:
    value = os.environ.get(name)
    assert value, f"missing {name}"
    return json.loads(Path(value).read_text(encoding="utf-8"))


def fixture(tmp_path: Path, message_id: str):
    manifest = load_env("UPSTREAM_MANIFEST_EXAMPLE")
    bundle = load_env("UPSTREAM_REPLAY_SUCCESS_EXAMPLE")
    proposal = runtime_proposal_model(bundle)
    resolver = build_synthetic_resolver(proposal, now=parse_time(EVAL))
    runtime = load_executor_runtime()
    destination = runtime["DurableRefundDestination"](tmp_path / "moltbot-state")
    registry = LocalRegistry({"refund-sender"}, {"refund-recipient"}, "refund-recipient")
    message = make_message(bundle, message_id=message_id)
    return manifest, bundle, resolver, destination, registry, message


def run_once(tmp_path: Path, message_id: str, **kwargs):
    manifest, bundle, resolver, destination, registry, message = fixture(tmp_path, message_id)
    result = run_exchange(
        message,
        bundle,
        registry,
        destination,
        evaluation_time=EVAL,
        manifest=manifest,
        store_path=tmp_path / "exchange.sqlite",
        resolver=resolver,
        execution_policy_factory=synthetic_refund_policy,
        **kwargs,
    )
    return manifest, bundle, resolver, destination, registry, message, result


def effect_count(destination) -> int:
    with sqlite3.connect(destination.path) as con:
        return con.execute("SELECT count(*) FROM effects").fetchone()[0]


def test_normal_and_duplicate_return_same_retained_original_artifacts(tmp_path):
    manifest, bundle, resolver, destination, registry, message, first = run_once(
        tmp_path, "retained-original"
    )
    original = first["artifact_export"]
    assert original["export_version"] == GAX_ARTIFACT_EXPORT_VERSION
    assert original["state"] == "original_complete"
    second = run_exchange(
        message,
        bundle,
        registry,
        destination,
        evaluation_time=EVAL,
        manifest=manifest,
        store_path=tmp_path / "exchange.sqlite",
        resolver=resolver,
        execution_policy_factory=synthetic_refund_policy,
    )
    assert second["artifact_export"] == original
    assert second["current_reconstruction_bundle"]["bundle_id"] == original["producer_refs"]["reconstruction_bundle_id"]
    assert effect_count(destination) == 1
    assert second["execution"]["newly_executed"] is False


def test_evidence_export_failure_recovers_distinct_derivative_without_effect(tmp_path):
    manifest, bundle, resolver, destination, registry, message, failed = run_once(
        tmp_path, "recover-derivative", fault_evidence_once=True
    )
    assert failed["execution"]["attempt_status"] == "evidence_export_failed"
    assert effect_count(destination) == 1
    recovered = run_exchange(
        message,
        bundle,
        registry,
        destination,
        evaluation_time=EVAL,
        manifest=manifest,
        store_path=tmp_path / "exchange.sqlite",
        resolver=resolver,
        execution_policy_factory=synthetic_refund_policy,
    )
    export = recovered["artifact_export"]
    assert export["state"] == "regenerated_derivative"
    assert export["lineage"]["relationship"] == "regenerated_derivative"
    assert export["lineage"]["original_artifacts"] == "unavailable"
    assert export["lineage"]["effect_reexecution"] is False
    assert effect_count(destination) == 1
    assert recovered["execution"]["newly_executed"] is False


def test_retained_artifact_digest_substitution_fails_closed(tmp_path):
    manifest, bundle, resolver, destination, registry, message, first = run_once(
        tmp_path, "artifact-tamper"
    )
    store = TransactionalExchangeStore(tmp_path / "exchange.sqlite")
    retained = store.artifact_export_for_message(message)
    retained["reconstruction_bundle"]["bundle_id"] = "substituted"
    with sqlite3.connect(store.path) as con:
        con.execute(
            "UPDATE artifact_exports SET artifact_json=? WHERE message_id=?",
            (json.dumps(retained, sort_keys=True, separators=(",", ":")), message["message_id"]),
        )
    with pytest.raises(ValueError, match="digest"):
        store.artifact_export_for_message(message)


def routes():
    return TrustedRouteTable([
        Route(
            route_id="accepted-gax-local",
            sender_endpoint_ref="local://refund-sender",
            sender_identity_ref="urn:cognous:transport-identity:refund-sender",
            expected_sender_claim="refund-sender",
            recipient_endpoint_ref="local://refund-recipient",
            recipient_identity_ref="urn:cognous:transport-identity:refund-recipient",
            expected_recipient_claim="refund-recipient",
            configured_identity_authenticated=False,
        )
    ])


def test_transport_persists_full_original_export_and_returns_it_on_redelivery(tmp_path):
    manifest, bundle, resolver, destination, registry, message = fixture(
        tmp_path, "transport-original"
    )
    handler = AcceptedGaxRecipientAdapter(
        bundle=bundle,
        registry=registry,
        manifest=manifest,
        exchange_store_path=tmp_path / "exchange.sqlite",
        resolver=resolver,
        destination=destination,
        execution_policy_factory=synthetic_refund_policy,
    )
    transport = LocalDurableTransport(
        sender_store_path=tmp_path / "sender.sqlite",
        recipient_store_path=tmp_path / "recipient.sqlite",
        routes=routes(),
        recipient_handler=handler,
        attempt_id_factory=iter(["transport-attempt-1", "transport-attempt-2"]).__next__,
        ack_id_factory=iter(["transport-ack-1", "transport-ack-2"]).__next__,
    )
    transport.queue(
        message,
        route_id="accepted-gax-local",
        sender_endpoint_ref="local://refund-sender",
        now=EVAL,
    )
    transport.deliver(message["message_id"], now=EVAL)
    retained = transport.recipient_store.retained_result(message["message_id"])
    assert retained is not None
    assert retained.result_state == "original_complete"
    original_id = retained.artifact_export["producer_refs"]["reconstruction_bundle_id"]

    duplicate = transport.deliver(message["message_id"], now=EVAL, force=True)
    assert duplicate["transport_state"] == "DELIVERED"
    again = transport.recipient_store.retained_result(message["message_id"])
    assert again.artifact_export["producer_refs"]["reconstruction_bundle_id"] == original_id
    assert again.artifact_export == retained.artifact_export
    assert effect_count(destination) == 1


def test_supported_runtime_import_does_not_require_upstream_test_directories(tmp_path):
    runtime = load_executor_runtime()
    assert runtime["PinnedControlPlaneExecutor"].__module__ == "engine.control_plane_adapter"
    assert runtime["producer"].__name__ == "engine.producer_contract"
    # The supported loader resolves only public module roots; it never opens or
    # imports tests/test_safe_executor.py or Control Plane test fixture modules.
    import inspect
    source = inspect.getsource(load_executor_runtime)
    assert "tests/" not in source
    assert "test_safe_executor" not in source



def _accepted_transport(tmp_path: Path, message_id: str, *, exchange_options=None):
    manifest, bundle, resolver, destination, registry, message = fixture(
        tmp_path, message_id
    )
    handler = AcceptedGaxRecipientAdapter(
        bundle=bundle,
        registry=registry,
        manifest=manifest,
        exchange_store_path=tmp_path / "exchange.sqlite",
        resolver=resolver,
        destination=destination,
        execution_policy_factory=synthetic_refund_policy,
    )
    if exchange_options:
        handler._run_exchange = functools.partial(
            handler._run_exchange, **exchange_options
        )
    ids = iter(
        [
            f"{message_id}-transport-attempt-1",
            f"{message_id}-transport-attempt-2",
            f"{message_id}-transport-attempt-3",
        ]
    )
    acks = iter(
        [
            f"{message_id}-transport-ack-1",
            f"{message_id}-transport-ack-2",
            f"{message_id}-transport-ack-3",
        ]
    )
    transport = LocalDurableTransport(
        sender_store_path=tmp_path / "sender.sqlite",
        recipient_store_path=tmp_path / "recipient.sqlite",
        routes=routes(),
        recipient_handler=handler,
        max_attempts=3,
        base_backoff_seconds=1,
        attempt_id_factory=ids.__next__,
        ack_id_factory=acks.__next__,
    )
    transport.queue(
        message,
        route_id="accepted-gax-local",
        sender_endpoint_ref="local://refund-sender",
        now=EVAL,
    )
    return transport, destination, message, handler


def test_transport_public_interface_returns_original_artifacts_and_commitments(tmp_path):
    transport, destination, message, _ = _accepted_transport(
        tmp_path, "public-original"
    )
    delivered = transport.deliver(message["message_id"], now=EVAL)
    assert delivered["transport_state"] == "DELIVERED"
    assert effect_count(destination) == 1

    retained = transport.retained_artifacts(message["message_id"])
    assert retained["result_state"] == "original_complete"
    export = retained["artifact_export"]
    assert export["reconstruction_bundle"]["bundle_id"] == (
        export["producer_refs"]["reconstruction_bundle_id"]
    )
    assert export["content_commitments"]["reconstruction_bundle"] == (
        export["producer_refs"]["reconstruction_digest"]
    )
    assert export["content_commitments"]["odes_package"] == (
        export["producer_refs"]["odes_package_digest"]
    )
    assert export["content_commitments"]["recipient_validation"] == (
        export["producer_refs"]["odes_validation_digest"]
    )
    assert export["content_commitments"]["successor_packet"] == (
        export["producer_refs"]["successor_packet_digest"]
    )
    assert export["producer_refs"]["effect_id"] == (
        next(iter(_effect_ids(destination)))
    )
    assert export["producer_refs"]["attempt_identity"]["namespace"] == "executor"

    duplicate = transport.deliver(message["message_id"], now=EVAL, force=True)
    assert duplicate["transport_state"] == "DELIVERED"
    assert transport.retained_artifacts(message["message_id"]) == retained
    assert effect_count(destination) == 1


def _effect_ids(destination):
    with sqlite3.connect(destination.path) as con:
        return {
            row[0]
            for row in con.execute("SELECT effect_id FROM effects ORDER BY rowid")
        }


@pytest.mark.parametrize(
    "exchange_options,initial_status",
    [
        ({"fault_after_dispatch": True}, "interrupted_after_dispatch"),
        ({"fault_evidence_once": True}, "evidence_export_failed"),
    ],
)
def test_transport_recovers_evidence_only_after_post_effect_failure(
    tmp_path, exchange_options, initial_status
):
    transport, destination, message, _ = _accepted_transport(
        tmp_path, f"recover-{initial_status}", exchange_options=exchange_options
    )
    first = transport.deliver(message["message_id"], now=EVAL)
    assert first["transport_state"] == "PENDING_RETRY"
    assert first["acknowledgement"]["detail"]["recoverable"] is True
    assert effect_count(destination) == 1
    incomplete = transport.retained_artifacts(message["message_id"])
    assert incomplete["result_state"] == "recovery_required"
    assert incomplete["artifact_export"] is None

    recovered = transport.recover_due(now="2026-08-08T01:00:02Z")
    assert recovered[0]["transport_state"] == "DELIVERED"
    retained = transport.retained_artifacts(message["message_id"])
    assert retained["result_state"] == "regenerated_derivative"
    export = retained["artifact_export"]
    assert export["lineage"]["relationship"] == "regenerated_derivative"
    assert export["lineage"]["original_artifacts"] == "unavailable"
    assert export["lineage"]["effect_reexecution"] is False
    assert effect_count(destination) == 1


def test_transport_partial_delivery_retains_original_partial_artifacts(tmp_path):
    transport, destination, message, _ = _accepted_transport(
        tmp_path, "partial-public", exchange_options={"partial_delivery": True}
    )
    delivered = transport.deliver(message["message_id"], now=EVAL)
    assert delivered["transport_state"] == "DELIVERED"
    assert effect_count(destination) == 1
    retained = transport.retained_artifacts(message["message_id"])
    assert retained["result_state"] == "original_complete"
    assert retained["artifact_export"]["lineage"]["relationship"] == "original"
    assert retained["artifact_export"]["successor_packet"]["unresolved_delivery"] is True


def test_transport_denied_outcome_retains_evidence_without_effect(tmp_path):
    manifest, bundle, resolver, destination, registry, message = fixture(
        tmp_path, "denied-public"
    )
    grant_id = next(iter(resolver.statuses))
    resolver.statuses[grant_id].status = "revoked"
    handler = AcceptedGaxRecipientAdapter(
        bundle=bundle,
        registry=registry,
        manifest=manifest,
        exchange_store_path=tmp_path / "exchange.sqlite",
        resolver=resolver,
        destination=destination,
        execution_policy_factory=synthetic_refund_policy,
    )
    transport = LocalDurableTransport(
        sender_store_path=tmp_path / "sender.sqlite",
        recipient_store_path=tmp_path / "recipient.sqlite",
        routes=routes(),
        recipient_handler=handler,
        attempt_id_factory=lambda: "denied-transport-attempt",
        ack_id_factory=lambda: "denied-transport-ack",
    )
    transport.queue(
        message,
        route_id="accepted-gax-local",
        sender_endpoint_ref="local://refund-sender",
        now=EVAL,
    )
    delivered = transport.deliver(message["message_id"], now=EVAL)
    assert delivered["transport_state"] == "DELIVERED"
    assert effect_count(destination) == 0
    retained = transport.retained_artifacts(message["message_id"])
    assert retained["artifact_export"]["state"] == "original_complete"
    assert retained["producer_refs"]["decision_id"]


def test_transport_retained_artifact_tamper_is_rejected(tmp_path):
    transport, destination, message, _ = _accepted_transport(
        tmp_path, "transport-tamper"
    )
    assert transport.deliver(message["message_id"], now=EVAL)["transport_state"] == "DELIVERED"
    assert effect_count(destination) == 1

    with sqlite3.connect(transport.recipient_store.path) as con:
        row = con.execute(
            "SELECT artifact_export_json FROM recipient_results WHERE message_id=?",
            (message["message_id"],),
        ).fetchone()
        artifact = json.loads(row[0])
        artifact["reconstruction_bundle"]["bundle_id"] = "tampered"
        con.execute(
            "UPDATE recipient_results SET artifact_export_json=? WHERE message_id=?",
            (
                json.dumps(artifact, sort_keys=True, separators=(",", ":")),
                message["message_id"],
            ),
        )

    with pytest.raises(ValueError, match="digest"):
        transport.retained_artifacts(message["message_id"])


def test_transport_legacy_result_without_artifacts_reports_unavailable(tmp_path):
    transport, destination, message, _ = _accepted_transport(
        tmp_path, "legacy-artifactless"
    )
    assert transport.deliver(message["message_id"], now=EVAL)["transport_state"] == "DELIVERED"
    assert effect_count(destination) == 1

    with sqlite3.connect(transport.recipient_store.path) as con:
        con.execute(
            "DELETE FROM recipient_results WHERE message_id=?",
            (message["message_id"],),
        )

    retained = transport.retained_artifacts(message["message_id"])
    assert retained["result_state"] == "historical_artifacts_unavailable"
    assert retained["artifact_export"] is None
    assert effect_count(destination) == 1


def test_runtime_public_modules_execute_with_tests_namespace_blocked(tmp_path):
    code = r"""
import importlib.abc
import json
import os
import sys
from pathlib import Path

class BlockTests(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "tests" or fullname.startswith("tests."):
            raise ImportError("test namespace unavailable")
        return None

sys.meta_path.insert(0, BlockTests())

from experiments.odex_gax_imx_reference.gax_ref_runtime import load_executor_runtime
runtime = load_executor_runtime()
producer = runtime["producer"]
payload = {"refund_reason": "blocked-tests-runtime"}
op = runtime["ExecutionOperation"](
    actor="urn:cognous:identity:agent-1",
    principal="urn:cognous:principal:service",
    institution_id="urn:cognous:institution:demo",
    authority_domain="customer-refunds",
    manifest_id="refund-integration-v1-1",
    manifest_version="1.1",
    manifest_digest="sha256:" + "a" * 64,
    proposal_commitment="sha256:" + "b" * 64,
    action_id="refund.issue",
    adapter_id="urn:cognous:adapter:synthetic-refund",
    target="urn:cognous:synthetic-account:customer-1",
    payload=payload,
    payload_commitment=producer.commitment(payload),
    requested_permissions=("refund.issue",),
    amount=50.0,
    unit="USD",
    effects=1,
    authority_context_id="urn:cognous:authority-context:refund-demo",
    requirement_id="urn:cognous:requirement:refund-t1",
    grant_id="urn:cognous:grant:refund-1",
    grant_revision="1",
    effective_max_effects=1,
)
destination = runtime["DurableRefundDestination"](Path(os.environ["BLOCKED_TESTS_STATE"]))
from engine.safe_executor import LocalDestinationExecutor, snapshot_envelope
policy = runtime["LocalExecutionPolicy"](
    allowed_institutions=frozenset({op.institution_id}),
    allowed_authority_domains=frozenset({op.authority_domain}),
    allowed_adapters=frozenset({op.adapter_id}),
    allowed_actions=frozenset({op.action_id}),
    allowed_target_prefixes=("urn:cognous:synthetic-account:",),
    allowed_units=frozenset({"USD"}),
    max_amount=1000.0,
    max_effects=1,
)
envelope = runtime["ExecutionEnvelope"](
    producer.EXECUTION_ENVELOPE_VERSION,
    "blocked-tests-decision",
    "blocked-tests-effect",
    op,
)
result = LocalDestinationExecutor(destination, policy).execute_snapshot(
    snapshot_envelope(envelope)
)
assert result.status == "executed"
assert destination.effect_count(op.grant_id) == 1
assert all(not name.startswith("tests.") for name in sys.modules)
"""
    env = os.environ.copy()
    env["BLOCKED_TESTS_STATE"] = str(tmp_path / "blocked-tests-state")
    completed = subprocess.run(
        [sys.executable, "-c", code],
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr



def test_retained_replay_passes_evidence_pack_and_odes_integrity_without_assurance_inflation(tmp_path):
    from agent_governance_evidence_pack import (
        import_manifest_reconstruction,
        render_traceable_markdown,
        validate_evidence_pack,
    )

    transport, destination, message, _ = _accepted_transport(
        tmp_path, "accepted-consumer-chain"
    )
    delivered = transport.deliver(message["message_id"], now=EVAL)
    assert delivered["transport_state"] == "DELIVERED"
    assert effect_count(destination) == 1

    retained = transport.retained_artifacts(message["message_id"])
    assert retained["result_state"] == "original_complete"
    export = retained["artifact_export"]
    replay = export["reconstruction_bundle"]
    odes = export["odes"]
    successor = export["successor_packet"]

    assert replay["bundle_id"] == export["producer_refs"]["reconstruction_bundle_id"]
    assert export["content_commitments"]["reconstruction_bundle"] == export["producer_refs"]["reconstruction_digest"]
    assert export["content_commitments"]["odes_package"] == export["producer_refs"]["odes_package_digest"]
    assert export["content_commitments"]["recipient_validation"] == export["producer_refs"]["odes_validation_digest"]
    assert successor["packet_id"] == export["producer_refs"]["successor_packet_id"]
    assert successor["packet_digest"] == export["producer_refs"]["successor_packet_digest"]
    assert export["content_commitments"]["successor_packet"] == successor["packet_digest"]

    pack = import_manifest_reconstruction(_load_env("UPSTREAM_MANIFEST_EXAMPLE"), replay)
    report = validate_evidence_pack(pack)
    assert report.valid is True
    rendered = render_traceable_markdown(pack)
    trace = pack.metadata["traceable_import"]

    assert trace["supported_revisions"]["replay"] == "f63ce914504dd06813c4ccd199b0570dbd8dd427"
    assert trace["supported_revisions"]["moltbot_safe"] == "1d308faf664c504b6e310db3c7a310153ef7b067"
    assert trace["supported_revisions"]["odes"] == "cba83a1c06f718a8afd76178f36e5cc15896347d"
    assert trace["lifecycle_summary"]["independent_verification"] == "unavailable"
    assert trace["lifecycle_summary"]["current_permission"] == "not_evaluated_from_historical_records"
    assert "does not authenticate the producer" in rendered

    recipient = odes["recipient_validation"]
    assert recipient["schema_validity"]["status"] == "pass"
    assert recipient["package_content_integrity"]["status"] == "pass"
    assert recipient["integrity_authentication_checks"]["status"] == "unavailable"
    assert recipient["authority_status_and_freshness"]["status"] == "unavailable"
    assert recipient["recipient_reliance_decision"]["status"] in {"fail", "informational_only"}

    duplicate = transport.deliver(message["message_id"], now=EVAL, force=True)
    assert duplicate["transport_state"] == "DELIVERED"
    assert effect_count(destination) == 1
    assert transport.retained_artifacts(message["message_id"]) == retained
