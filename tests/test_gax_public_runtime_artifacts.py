from __future__ import annotations

import json
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
        attempt_id_factory=lambda: "transport-attempt",
        ack_id_factory=lambda: "transport-ack",
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
