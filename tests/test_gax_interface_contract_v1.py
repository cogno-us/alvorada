from __future__ import annotations

import copy
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
    LocalRegistry,
    TransactionalExchangeStore,
    _build_resolver,
    _validate_artifact_result,
    load_public_executor_runtime,
    make_message,
    parse_time,
    run_exchange,
    runtime_proposal_model,
)

EVAL = "2026-08-08T01:00:00Z"


def load_json_env(name: str) -> dict:
    return json.loads(Path(os.environ[name]).read_text(encoding="utf-8"))


def setup_runtime(tmp_path):
    bundle = load_json_env("UPSTREAM_REPLAY_SUCCESS_EXAMPLE")
    manifest = load_json_env("UPSTREAM_MANIFEST_EXAMPLE")
    proposal = runtime_proposal_model(bundle)
    resolver = _build_resolver(proposal, now=parse_time(EVAL))
    h = load_public_executor_runtime()
    destination = h.DurableRefundDestination(tmp_path / "destination")
    registry = LocalRegistry({"refund-sender"}, {"refund-recipient"}, "refund-recipient")
    return bundle, manifest, resolver, destination, registry


def effect_count(destination) -> int:
    with sqlite3.connect(destination.path) as conn:
        return conn.execute("SELECT COUNT(*) FROM effects").fetchone()[0]


def routes():
    return TrustedRouteTable([
        Route(
            route_id="accepted",
            sender_endpoint_ref="local://sender",
            sender_identity_ref="urn:test:sender",
            expected_sender_claim="refund-sender",
            recipient_endpoint_ref="local://recipient",
            recipient_identity_ref="urn:test:recipient",
            expected_recipient_claim="refund-recipient",
            configured_identity_authenticated=False,
        )
    ])


def test_supported_runtime_imports_do_not_load_executor_test_helpers():
    h = load_public_executor_runtime()
    assert h.PinnedControlPlaneExecutor.__module__ == "engine.control_plane_adapter"
    assert h.DurableRefundDestination.__module__ == "engine.safe_executor"
    assert h.export_execution_producer_record.__module__ == "engine.producer_profile"
    for value in (
        h.PinnedControlPlaneExecutor,
        h.DurableRefundDestination,
        h.export_execution_producer_record,
    ):
        source = Path(__import__(value.__module__, fromlist=["x"]).__file__).as_posix()
        assert "/tests/" not in source


def test_original_artifacts_are_retained_and_duplicate_returns_same_objects(tmp_path):
    bundle, manifest, resolver, destination, registry = setup_runtime(tmp_path)
    message = make_message(bundle, message_id="retain-original")
    store_path = tmp_path / "exchange.sqlite"
    first = run_exchange(
        message, bundle, registry, destination,
        evaluation_time=EVAL, manifest=manifest, store_path=store_path, resolver=resolver,
    )
    retained = first["artifact_result"]
    assert retained["original_artifacts"] is True
    assert retained["retention_state"] == "complete_original"
    assert effect_count(destination) == 1

    second = run_exchange(
        message, bundle, registry, destination,
        evaluation_time=EVAL, manifest=manifest, store_path=store_path, resolver=resolver,
    )
    assert second["artifact_result"] == retained
    assert second["current_reconstruction_bundle"] == retained["reconstruction_bundle"]
    assert second["odes_reference"] == retained["odes_reference"]
    assert second["successor_packet"] == retained["successor_packet"]
    assert effect_count(destination) == 1


def test_interruption_after_effect_before_artifact_persistence_regenerates_derivative_only(tmp_path):
    bundle, manifest, resolver, destination, registry = setup_runtime(tmp_path)
    message = make_message(bundle, message_id="interrupted-retention")
    store_path = tmp_path / "exchange.sqlite"
    first = run_exchange(
        message, bundle, registry, destination,
        evaluation_time=EVAL, manifest=manifest, store_path=store_path, resolver=resolver,
        fault_after_dispatch=True,
    )
    assert first["execution"]["attempt_status"] == "interrupted_after_dispatch"
    assert effect_count(destination) == 1
    assert TransactionalExchangeStore(store_path).artifact_result_for_message(message) is None

    second = run_exchange(
        message, bundle, registry, destination,
        evaluation_time=EVAL, manifest=manifest, store_path=store_path, resolver=resolver,
    )
    retained = second["artifact_result"]
    assert retained["original_artifacts"] is False
    assert retained["retention_state"] == "complete_regenerated_derivative"
    assert retained["lineage"]["relationship"] == "regenerated_from_retained_source_records"
    assert retained["lineage"]["replacement_effect_executed"] is False
    assert effect_count(destination) == 1


def test_retained_artifact_digest_substitution_rejected(tmp_path):
    bundle, manifest, resolver, destination, registry = setup_runtime(tmp_path)
    message = make_message(bundle, message_id="tamper-artifact")
    result = run_exchange(
        message, bundle, registry, destination,
        evaluation_time=EVAL, manifest=manifest,
        store_path=tmp_path / "exchange.sqlite", resolver=resolver,
    )
    changed = copy.deepcopy(result["artifact_result"])
    changed["reconstruction_bundle"]["bundle_id"] = "substituted"
    with pytest.raises(ValueError, match="digest|identity"):
        _validate_artifact_result(changed)


def test_transport_inbox_durably_retains_original_artifact_result(tmp_path):
    bundle, manifest, resolver, destination, registry = setup_runtime(tmp_path)
    handler = AcceptedGaxRecipientAdapter(
        bundle=bundle,
        registry=registry,
        manifest=manifest,
        exchange_store_path=tmp_path / "exchange.sqlite",
        resolver=resolver,
        destination=destination,
    )
    transport = LocalDurableTransport(
        sender_store_path=tmp_path / "sender.sqlite",
        recipient_store_path=tmp_path / "recipient.sqlite",
        routes=routes(),
        recipient_handler=handler,
        attempt_id_factory=lambda: "attempt-1",
        ack_id_factory=lambda: "ack-1",
    )
    message = make_message(bundle, message_id="transport-artifacts")
    transport.queue(message, route_id="accepted", sender_endpoint_ref="local://sender", now=EVAL)
    delivered = transport.deliver(message["message_id"], now=EVAL)
    assert delivered["transport_state"] == "DELIVERED"
    row = transport.recipient_store.inbox_record(message["message_id"])
    artifacts = json.loads(row["artifacts_json"])
    assert artifacts["result_profile_version"] == "1.0.0"
    assert artifacts["original_artifacts"] is True
    assert artifacts["reconstruction_bundle"]["bundle_id"] == artifacts["producer_refs"]["reconstruction_bundle_id"]
