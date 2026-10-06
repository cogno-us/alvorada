from __future__ import annotations

import copy
import json
import os
from pathlib import Path

import pytest

from experiments.odex_gax_imx_reference.gax_ref_runtime import (
    LocalRegistry,
    load_executor_runtime,
    actual_executor_classes,
    assess_message,
    digest,
    load_successor_packet,
    make_message,
    make_successor_packet,
    parse_time,
    run_actual_outcome,
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


def manifest() -> dict:
    return load_env("UPSTREAM_MANIFEST_EXAMPLE")


def success_bundle() -> dict:
    return load_env("UPSTREAM_REPLAY_SUCCESS_EXAMPLE")


def registry() -> LocalRegistry:
    return LocalRegistry({"refund-sender"}, {"refund-recipient"}, "refund-recipient")


def _runtime_fixture(tmp_path: Path, bundle: dict | None = None):
    bundle = bundle or success_bundle()
    proposal = runtime_proposal_model(bundle)
    resolver = build_synthetic_resolver(proposal, now=parse_time(EVAL))
    runtime = load_executor_runtime()
    destination = runtime["DurableRefundDestination"](tmp_path / "moltbot-state")
    return resolver, destination


def _effect_rows(destination):
    import sqlite3

    with sqlite3.connect(destination.path) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute("SELECT * FROM effects ORDER BY rowid")]


def test_imports_actual_pinned_moltbot_executor_classes():
    classes = actual_executor_classes()
    assert classes["PinnedControlPlaneExecutor"].__module__ == "engine.control_plane_adapter"
    assert classes["DurableRefundDestination"].__module__ == "engine.safe_executor"


def test_run_exchange_success_uses_assessed_request_supplied_resolver_destination_and_successor(tmp_path):
    resolver, destination = _runtime_fixture(tmp_path)
    result = run_exchange(
        make_message(success_bundle()),
        success_bundle(),
        registry(),
        destination,
        evaluation_time=EVAL,
        manifest=manifest(),
        store_path=tmp_path / "exchange.sqlite",
        resolver=resolver,
        execution_policy_factory=synthetic_refund_policy,
    )
    assert result["execution"]["attempt_status"] == "executed"
    assert result["execution"]["newly_executed"] is True
    assert len(_effect_rows(destination)) == 1
    assert result["successor_packet"] is not None
    assert result["successor_packet"]["source_packet_id"] == result["successor_packet"]["source_packet_id"]
    assert result["successor_packet"]["unresolved_delivery"] is False
    assert result["odes_reference"]["recipient_validation"]["package_content_integrity"]["status"] == "pass"


def test_run_exchange_missing_resolver_fails_closed_without_effect(tmp_path):
    _, destination = _runtime_fixture(tmp_path)
    result = run_exchange(
        make_message(success_bundle()),
        success_bundle(),
        registry(),
        destination,
        evaluation_time=EVAL,
        manifest=manifest(),
        store_path=tmp_path / "exchange.sqlite",
    )
    assert result["execution"]["attempted"] is False
    assert result["execution"]["reason"] == "trusted_resolver_required"
    assert _effect_rows(destination) == []


@pytest.mark.parametrize("mutation", ["actor", "principal", "scope"])
def test_run_exchange_altered_actor_principal_or_scope_cannot_acquire_authority(tmp_path, mutation):
    baseline = success_bundle()
    resolver, _ = _runtime_fixture(tmp_path / "baseline", baseline)
    trial = copy.deepcopy(baseline)
    proposal = next(r for r in trial["records"] if r["record_type"] == "runtime_proposal")["data"]
    if mutation == "actor":
        proposal["actor"] = "urn:cognous:identity:attacker"
    elif mutation == "principal":
        proposal["principal"] = "urn:cognous:principal:attacker"
    else:
        proposal["requested_permissions"] = ["refund.issue.high"]
    runtime = load_executor_runtime()
    destination = runtime["DurableRefundDestination"](tmp_path / mutation / "moltbot-state")
    result = run_exchange(
        make_message(trial),
        trial,
        registry(),
        destination,
        evaluation_time=EVAL,
        manifest=manifest(),
        store_path=tmp_path / mutation / "exchange.sqlite",
        resolver=resolver,
        execution_policy_factory=synthetic_refund_policy,
    )
    assert result["execution"]["attempted"] is False
    assert result["assessment"]["stages"]["authority"] in {"hold", "denied"}
    assert _effect_rows(destination) == []
    assert result["successor_packet"] is not None


def test_run_exchange_revocation_after_decision_prevents_execution(tmp_path):
    resolver, destination = _runtime_fixture(tmp_path)

    def revoke(r):
        grant_id = next(iter(r.statuses))
        r.statuses[grant_id].status = "revoked"

    result = run_exchange(
        make_message(success_bundle()),
        success_bundle(),
        registry(),
        destination,
        evaluation_time=EVAL,
        manifest=manifest(),
        store_path=tmp_path / "exchange.sqlite",
        resolver=resolver,
        execution_policy_factory=synthetic_refund_policy,
        mutate_resolver_after_decision=revoke,
    )
    assert result["execution"]["attempt_status"] == "denied"
    assert result["execution"]["newly_executed"] is False
    assert _effect_rows(destination) == []
    assert result["successor_packet"] is not None


def test_run_exchange_uses_supplied_destination_state_restart_and_redelivery(tmp_path):
    resolver, destination = _runtime_fixture(tmp_path)
    first = run_exchange(
        make_message(success_bundle(), message_id="m1"),
        success_bundle(),
        registry(),
        destination,
        evaluation_time=EVAL,
        manifest=manifest(),
        store_path=tmp_path / "exchange.sqlite",
        resolver=resolver,
        execution_policy_factory=synthetic_refund_policy,
        lose_ack=True,
    )
    assert first["execution"]["attempt_status"] == "unknown"
    assert first["successor_packet"]["unresolved_delivery"] is True
    assert len(_effect_rows(destination)) == 1

    runtime = load_executor_runtime()
    restarted_destination = runtime["DurableRefundDestination"](destination.root)
    resolver2, _ = _runtime_fixture(tmp_path / "second")
    second = run_exchange(
        make_message(success_bundle(), message_id="m2"),
        success_bundle(),
        registry(),
        restarted_destination,
        evaluation_time="2026-08-08T01:01:00Z",
        manifest=manifest(),
        store_path=tmp_path / "exchange.sqlite",
        resolver=resolver2,
        execution_policy_factory=synthetic_refund_policy,
    )
    assert second["execution"]["attempt_status"] == "reconciled"
    assert second["execution"]["newly_executed"] is False
    assert len(_effect_rows(restarted_destination)) == 1

    resolver3, _ = _runtime_fixture(tmp_path / "third")
    exact_redelivery = run_exchange(
        make_message(success_bundle(), message_id="m2"),
        success_bundle(),
        registry(),
        restarted_destination,
        evaluation_time="2026-08-08T01:02:00Z",
        manifest=manifest(),
        store_path=tmp_path / "exchange.sqlite",
        resolver=resolver3,
        execution_policy_factory=synthetic_refund_policy,
    )
    assert exact_redelivery["execution"]["attempt_status"] == "reconciled"
    assert len(_effect_rows(restarted_destination)) == 1


def test_run_exchange_partial_delivery_successor_reports_pending_unresolved_effect(tmp_path):
    resolver, destination = _runtime_fixture(tmp_path)
    result = run_exchange(
        make_message(success_bundle()),
        success_bundle(),
        registry(),
        destination,
        evaluation_time=EVAL,
        manifest=manifest(),
        store_path=tmp_path / "exchange.sqlite",
        resolver=resolver,
        execution_policy_factory=synthetic_refund_policy,
        partial_delivery=True,
    )
    assert result["execution"]["attempt_status"] == "partial"
    assert result["successor_packet"]["unresolved_delivery"] is True
    assert result["successor_packet"]["pending_effects"]
    assert len(_effect_rows(destination)) == 1


def test_success_uses_actual_executor_and_replay_odes_pipeline(tmp_path):
    outcome = run_actual_outcome(tmp_path, "success")
    assert outcome["execution"]["attempt_status"] == "executed"
    assert outcome["execution"]["newly_executed"] is True
    assert len(outcome["execution_facts"]["destination_effects"]) == 1
    assert any(r["record_type"] == "runtime_decision" for r in outcome["current_reconstruction_bundle"]["records"])
    assert any(r["record_type"] == "destination_effect" for r in outcome["current_reconstruction_bundle"]["records"])
    assert outcome["successor_packet"] is not None
    validation = outcome["odes_reference"]["recipient_validation"]
    assert validation["schema_validity"]["status"] == "pass"
    assert validation["package_content_integrity"]["status"] == "pass"
    assert validation["integrity_authentication_checks"]["status"] == "unavailable"
    assert validation["historical_authority_assertions"]["status"] == "unavailable"
    assert validation["authority_status_and_freshness"]["status"] == "unavailable"


def test_odes_package_is_not_mutated_after_export(tmp_path):
    outcome = run_actual_outcome(tmp_path, "success")
    package = outcome["odes_reference"]["odes_package"]
    validation = outcome["odes_reference"]["recipient_validation"]
    assert validation["package_content_integrity"]["status"] == "pass"
    original_digest = package["package_digest"]
    package["provenance"]["execution_facts"]["post_export_mutation"] = True
    from odes import evaluate_recipient_package

    mutated = evaluate_recipient_package(package, outcome["odes_reference"]["recipient_policy"])
    assert package["package_digest"] == original_digest
    assert mutated["package_content_integrity"]["status"] == "fail"


@pytest.mark.parametrize(
    "outcome,expected_status,expected_effects",
    [
        ("hold", "hold", 0),
        ("denied_after_decision", "denied", 0),
        ("lost_ack", "unknown", 1),
        ("restart_reconciliation", "reconciled", 1),
        ("duplicate_delivery", "reconciled", 1),
        ("partial", "partial", 1),
    ],
)
def test_pipeline_exists_for_supported_outcomes(tmp_path, outcome, expected_status, expected_effects):
    result = run_actual_outcome(tmp_path / outcome, outcome)
    assert result["execution"]["attempt_status"] == expected_status
    assert len(result["execution_facts"]["destination_effects"]) == expected_effects
    assert result["current_reconstruction_bundle"]["records"]
    assert result["successor_packet"] is not None
    assert result["odes_reference"]["recipient_validation"]["package_content_integrity"]["status"] == "pass"
    if outcome == "partial":
        assert result["execution_facts"]["destination_observed"] == "partial"
        assert result["successor_packet"]["pending_effects"]
    if outcome in {"restart_reconciliation", "duplicate_delivery"}:
        assert result["execution"]["newly_executed"] is False
        assert result["execution"]["attempt_id"] is not None


def test_identical_refund_content_with_different_operation_identity_does_not_reconcile(tmp_path):
    runtime = load_executor_runtime()
    producer = runtime["producer"]
    from engine.safe_executor import LocalDestinationExecutor, snapshot_envelope

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
        payload={"refund_reason": "same"},
        payload_commitment=producer.commitment({"refund_reason": "same"}),
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
    destination = runtime["DurableRefundDestination"](tmp_path / "state")
    executor = LocalDestinationExecutor(destination, synthetic_refund_policy(op))
    request = runtime["ExecutionEnvelope"]("0.2.0", "decision-1", "effect-1", op)
    assert executor.execute_snapshot(snapshot_envelope(request)).status == "executed"

    changed_op = runtime["ExecutionOperation"](
        **{
            **op.__dict__,
            "grant_id": "urn:cognous:grant:other",
            "grant_revision": "2",
            "proposal_commitment": "sha256:" + "9" * 64,
        }
    )
    changed = runtime["ExecutionEnvelope"]("0.2.0", "decision-1", "effect-1", changed_op)
    with pytest.raises(PermissionError):
        LocalDestinationExecutor(destination, synthetic_refund_policy(changed_op)).observe_historical(changed)
    assert len(_effect_rows(destination)) == 1


def test_unrelated_existing_effect_is_not_reused_when_current_operation_fails(tmp_path):
    runtime = load_executor_runtime()
    producer = runtime["producer"]
    from engine.safe_executor import LocalDestinationExecutor, snapshot_envelope

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
        payload={"refund_reason": "first"},
        payload_commitment=producer.commitment({"refund_reason": "first"}),
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
    destination = runtime["DurableRefundDestination"](tmp_path / "state")
    executor = LocalDestinationExecutor(destination, synthetic_refund_policy(op))
    request = runtime["ExecutionEnvelope"]("0.2.0", "decision-1", "effect-1", op)
    assert executor.execute_snapshot(snapshot_envelope(request)).status == "executed"

    other_payload = {"refund_reason": "other"}
    other_op = runtime["ExecutionOperation"](
        **{
            **op.__dict__,
            "target": "urn:cognous:synthetic-account:customer-999",
            "payload": other_payload,
            "payload_commitment": producer.commitment(other_payload),
            "proposal_commitment": producer.commitment({"unrelated": True}),
        }
    )
    other_request = runtime["ExecutionEnvelope"]("0.2.0", "decision-other", "effect-other", other_op)
    other_result = LocalDestinationExecutor(
        destination, synthetic_refund_policy(other_op)
    ).execute_snapshot(snapshot_envelope(other_request))
    assert other_result.status == "failed"
    assert other_result.newly_executed is False
    assert {row["effect_id"] for row in _effect_rows(destination)} == {"effect-1"}

def test_gax_assessment_rejects_message_tampering_without_effect():
    bundle = success_bundle()
    msg = make_message(bundle)
    for mutate in ("payload", "target", "proposal"):
        trial = copy.deepcopy(msg)
        if mutate == "payload":
            trial["requested_action"]["payload"] = {"tampered": True}
        elif mutate == "target":
            trial["requested_action"]["target"] = "refunds/attacker"
        else:
            trial["proposal_commitment"] = "sha256:" + "0" * 64
        trial["message_digest"] = digest({k: v for k, v in trial.items() if k != "message_digest"})
        result = assess_message(trial, bundle, registry(), evaluation_time=EVAL)
        assert result["permitted_handling"] == "REFUSE"
        assert "GAX-REFERENCE-OPERATION-BINDING-MISMATCH" in result["errors"]


def test_report_refuse_not_understood_never_execute(tmp_path):
    for message_type in ("REPORT", "REFUSE", "NOT_UNDERSTOOD"):
        result = run_exchange(
            make_message(success_bundle(), message_type=message_type),
            success_bundle(),
            registry(),
            None,
            evaluation_time=EVAL,
            manifest=manifest(),
            store_path=tmp_path / message_type / "exchange.sqlite",
        )
        assert result["execution"]["attempted"] is False


def test_successor_lineage_requires_explicit_accepted_parent(tmp_path):
    store = tmp_path / "lineage.sqlite"
    predecessor = {"message_id": "m0", "conversation_id": "conv", "payload": "source"}
    p1 = make_successor_packet(predecessor, facts={"decision_id": "d1", "pending_effects": [], "attempts": {}, "unresolved_delivery": False}, state_version=1)
    assert load_successor_packet(p1, None, predecessor=predecessor, store_path=store)["loaded"] is True
    p2_missing = make_successor_packet(p1, facts={"decision_id": "d2", "pending_effects": [], "attempts": {}, "unresolved_delivery": False}, state_version=2)
    assert load_successor_packet(p2_missing, None, predecessor=p1, store_path=store)["status"] == "missing_predecessor_packet_id"
    p2 = make_successor_packet(p1, facts={"decision_id": "d2", "pending_effects": [], "attempts": {}, "unresolved_delivery": False}, state_version=2, predecessor_packet_id=p1["packet_id"])
    assert load_successor_packet(p2, None, predecessor=p1, store_path=store)["loaded"] is True


def test_successor_lineage_rejects_wrong_conversation_stale_and_divergent_branch(tmp_path):
    store = tmp_path / "lineage.sqlite"
    predecessor = {"message_id": "m0", "conversation_id": "conv", "payload": "source"}
    p1 = make_successor_packet(predecessor, facts={"decision_id": "d1"}, state_version=1)
    assert load_successor_packet(p1, None, predecessor=predecessor, store_path=store)["loaded"] is True
    wrong_conv_pred = {**p1, "conversation_id": "other"}
    p2_wrong_conv = make_successor_packet(wrong_conv_pred, facts={"decision_id": "d2"}, state_version=2, predecessor_packet_id=p1["packet_id"])
    assert load_successor_packet(p2_wrong_conv, None, predecessor=wrong_conv_pred, store_path=store)["status"] == "conversation_mismatch"
    stale = make_successor_packet(p1, facts={"decision_id": "d-stale"}, state_version=1, predecessor_packet_id=p1["packet_id"])
    assert load_successor_packet(stale, None, predecessor=p1, store_path=store)["status"] == "stale_or_rollback"
    branch = make_successor_packet(p1, facts={"decision_id": "d3"}, state_version=3, predecessor_packet_id="not-head")
    assert load_successor_packet(branch, None, predecessor=p1, store_path=store)["status"] == "divergent_branch"


def test_copied_predecessor_digest_field_cannot_mask_altered_content(tmp_path):
    store = tmp_path / "lineage.sqlite"
    source = {"message_id": "m0", "conversation_id": "conv", "payload": "source"}
    p1 = make_successor_packet(source, facts={"decision_id": "d1", "pending_effects": []}, state_version=1)
    assert load_successor_packet(p1, None, predecessor=source, store_path=store)["loaded"] is True
    altered = copy.deepcopy(p1)
    altered["pending_effects"] = ["changed"]
    # Deliberately retain p1's original packet_digest while p2 is correctly hashed
    # against the altered predecessor content.
    p2 = make_successor_packet(altered, facts={"decision_id": "d2"}, state_version=2, predecessor_packet_id=p1["packet_id"])
    assert load_successor_packet(p2, None, predecessor=altered, store_path=store)["status"] == "predecessor_digest_mismatch"


def test_caller_supplied_predecessor_content_cannot_establish_history_when_rehashed(tmp_path):
    store = tmp_path / "lineage.sqlite"
    source = {"message_id": "m0", "conversation_id": "conv", "payload": "source"}
    p1 = make_successor_packet(source, facts={"decision_id": "d1"}, state_version=1)
    assert load_successor_packet(p1, None, predecessor=source, store_path=store)["loaded"] is True
    forged_parent_content = {**p1, "pending_effects": ["changed"]}
    forged_parent_content["packet_digest"] = digest({k: v for k, v in forged_parent_content.items() if k != "packet_digest"})
    p2 = make_successor_packet(forged_parent_content, facts={"decision_id": "d2"}, state_version=2, predecessor_packet_id=p1["packet_id"])
    assert load_successor_packet(p2, None, predecessor=forged_parent_content, store_path=store)["status"] == "predecessor_content_not_accepted_head"
