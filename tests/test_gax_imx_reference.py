from __future__ import annotations

import copy
import json
import os
from pathlib import Path

import pytest

from experiments.odex_gax_imx_reference.gax_ref_runtime import (
    LocalRegistry,
    TransactionalExchangeStore,
    actual_executor_classes,
    assess_message,
    digest,
    execution_facts,
    load_actual_pinned_moltbot_helpers,
    load_successor_packet,
    make_message,
    make_successor_packet,
    run_actual_outcome,
    run_exchange,
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


def test_imports_actual_pinned_moltbot_executor_classes():
    classes = actual_executor_classes()
    assert classes["PinnedControlPlaneExecutor"].__module__ == "engine.control_plane_adapter"
    assert classes["ControlPlaneRefundDestinationAdapter"].__module__ == "engine.control_plane_adapter"
    assert classes["DurableRefundDestination"].__module__ == "engine.safe_executor"


def test_success_uses_actual_executor_and_replay_odes_pipeline(tmp_path):
    outcome = run_actual_outcome(tmp_path, "success")
    result = outcome["result"]
    assert result.status == "executed"
    assert result.newly_executed is True
    assert len(outcome["destination_effects"]) == 1
    assert any(r.record_type == "runtime_decision" for r in outcome["reconstruction"].records)
    assert any(r.record_type == "destination_effect" for r in outcome["reconstruction"].records)
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
    assert result["status"] == expected_status
    assert len(result["destination_effects"]) == expected_effects
    assert result["reconstruction_bundle"]["records"]
    assert result["odes_reference"]["recipient_validation"]["package_content_integrity"]["status"] == "pass"
    if outcome == "partial":
        assert result["execution_facts"]["destination_observed"] == "partial"
    if outcome in {"restart_reconciliation", "duplicate_delivery"}:
        assert result["result"].newly_executed is False
        assert result["result"].attempt_id is not None


def test_identical_refund_content_with_different_operation_identity_does_not_reconcile(tmp_path):
    h = load_actual_pinned_moltbot_helpers()
    helper, proposal, resolver, workflow, decision, destination, executor, request = h._integrated(tmp_path)
    first = executor.execute(envelope=request, proposal=proposal, decision=decision, now=helper.NOW)
    assert first.status == "executed"
    changed_op = h.ExecutionOperation(
        **{
            **request.operation.__dict__,
            "grant_id": "urn:cognous:grant:other",
            "grant_revision": "2",
            "proposal_commitment": "sha256:" + "9" * 64,
        }
    )
    changed = h.ExecutionEnvelope(request.version, request.decision_id, request.effect_id, changed_op)
    with pytest.raises(PermissionError):
        h.LocalDestinationExecutor(destination, h.policy(changed_op)).observe_historical(changed)
    assert len(_effect_rows(destination)) == 1


def test_unrelated_existing_effect_is_not_reused_when_current_operation_fails(tmp_path):
    h = load_actual_pinned_moltbot_helpers()
    helper, proposal, resolver, workflow, decision, destination, executor, request = h._integrated(tmp_path)
    assert executor.execute(envelope=request, proposal=proposal, decision=decision, now=helper.NOW).status == "executed"
    other_op = h.ExecutionOperation(
        **{
            **request.operation.__dict__,
            "target": "urn:cognous:synthetic-account:customer-999",
            "payload": {"refund_reason": "other"},
            "payload_commitment": h.commitment({"refund_reason": "other"}),
            "proposal_commitment": h.commitment({"unrelated": True}),
        }
    )
    other_request = h.ExecutionEnvelope(request.version, "decision-other", "effect-other", other_op)
    other_result = h.LocalDestinationExecutor(destination, h.policy(other_op)).execute_snapshot(
        h.snapshot_envelope(other_request)
    )
    assert other_result.status == "failed"
    assert other_result.newly_executed is False
    assert {row["effect_id"] for row in _effect_rows(destination)} == {decision.effect_id}


def test_lost_ack_restart_and_exact_redelivery_do_not_duplicate_effect(tmp_path):
    lost = run_actual_outcome(tmp_path / "lost", "restart_reconciliation")
    assert lost["result"].status == "reconciled"
    assert lost["result"].newly_executed is False
    assert len(lost["destination_effects"]) == 1
    dup = run_actual_outcome(tmp_path / "dup", "duplicate_delivery")
    assert dup["result"].status == "reconciled"
    assert dup["result"].newly_executed is False
    assert len(dup["destination_effects"]) == 1


def _effect_rows(destination):
    import sqlite3

    with sqlite3.connect(destination.path) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute("SELECT * FROM effects ORDER BY rowid")]


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


def test_caller_supplied_predecessor_content_cannot_establish_history(tmp_path):
    store = tmp_path / "lineage.sqlite"
    source = {"message_id": "m0", "conversation_id": "conv", "payload": "source"}
    p1 = make_successor_packet(source, facts={"decision_id": "d1"}, state_version=1)
    assert load_successor_packet(p1, None, predecessor=source, store_path=store)["loaded"] is True
    forged_parent_content = {**p1, "pending_effects": ["changed"]}
    forged_parent_content["packet_digest"] = digest({k: v for k, v in forged_parent_content.items() if k != "packet_digest"})
    p2 = make_successor_packet(forged_parent_content, facts={"decision_id": "d2"}, state_version=2, predecessor_packet_id=p1["packet_id"])
    assert load_successor_packet(p2, None, predecessor=forged_parent_content, store_path=store)["status"] == "predecessor_content_not_accepted_head"
