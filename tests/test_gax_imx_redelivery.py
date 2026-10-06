from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

from experiments.odex_gax_imx_reference.gax_ref_runtime import (
    LocalRegistry,
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


def manifest() -> dict:
    return load_env("UPSTREAM_MANIFEST_EXAMPLE")


def success_bundle() -> dict:
    return load_env("UPSTREAM_REPLAY_SUCCESS_EXAMPLE")


def registry() -> LocalRegistry:
    return LocalRegistry({"refund-sender"}, {"refund-recipient"}, "refund-recipient")


def resolver_for(bundle: dict | None = None):
    proposal = runtime_proposal_model(bundle or success_bundle())
    return build_synthetic_resolver(proposal, now=parse_time(EVAL))


def destination_at(tmp_path: Path):
    runtime = load_executor_runtime()
    return runtime["DurableRefundDestination"](tmp_path / "moltbot-state")


def effect_rows(destination):
    with sqlite3.connect(destination.path) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute("SELECT * FROM effects ORDER BY rowid")]


def assert_artifacts(result: dict):
    assert result["current_reconstruction_bundle"]["records"]
    assert result["odes_reference"]["recipient_validation"]["package_content_integrity"]["status"] == "pass"
    assert result["successor_packet"] is not None
    assert result["successor_packet"]["missing_evidence"] == []


def insert_unrelated_first_effect(destination) -> None:
    with sqlite3.connect(destination.path) as conn:
        conn.execute(
            """
            INSERT INTO effects(effect_id,operation_digest,grant_id,target,amount,unit,payload_json,state)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                "effect-unrelated-first",
                "sha256:" + "1" * 64,
                "urn:cognous:grant:unrelated",
                "urn:cognous:synthetic-account:unrelated",
                1.0,
                "USD",
                json.dumps({"refund_reason": "unrelated"}, sort_keys=True),
                "applied",
            ),
        )


def run_success(tmp_path: Path, *, message_id: str = "m-success", destination=None):
    destination = destination or destination_at(tmp_path)
    msg = make_message(success_bundle(), message_id=message_id)
    result = run_exchange(
        msg,
        success_bundle(),
        registry(),
        destination,
        evaluation_time=EVAL,
        manifest=manifest(),
        store_path=tmp_path / "exchange.sqlite",
        resolver=resolver_for(),
        execution_policy_factory=synthetic_refund_policy,
    )
    return msg, destination, result


def redeliver(tmp_path: Path, msg: dict, destination, *, partial_delivery: bool = False, lose_ack: bool = False, resolver=None):
    return run_exchange(
        msg,
        success_bundle(),
        registry(),
        destination,
        evaluation_time=EVAL,
        manifest=manifest(),
        store_path=tmp_path / "exchange.sqlite",
        resolver=resolver or resolver_for(),
        execution_policy_factory=synthetic_refund_policy,
        partial_delivery=partial_delivery,
        lose_ack=lose_ack,
    )


def test_redelivery_uses_bound_effect_not_first_destination_row(tmp_path):
    destination = destination_at(tmp_path)
    insert_unrelated_first_effect(destination)
    msg, destination, first = run_success(tmp_path, message_id="m-unrelated", destination=destination)
    rows = effect_rows(destination)
    assert rows[0]["effect_id"] == "effect-unrelated-first"
    original_effect = first["execution"]["effect_id"]
    assert original_effect != rows[0]["effect_id"]

    replayed = redeliver(tmp_path, msg, destination)

    assert replayed["execution"]["attempt_status"] == "reconciled"
    assert replayed["execution"]["effect_id"] == original_effect
    assert replayed["execution"]["decision_id"] == first["execution"]["decision_id"]
    assert replayed["execution"]["newly_executed"] is False
    assert [row["effect_id"] for row in effect_rows(destination)].count(original_effect) == 1
    assert len(effect_rows(destination)) == 2
    assert_artifacts(replayed)


def test_partial_delivery_exact_redelivery_preserves_pending_and_unresolved(tmp_path):
    destination = destination_at(tmp_path)
    msg = make_message(success_bundle(), message_id="m-partial")
    first = run_exchange(
        msg,
        success_bundle(),
        registry(),
        destination,
        evaluation_time=EVAL,
        manifest=manifest(),
        store_path=tmp_path / "exchange.sqlite",
        resolver=resolver_for(),
        execution_policy_factory=synthetic_refund_policy,
        partial_delivery=True,
    )
    original_effect = first["execution"]["effect_id"]

    replayed = redeliver(tmp_path, msg, destination)

    assert replayed["execution"]["attempt_status"] == "partial"
    assert replayed["execution"]["effect_id"] == original_effect
    assert replayed["execution"]["destination_observed"] == "partial"
    assert replayed["successor_packet"]["pending_effects"] == [original_effect]
    assert replayed["successor_packet"]["unresolved_delivery"] is True
    assert len(effect_rows(destination)) == 1
    assert_artifacts(replayed)


def test_lost_ack_exact_redelivery_preserves_unknown_unresolved_without_second_effect(tmp_path):
    destination = destination_at(tmp_path)
    msg = make_message(success_bundle(), message_id="m-lost-ack")
    first = run_exchange(
        msg,
        success_bundle(),
        registry(),
        destination,
        evaluation_time=EVAL,
        manifest=manifest(),
        store_path=tmp_path / "exchange.sqlite",
        resolver=resolver_for(),
        execution_policy_factory=synthetic_refund_policy,
        lose_ack=True,
    )
    original_effect = first["execution"]["effect_id"]

    replayed = redeliver(tmp_path, msg, destination)

    assert replayed["execution"]["attempt_status"] == "unknown"
    assert replayed["execution"]["effect_id"] == original_effect
    assert replayed["successor_packet"]["unresolved_delivery"] is True
    assert replayed["successor_packet"]["pending_effects"] == []
    assert len(effect_rows(destination)) == 1
    assert_artifacts(replayed)


def test_receipt_then_failure_before_effect_redelivery_returns_evidence_not_shortcut(tmp_path):
    destination = destination_at(tmp_path)
    msg = make_message(success_bundle(), message_id="m-revoked")

    def revoke(r):
        grant_id = next(iter(r.statuses))
        r.statuses[grant_id].status = "revoked"

    first = run_exchange(
        msg,
        success_bundle(),
        registry(),
        destination,
        evaluation_time=EVAL,
        manifest=manifest(),
        store_path=tmp_path / "exchange.sqlite",
        resolver=resolver_for(),
        execution_policy_factory=synthetic_refund_policy,
        mutate_resolver_after_decision=revoke,
    )
    assert first["execution"]["attempt_status"] == "denied"
    assert effect_rows(destination) == []

    replayed = redeliver(tmp_path, msg, destination)

    assert replayed["execution"]["attempt_status"] == "denied"
    assert replayed["execution"]["effect_id"] == first["execution"]["effect_id"]
    assert replayed["execution"]["decision_id"] == first["execution"]["decision_id"]
    assert replayed["successor_packet"]["pending_effects"] == []
    assert replayed["successor_packet"]["unresolved_delivery"] is False
    assert effect_rows(destination) == []
    assert_artifacts(replayed)


def test_missing_resolver_receipt_can_resume_with_valid_retry(tmp_path):
    destination = destination_at(tmp_path)
    msg = make_message(success_bundle(), message_id="m-missing-resolver")
    first = run_exchange(
        msg,
        success_bundle(),
        registry(),
        destination,
        evaluation_time=EVAL,
        manifest=manifest(),
        store_path=tmp_path / "exchange.sqlite",
    )
    assert first["execution"]["reason"] == "trusted_resolver_required"
    assert effect_rows(destination) == []

    retry = redeliver(tmp_path, msg, destination)
    assert retry["execution"]["attempt_status"] == "executed"
    assert retry["execution"]["newly_executed"] is True
    assert len(effect_rows(destination)) == 1
    assert_artifacts(retry)

    completed_redelivery = redeliver(tmp_path, msg, destination)
    assert completed_redelivery["execution"]["attempt_status"] == "reconciled"
    assert completed_redelivery["execution"]["effect_id"] == retry["execution"]["effect_id"]
    assert len(effect_rows(destination)) == 1
    assert_artifacts(completed_redelivery)


def test_completed_delivery_exact_redelivery_preserves_original_identity_no_second_effect(tmp_path):
    msg, destination, first = run_success(tmp_path, message_id="m-completed")
    original_effect = first["execution"]["effect_id"]
    original_decision = first["execution"]["decision_id"]

    replayed = redeliver(tmp_path, msg, destination)

    assert replayed["execution"]["attempt_status"] == "reconciled"
    assert replayed["execution"]["effect_id"] == original_effect
    assert replayed["execution"]["decision_id"] == original_decision
    assert replayed["execution"]["newly_executed"] is False
    assert replayed["successor_packet"]["pending_effects"] == []
    assert replayed["successor_packet"]["unresolved_delivery"] is False
    assert len(effect_rows(destination)) == 1
    assert_artifacts(replayed)


def test_checkpoint_after_destination_commit_recovers_original_effect_without_replacement(tmp_path):
    destination = destination_at(tmp_path)
    msg = make_message(success_bundle(), message_id="m-checkpoint-after-dispatch")
    interrupted = run_exchange(
        msg,
        success_bundle(),
        registry(),
        destination,
        evaluation_time=EVAL,
        manifest=manifest(),
        store_path=tmp_path / "exchange.sqlite",
        resolver=resolver_for(),
        execution_policy_factory=synthetic_refund_policy,
        fault_after_dispatch=True,
    )
    original_effect = interrupted["execution"]["effect_id"]
    original_decision = interrupted["execution"]["decision_id"]
    assert interrupted["execution"]["attempt_status"] == "interrupted_after_dispatch"
    assert len(effect_rows(destination)) == 1

    restarted = redeliver(tmp_path, msg, destination)

    assert restarted["execution"]["attempt_status"] == "reconciled"
    assert restarted["execution"]["effect_id"] == original_effect
    assert restarted["execution"]["decision_id"] == original_decision
    assert restarted["execution"]["newly_executed"] is False
    assert len(effect_rows(destination)) == 1
    assert_artifacts(restarted)


def test_checkpoint_recovery_with_changed_policy_does_not_issue_replacement_refund(tmp_path):
    destination = destination_at(tmp_path)
    msg = make_message(success_bundle(), message_id="m-checkpoint-policy-change")
    interrupted = run_exchange(
        msg,
        success_bundle(),
        registry(),
        destination,
        evaluation_time=EVAL,
        manifest=manifest(),
        store_path=tmp_path / "exchange.sqlite",
        resolver=resolver_for(),
        execution_policy_factory=synthetic_refund_policy,
        fault_after_dispatch=True,
    )
    original_effect = interrupted["execution"]["effect_id"]

    changed = resolver_for()
    grant_id = next(iter(changed.statuses))
    changed.statuses[grant_id].status = "revoked"
    restarted = redeliver(tmp_path, msg, destination, resolver=changed)

    assert restarted["execution"]["attempt_status"] == "reconciled"
    assert restarted["execution"]["effect_id"] == original_effect
    assert restarted["execution"]["newly_executed"] is False
    assert len(effect_rows(destination)) == 1
    assert_artifacts(restarted)


def test_replay_odes_failure_after_commit_retries_evidence_without_repeating_effect(tmp_path):
    destination = destination_at(tmp_path)
    msg = make_message(success_bundle(), message_id="m-evidence-fault")
    interrupted = run_exchange(
        msg,
        success_bundle(),
        registry(),
        destination,
        evaluation_time=EVAL,
        manifest=manifest(),
        store_path=tmp_path / "exchange.sqlite",
        resolver=resolver_for(),
        execution_policy_factory=synthetic_refund_policy,
        fault_evidence_once=True,
    )
    original_effect = interrupted["execution"]["effect_id"]
    assert interrupted["execution"]["attempt_status"] == "evidence_export_failed"
    assert len(effect_rows(destination)) == 1

    retried = redeliver(tmp_path, msg, destination)

    assert retried["execution"]["attempt_status"] == "reconciled"
    assert retried["execution"]["effect_id"] == original_effect
    assert retried["execution"]["newly_executed"] is False
    assert len(effect_rows(destination)) == 1
    assert_artifacts(retried)
