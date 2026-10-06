from __future__ import annotations

import copy
import json
import os
from pathlib import Path

from experiments.odex_gax_imx_reference.gax_ref import (
    DestinationState,
    DurableExchangeStore,
    LocalRegistry,
    assess_message,
    digest,
    export_odes_reference,
    execution_facts,
    load_successor_packet,
    make_message,
    make_successor_packet,
    run_exchange,
    runtime_proposal_model,
    _build_resolver,
    parse_time,
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


def lost_ack_bundle() -> dict:
    return load_env("UPSTREAM_REPLAY_LOST_ACK_EXAMPLE")


def registry() -> LocalRegistry:
    return LocalRegistry({"refund-sender"}, {"refund-recipient"}, "refund-recipient")


def run_ok(bundle: dict, tmp_path: Path, **kwargs):
    destination = DestinationState(tmp_path / "destination.json")
    msg = kwargs.pop("message", make_message(bundle))
    return run_exchange(msg, bundle, registry(), destination, evaluation_time=EVAL, manifest=manifest(), store_path=tmp_path / "exchange.json", **kwargs), destination


def test_valid_bounded_request_uses_workflow_and_produces_one_effect(tmp_path):
    bundle = success_bundle()
    result, destination = run_ok(bundle, tmp_path)
    assert result["assessment"]["permitted_handling"] == "ACCEPT_FOR_ASSESSMENT"
    assert result["assessment"]["stages"]["authority"] == "authorized"
    assert result["execution"]["attempted"] is True
    assert result["execution"]["newly_executed"] is True
    assert len(destination.effects) == 1
    second, _ = run_ok(bundle, tmp_path, message=make_message(bundle))
    assert second["execution"]["newly_executed"] is False
    assert len(destination.effects) == 1


def test_governor_fabricated_authorized_record_cannot_execute(tmp_path):
    bundle = success_bundle()
    fake = copy.deepcopy(bundle)
    proposal = fake["records"][0]["data"]
    proposal["target"] = "urn:cognous:synthetic-account:attacker"
    proposal["payload"]["refund_reason"] = "fabricated"
    proposal["payload_commitment"] = digest(proposal["payload"])
    fake["records"] = [r for r in fake["records"] if r.get("record_type") in {"runtime_proposal", "runtime_decision"}]
    for record in fake["records"]:
        if record.get("record_type") == "runtime_decision":
            record["data"] = {"decision_id": "caller-decision", "result": "authorized", "effect_id": "caller-effect", "binding": {"proposal_commitment": digest(proposal)}}
    destination = DestinationState(tmp_path / "destination.json")
    try:
        result = run_exchange(make_message(fake), fake, registry(), destination, evaluation_time=EVAL, manifest=manifest(), store_path=tmp_path / "exchange.json")
    except Exception:
        result = {"execution": {"attempted": False}}
    assert result["execution"]["attempted"] is False
    assert destination.effects == {}


def test_report_refuse_and_not_understood_never_execute(tmp_path):
    for message_type in ("REPORT", "REFUSE", "NOT_UNDERSTOOD"):
        bundle = success_bundle()
        msg = make_message(bundle, message_type=message_type)
        result, destination = run_ok(bundle, tmp_path / message_type, message=msg)
        assert result["execution"]["attempted"] is False
        assert result["assessment"]["permitted_handling"] == "INFORMATIONAL_ONLY"
        assert destination.effects == {}


def test_wrong_sender_recipient_or_expired_message_no_effect(tmp_path):
    bundle = success_bundle()
    for kwargs in ({"sender": "unknown"}, {"recipient": "other"}, {"expires_at": "2020-01-01T00:00:00Z"}):
        msg = make_message(bundle, **kwargs)
        result, destination = run_ok(bundle, tmp_path / str(kwargs), message=msg)
        assert result["execution"]["attempted"] is False
        assert destination.effects == {}


def test_changed_payload_target_or_proposal_commitment_rejects():
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


def test_revocation_or_policy_change_after_receipt_prevents_execution(tmp_path):
    bundle = success_bundle()
    proposal = runtime_proposal_model(bundle)
    resolver = _build_resolver(proposal, now=parse_time(EVAL))
    grant_id = next(iter(resolver.statuses))
    def revoke(r):
        r.statuses[grant_id].status = "revoked"
    result, destination = run_ok(bundle, tmp_path, resolver=resolver, mutate_resolver_after_decision=revoke)
    assert result["execution"]["attempted"] is False
    assert "authorization-critical inputs changed" in result["execution"]["reason"]
    assert destination.effects == {}


def test_durable_restart_after_commit_before_ack_reconciles_existing_effect(tmp_path):
    bundle = success_bundle()
    destination1 = DestinationState(tmp_path / "destination.json")
    first = run_exchange(make_message(bundle, message_id="m1"), bundle, registry(), destination1, evaluation_time=EVAL, manifest=manifest(), store_path=tmp_path / "exchange.json", lose_ack=True)
    assert first["execution"]["attempt_status"] == "unknown"
    assert len(destination1.effects) == 1
    destination2 = DestinationState(tmp_path / "destination.json")
    second = run_exchange(make_message(bundle, message_id="m2"), bundle, registry(), destination2, evaluation_time="2026-08-08T01:02:00Z", manifest=manifest(), store_path=tmp_path / "exchange.json")
    assert second["execution"]["newly_executed"] is False
    assert len(destination2.effects) == 1


def test_new_message_id_replay_does_not_duplicate_effect(tmp_path):
    bundle = success_bundle()
    destination = DestinationState(tmp_path / "destination.json")
    first = run_exchange(make_message(bundle, message_id="m1"), bundle, registry(), destination, evaluation_time=EVAL, manifest=manifest(), store_path=tmp_path / "exchange.json")
    second = run_exchange(make_message(bundle, message_id="m2"), bundle, registry(), destination, evaluation_time="2026-08-08T01:01:00Z", manifest=manifest(), store_path=tmp_path / "exchange.json")
    assert first["execution"]["newly_executed"] is True
    assert second["execution"]["newly_executed"] is False
    assert len(destination.effects) == 1


def test_message_id_reuse_with_changed_content_rejects(tmp_path):
    bundle = success_bundle()
    destination = DestinationState(tmp_path / "destination.json")
    msg = make_message(bundle, message_id="same")
    run_exchange(msg, bundle, registry(), destination, evaluation_time=EVAL, manifest=manifest(), store_path=tmp_path / "exchange.json")
    changed = copy.deepcopy(msg)
    changed["content"]["summary"] = "changed"
    changed["content_digest"] = digest(changed["content"])
    changed["message_digest"] = digest({k: v for k, v in changed.items() if k != "message_digest"})
    result = run_exchange(changed, bundle, registry(), destination, evaluation_time="2026-08-08T01:01:00Z", manifest=manifest(), store_path=tmp_path / "exchange.json")
    assert result["execution"]["attempted"] is False
    assert "GAX-INTEGRITY-MESSAGE-ID-REUSE" in result["assessment"]["errors"]


def test_same_effect_id_with_different_operation_content_rejects(tmp_path):
    bundle = success_bundle()
    destination = DestinationState(tmp_path / "destination.json")
    first = run_exchange(make_message(bundle, message_id="m1"), bundle, registry(), destination, evaluation_time=EVAL, manifest=manifest(), store_path=tmp_path / "exchange.json")
    store = DurableExchangeStore(tmp_path / "exchange.json")
    effect_id = first["execution"]["effect_id"]
    state = store._load(); state["effects"][effect_id] = {"operation_digest": "sha256:" + "0" * 64}; store._write(state)
    second = run_exchange(make_message(bundle, message_id="m2"), bundle, registry(), destination, evaluation_time="2026-08-08T01:01:00Z", manifest=manifest(), store_path=tmp_path / "exchange.json")
    assert second["execution"]["attempted"] is False
    assert "GAX-EFFECT-ID-REUSE-WITH-DIFFERENT-OPERATION" in second["assessment"]["errors"]


def test_partial_delivery_requires_actual_execution_flag_and_remains_unresolved(tmp_path):
    bundle = success_bundle()
    result, destination = run_ok(bundle, tmp_path, partial_delivery=True)
    assert result["execution"]["destination_observed"] == "partial"
    assert next(iter(destination.effects.values()))["state"] == "partial"


def test_historical_partial_evidence_alone_does_not_create_partial_effect(tmp_path):
    bundle = success_bundle()
    for record in bundle["records"]:
        if record.get("record_type") in {"effect_observation", "destination_effect"}:
            record["data"]["state"] = "partial"
    destination = DestinationState(tmp_path / "destination.json")
    try:
        result = run_exchange(make_message(bundle), bundle, registry(), destination, evaluation_time=EVAL, manifest=manifest(), store_path=tmp_path / "exchange.json")
    except Exception:
        result = {"execution": {"attempted": False}}
    assert not any(effect.get("state") == "partial" for effect in destination.effects.values())


def test_hold_deny_no_effect_from_retained_decision_outcomes(tmp_path):
    for outcome in ("hold", "denied"):
        bundle = success_bundle()
        for record in bundle["records"]:
            if record.get("record_type") == "runtime_decision":
                record["data"]["result"] = outcome
                record["data"].pop("binding", None)
                record["data"].pop("effect_id", None)
            if record.get("record_type") in {"execution_envelope", "execution_result", "destination_attempt", "destination_attempt_event", "destination_effect", "effect_observation", "reconciliation"}:
                record["_drop"] = True
        bundle["records"] = [r for r in bundle["records"] if not r.get("_drop")]
        destination = DestinationState(tmp_path / outcome / "destination.json")
        try:
            result = run_exchange(make_message(success_bundle()), bundle, registry(), destination, evaluation_time=EVAL, manifest=manifest(), store_path=tmp_path / outcome / "exchange.json")
        except Exception:
            result = {"execution": {"attempted": False}}
        assert result["execution"]["attempted"] is False
        assert destination.effects == {}


def test_successor_verifies_source_commitment_and_lineage(tmp_path):
    bundle = success_bundle(); msg = make_message(bundle); destination = DestinationState(tmp_path / "destination.json")
    packet = make_successor_packet(msg, facts=execution_facts(bundle), state_version=1)
    loaded = load_successor_packet(packet, destination, predecessor=msg, store_path=tmp_path / "exchange.json")
    assert loaded["loaded"] is True
    assert loaded["effect_created"] is False
    stale = copy.deepcopy(packet); stale["state_version"] = "state-1"; stale["packet_digest"] = digest({k: v for k, v in stale.items() if k != "packet_digest"})
    assert load_successor_packet(stale, destination, predecessor=msg, store_path=tmp_path / "exchange.json")["status"] == "stale_or_rollback"
    bad = copy.deepcopy(packet); bad["source_commitment"] = digest({"wrong": True}); bad["state_version"] = "state-2"; bad["packet_digest"] = digest({k: v for k, v in bad.items() if k != "packet_digest"})
    assert load_successor_packet(bad, destination, predecessor=msg, store_path=tmp_path / "exchange2.json")["status"] == "source_commitment_mismatch"


def test_unsupported_versions_and_downgrade_fail():
    bundle = success_bundle(); msg = make_message(bundle)
    msg["profile"] = "https://opendecisionevidence.org/profiles/odex-gax/0.2"
    msg["message_digest"] = digest({k: v for k, v in msg.items() if k != "message_digest"})
    result = assess_message(msg, bundle, registry(), evaluation_time=EVAL)
    assert "GAX-PROFILE-UNSUPPORTED-OR-DOWNGRADE" in result["errors"]


def test_odes_export_uses_merged_api_and_preserves_outcomes():
    ref = export_odes_reference(manifest(), success_bundle())
    assert ref["odes_package"]["record"]["schema_version"] == "0.1"
    assert "recipient_reliance_decision" in ref["recipient_validation"]
    assert ref["odes_package"]["provenance"]["execution_facts"]["destination_observed"] in {"applied", "partial", "unknown", "unavailable"}


def test_distinct_receipt_execution_observation_verification_facts():
    facts = execution_facts(success_bundle())
    assert "control_plane" in facts["attempts"]
    assert "executor" in facts["attempts"]
    assert facts["destination_observed"] in {"applied", "partial", "unavailable"}
    assert facts.get("independent_verification") is None
