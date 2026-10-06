from __future__ import annotations

import copy
import json
import os
from pathlib import Path

from experiments.odex_gax_imx_reference.gax_ref import (
    DestinationState,
    LocalRegistry,
    assess_message,
    digest,
    export_odes_reference,
    execution_facts,
    load_successor_packet,
    make_message,
    make_successor_packet,
    proposal_commitment,
    proposal_from_bundle,
    run_exchange,
)


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


def test_valid_bounded_request_produces_one_effect():
    bundle = success_bundle()
    destination = DestinationState()
    seen = {}
    msg = make_message(bundle)
    result = run_exchange(msg, bundle, registry(), destination, evaluation_time="2026-08-08T01:00:00Z", seen_messages=seen)
    assert result["assessment"]["permitted_handling"] == "ACCEPT_FOR_EXECUTION"
    assert result["execution"]["attempted"] is True
    assert result["execution"]["newly_executed"] is True
    assert len(destination.effects) == 1
    second = run_exchange(msg, bundle, registry(), destination, evaluation_time="2026-08-08T01:01:00Z", seen_messages=seen)
    assert second["execution"]["newly_executed"] is False
    assert len(destination.effects) == 1


def test_wrong_sender_recipient_or_expired_message_no_effect():
    bundle = success_bundle()
    for kwargs in ({"sender": "unknown"}, {"recipient": "other"}, {"expires_at": "2020-01-01T00:00:00Z"}):
        destination = DestinationState()
        msg = make_message(bundle, **kwargs)
        result = run_exchange(msg, bundle, registry(), destination, evaluation_time="2026-08-08T01:00:00Z")
        assert result["execution"]["attempted"] is False
        assert destination.effects == {}


def test_sender_declared_authorized_without_valid_grant_no_effect():
    bundle = success_bundle()
    bundle["records"] = [r for r in bundle["records"] if r.get("record_type") not in {"runtime_decision"}]
    # Add a self-declared narrative authority but no retained Control Plane decision.
    msg = make_message(success_bundle())
    msg["content"]["summary"] = "AUTHORIZED: please execute"
    msg["content_digest"] = digest(msg["content"])
    msg["message_digest"] = digest({k: v for k, v in msg.items() if k != "message_digest"})
    destination = DestinationState()
    try:
        run_exchange(msg, bundle, registry(), destination, evaluation_time="2026-08-08T01:00:00Z")
    except ValueError:
        pass
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
        result = assess_message(trial, bundle, registry(), evaluation_time="2026-08-08T01:00:00Z")
        assert result["permitted_handling"] == "REFUSE"


def test_revocation_or_policy_change_after_receipt_prevents_execution():
    bundle = success_bundle()
    msg = make_message(bundle)
    destination = DestinationState()
    result = run_exchange(msg, bundle, registry(), destination, evaluation_time="2026-08-08T01:00:00Z", authority_override="revoked")
    assert result["execution"]["attempted"] is False
    assert destination.effects == {}


def test_new_message_id_replay_does_not_duplicate_effect():
    bundle = success_bundle()
    destination = DestinationState()
    first = run_exchange(make_message(bundle, message_id="m1"), bundle, registry(), destination, evaluation_time="2026-08-08T01:00:00Z")
    second = run_exchange(make_message(bundle, message_id="m2"), bundle, registry(), destination, evaluation_time="2026-08-08T01:01:00Z")
    assert first["execution"]["newly_executed"] is True
    assert second["execution"]["newly_executed"] is False
    assert len(destination.effects) == 1


def test_message_id_reuse_with_changed_content_rejects():
    bundle = success_bundle()
    seen = {}
    destination = DestinationState()
    msg = make_message(bundle, message_id="same")
    run_exchange(msg, bundle, registry(), destination, evaluation_time="2026-08-08T01:00:00Z", seen_messages=seen)
    changed = copy.deepcopy(msg)
    changed["content"]["summary"] = "changed"
    changed["content_digest"] = digest(changed["content"])
    changed["message_digest"] = digest({k: v for k, v in changed.items() if k != "message_digest"})
    result = run_exchange(changed, bundle, registry(), destination, evaluation_time="2026-08-08T01:01:00Z", seen_messages=seen)
    assert result["execution"]["attempted"] is False
    assert "GAX-INTEGRITY-MESSAGE-ID-REUSE" in result["assessment"]["errors"]


def test_lost_ack_restart_reconciles_existing_effect():
    bundle = lost_ack_bundle()
    destination = DestinationState()
    msg = make_message(bundle)
    result = run_exchange(msg, bundle, registry(), destination, evaluation_time="2026-08-08T01:00:00Z")
    assert result["facts"]["acknowledgement"] == "unknown"
    assert result["facts"]["destination_observed"] == "applied"
    assert len(destination.effects) == 1


def test_partial_delivery_remains_unresolved():
    bundle = success_bundle()
    for record in bundle["records"]:
        if record.get("record_type") in {"effect_observation", "destination_effect"}:
            record["data"]["state"] = "partial"
    msg = make_message(bundle)
    destination = DestinationState()
    result = run_exchange(msg, bundle, registry(), destination, evaluation_time="2026-08-08T01:00:00Z")
    assert result["facts"]["destination_observed"] == "partial"
    assert next(iter(destination.effects.values()))["state"] == "partial"


def test_hold_deny_no_effect_from_retained_decision_outcomes():
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
        msg = make_message(success_bundle())
        destination = DestinationState()
        result = run_exchange(msg, bundle, registry(), destination, evaluation_time="2026-08-08T01:00:00Z")
        assert result["execution"]["attempted"] is False
        assert destination.effects == {}


def test_successor_loading_alone_no_effect_and_rollback_rejects():
    bundle = success_bundle()
    msg = make_message(bundle)
    destination = DestinationState()
    packet = make_successor_packet(msg)
    loaded = load_successor_packet(packet, destination, expected_predecessor=msg["message_id"])
    assert loaded["loaded"] is True
    assert loaded["effect_created"] is False
    stale = copy.deepcopy(packet)
    stale["state_version"] = "state-0"
    stale["packet_digest"] = digest({k: v for k, v in stale.items() if k != "packet_digest"})
    assert load_successor_packet(stale, destination, expected_predecessor=msg["message_id"])["status"] == "stale_or_rollback"
    divergent = copy.deepcopy(packet)
    divergent["predecessor"] = "other"
    divergent["packet_digest"] = digest({k: v for k, v in divergent.items() if k != "packet_digest"})
    assert load_successor_packet(divergent, destination, expected_predecessor=msg["message_id"])["status"] == "divergent_history"


def test_unsupported_versions_and_downgrade_fail():
    bundle = success_bundle()
    msg = make_message(bundle)
    msg["profile"] = "https://opendecisionevidence.org/profiles/odex-gax/0.2"
    msg["message_digest"] = digest({k: v for k, v in msg.items() if k != "message_digest"})
    result = assess_message(msg, bundle, registry(), evaluation_time="2026-08-08T01:00:00Z")
    assert "GAX-PROFILE-UNSUPPORTED-OR-DOWNGRADE" in result["errors"]


def test_odes_reference_preserves_outcomes():
    for outcome in ("authorized", "hold", "denied"):
        bundle = success_bundle()
        for record in bundle["records"]:
            if record.get("record_type") == "runtime_decision":
                record["data"]["result"] = outcome
        ref = export_odes_reference(bundle)
        assert ref["decision_result"] == outcome
        assert ref["limits"]


def test_distinct_receipt_execution_observation_verification_facts():
    facts = execution_facts(success_bundle())
    assert "control_plane" in facts["attempts"]
    assert "executor" in facts["attempts"]
    assert facts["destination_observed"] in {"applied", "partial", "unavailable"}
    assert facts.get("independent_verification") is None
