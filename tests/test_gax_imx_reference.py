from __future__ import annotations

import copy
import json
import os
from pathlib import Path

import pytest

from experiments.odex_gax_imx_reference.gax_ref import (
    DestinationState, LocalRegistry, TransactionalExchangeStore, _build_resolver,
    assess_message, digest, execution_facts, export_odes_reference, load_successor_packet,
    make_message, make_successor_packet, parse_time, run_exchange, runtime_proposal_model,
)

EVAL = "2026-08-08T01:00:00Z"


def load_env(name: str) -> dict:
    value = os.environ.get(name)
    assert value, f"missing {name}"
    return json.loads(Path(value).read_text(encoding="utf-8"))


def manifest() -> dict: return load_env("UPSTREAM_MANIFEST_EXAMPLE")
def success_bundle() -> dict: return load_env("UPSTREAM_REPLAY_SUCCESS_EXAMPLE")
def lost_ack_bundle() -> dict: return load_env("UPSTREAM_REPLAY_LOST_ACK_EXAMPLE")
def registry() -> LocalRegistry: return LocalRegistry({"refund-sender"}, {"refund-recipient"}, "refund-recipient")
def resolver_for(bundle: dict): return _build_resolver(runtime_proposal_model(bundle), now=parse_time(EVAL))


def run_ok(bundle: dict, tmp_path: Path, **kwargs):
    destination = DestinationState(tmp_path / "destination.sqlite")
    msg = kwargs.pop("message", make_message(bundle))
    resolver = kwargs.pop("resolver", resolver_for(bundle))
    return run_exchange(msg, bundle, registry(), destination, evaluation_time=EVAL, manifest=manifest(), store_path=tmp_path / "exchange.sqlite", resolver=resolver, **kwargs), destination


def test_valid_bounded_request_uses_actual_workflow_and_moltbot_sqlite_destination(tmp_path):
    result, destination = run_ok(success_bundle(), tmp_path)
    assert result["assessment"]["stages"]["authority"] == "authorized"
    assert result["execution"]["attempted"] is True
    assert result["execution"]["newly_executed"] is True
    assert len(destination.effects) == 1
    current = result["current_reconstruction_bundle"]
    facts = execution_facts(current)
    assert result["execution"]["effect_id"] == facts["effect_id"]
    assert result["execution"]["attempt_id"] in facts["attempts"]["control_plane"]
    assert result["odes_reference"]["odes_package"]["record"]["schema_version"] == "0.1"


def test_missing_resolver_fails_closed(tmp_path):
    destination = DestinationState(tmp_path / "destination.sqlite")
    with pytest.raises(ValueError, match="trusted resolver is required"):
        run_exchange(make_message(success_bundle()), success_bundle(), registry(), destination, evaluation_time=EVAL, manifest=manifest(), store_path=tmp_path / "exchange.sqlite")
    assert destination.effects == {}


def test_structurally_valid_rehashed_actor_or_scope_change_cannot_acquire_authority(tmp_path):
    for mutate in ("actor", "principal", "scope"):
        bundle = copy.deepcopy(success_bundle())
        proposal = next(r for r in bundle["records"] if r["record_type"] == "runtime_proposal")["data"]
        if mutate == "actor": proposal["actor"] = "urn:cognous:identity:attacker"
        elif mutate == "principal": proposal["principal"] = "urn:cognous:principal:attacker"
        else: proposal["requested_permissions"] = ["refund.issue.high"]
        proposal["payload_commitment"] = digest(proposal["payload"])
        destination = DestinationState(tmp_path / mutate / "destination.sqlite")
        resolver = resolver_for(success_bundle())
        result = run_exchange(make_message(bundle), bundle, registry(), destination, evaluation_time=EVAL, manifest=manifest(), store_path=tmp_path / mutate / "exchange.sqlite", resolver=resolver)
        assert result["execution"]["attempted"] is False
        assert destination.effects == {}


def test_fabricated_authorized_record_cannot_execute(tmp_path):
    fake = copy.deepcopy(success_bundle())
    proposal = next(r for r in fake["records"] if r["record_type"] == "runtime_proposal")["data"]
    proposal["target"] = "urn:cognous:synthetic-account:attacker"
    proposal["payload"]["refund_reason"] = "fabricated"
    proposal["payload_commitment"] = digest(proposal["payload"])
    fake["records"] = [r for r in fake["records"] if r.get("record_type") in {"runtime_proposal", "runtime_decision"}]
    for record in fake["records"]:
        if record["record_type"] == "runtime_decision":
            record["data"] = {"decision_id": "caller-decision", "result": "authorized", "effect_id": "caller-effect", "binding": {"proposal_commitment": digest(proposal)}}
    destination = DestinationState(tmp_path / "destination.sqlite")
    result = run_exchange(make_message(fake), fake, registry(), destination, evaluation_time=EVAL, manifest=manifest(), store_path=tmp_path / "exchange.sqlite", resolver=resolver_for(success_bundle()))
    assert result["execution"]["attempted"] is False
    assert destination.effects == {}


def test_report_refuse_not_understood_never_execute(tmp_path):
    for mt in ("REPORT", "REFUSE", "NOT_UNDERSTOOD"):
        result, destination = run_ok(success_bundle(), tmp_path / mt, message=make_message(success_bundle(), message_type=mt))
        assert result["assessment"]["permitted_handling"] == "INFORMATIONAL_ONLY"
        assert result["execution"]["attempted"] is False
        assert destination.effects == {}


def test_wrong_sender_recipient_or_expired_message_no_effect(tmp_path):
    for kwargs in ({"sender": "unknown"}, {"recipient": "other"}, {"expires_at": "2020-01-01T00:00:00Z"}):
        result, destination = run_ok(success_bundle(), tmp_path / str(kwargs), message=make_message(success_bundle(), **kwargs))
        assert result["execution"]["attempted"] is False
        assert destination.effects == {}


def test_changed_payload_target_or_proposal_commitment_rejects():
    bundle = success_bundle(); msg = make_message(bundle)
    for mutate in ("payload", "target", "proposal"):
        trial = copy.deepcopy(msg)
        if mutate == "payload": trial["requested_action"]["payload"] = {"tampered": True}
        elif mutate == "target": trial["requested_action"]["target"] = "refunds/attacker"
        else: trial["proposal_commitment"] = "sha256:" + "0" * 64
        trial["message_digest"] = digest({k: v for k, v in trial.items() if k != "message_digest"})
        result = assess_message(trial, bundle, registry(), evaluation_time=EVAL)
        assert result["permitted_handling"] == "REFUSE"
        assert "GAX-REFERENCE-OPERATION-BINDING-MISMATCH" in result["errors"]


def test_revocation_after_receipt_prevents_execution(tmp_path):
    bundle = success_bundle(); resolver = resolver_for(bundle)
    grant_id = next(iter(resolver.statuses))
    def revoke(r): r.statuses[grant_id].status = "revoked"
    result, destination = run_ok(bundle, tmp_path, resolver=resolver, mutate_resolver_after_decision=revoke)
    assert result["execution"]["attempted"] is False
    assert destination.effects == {}


def test_durable_restart_after_commit_before_ack_reconciles_existing_effect(tmp_path):
    bundle = success_bundle(); resolver = resolver_for(bundle)
    dest1 = DestinationState(tmp_path / "destination.sqlite")
    first = run_exchange(make_message(bundle, message_id="m1"), bundle, registry(), dest1, evaluation_time=EVAL, manifest=manifest(), store_path=tmp_path / "exchange.sqlite", resolver=resolver, lose_ack=True)
    assert first["execution"]["attempted"] is True
    assert first["execution"]["attempt_status"] == "unknown"
    assert len(dest1.effects) == 1
    dest2 = DestinationState(tmp_path / "destination.sqlite")
    second = run_exchange(make_message(bundle, message_id="m2"), bundle, registry(), dest2, evaluation_time="2026-08-08T01:02:00Z", manifest=manifest(), store_path=tmp_path / "exchange.sqlite", resolver=resolver_for(bundle))
    assert second["execution"]["newly_executed"] is False
    assert len(dest2.effects) == 1


def test_new_message_id_replay_and_message_id_reuse(tmp_path):
    bundle = success_bundle(); dest = DestinationState(tmp_path / "destination.sqlite")
    first = run_exchange(make_message(bundle, message_id="m1"), bundle, registry(), dest, evaluation_time=EVAL, manifest=manifest(), store_path=tmp_path / "exchange.sqlite", resolver=resolver_for(bundle))
    second = run_exchange(make_message(bundle, message_id="m2"), bundle, registry(), dest, evaluation_time="2026-08-08T01:01:00Z", manifest=manifest(), store_path=tmp_path / "exchange.sqlite", resolver=resolver_for(bundle))
    assert first["execution"]["newly_executed"] is True
    assert second["execution"]["newly_executed"] is False
    changed = make_message(bundle, message_id="m1"); changed["content"]["summary"] = "changed"; changed["content_digest"] = digest(changed["content"]); changed["message_digest"] = digest({k: v for k, v in changed.items() if k != "message_digest"})
    third = run_exchange(changed, bundle, registry(), dest, evaluation_time="2026-08-08T01:02:00Z", manifest=manifest(), store_path=tmp_path / "exchange.sqlite", resolver=resolver_for(bundle))
    assert third["execution"]["attempted"] is False
    assert "GAX-INTEGRITY-MESSAGE-ID-REUSE" in third["assessment"]["errors"]


def test_partial_delivery_and_lost_ack_use_new_records(tmp_path):
    partial, _ = run_ok(success_bundle(), tmp_path / "partial", partial_delivery=True)
    assert partial["execution"]["destination_observed"] == "partial"
    assert execution_facts(partial["current_reconstruction_bundle"])["destination_observed"] == "partial"
    lost, _ = run_ok(success_bundle(), tmp_path / "lost", lose_ack=True)
    assert lost["execution"]["attempt_status"] == "unknown"
    assert execution_facts(lost["current_reconstruction_bundle"])["destination_observed"] == "applied"


def test_same_effect_id_different_operation_rejects(tmp_path):
    bundle = success_bundle(); dest = DestinationState(tmp_path / "destination.sqlite")
    first = run_exchange(make_message(bundle, message_id="m1"), bundle, registry(), dest, evaluation_time=EVAL, manifest=manifest(), store_path=tmp_path / "exchange.sqlite", resolver=resolver_for(bundle))
    store = TransactionalExchangeStore(tmp_path / "exchange.sqlite")
    ok, code = store.bind_effect(first["execution"]["effect_id"], "sha256:" + "0" * 64)
    assert ok is False
    assert code == "GAX-EFFECT-ID-REUSE-WITH-DIFFERENT-OPERATION"


def test_successor_verifies_source_identity_commitment_and_lineage(tmp_path):
    bundle = success_bundle(); msg = make_message(bundle); dest = DestinationState(tmp_path / "destination.sqlite")
    packet = make_successor_packet(msg, facts={"decision_id":"d","effect_id":"e","attempts":{},"pending_effects":[],"unresolved_delivery":False}, state_version=1)
    loaded = load_successor_packet(packet, dest, predecessor=msg, store_path=tmp_path / "exchange.sqlite")
    assert loaded["loaded"] is True
    stale = copy.deepcopy(packet); stale["state_version"] = "state-1"; stale["packet_digest"] = digest({k:v for k,v in stale.items() if k != "packet_digest"})
    assert load_successor_packet(stale, dest, predecessor=msg, store_path=tmp_path / "exchange.sqlite")["status"] in {"duplicate_packet", "stale_or_rollback"}
    bad = copy.deepcopy(packet); bad["source_packet_id"] = "wrong"; bad["state_version"] = "state-2"; bad["packet_digest"] = digest({k:v for k,v in bad.items() if k != "packet_digest"})
    assert load_successor_packet(bad, dest, predecessor=msg, store_path=tmp_path / "exchange2.sqlite")["status"] == "source_identity_mismatch"
    other = copy.deepcopy(packet); other["state_version"] = "state-3"; other["predecessor_packet_id"] = "unaccepted"; other["packet_id"] = "succ-other"; other["packet_digest"] = digest({k:v for k,v in other.items() if k != "packet_digest"})
    assert load_successor_packet(other, dest, predecessor=msg, store_path=tmp_path / "exchange.sqlite")["status"] == "undeclared_branch"


def test_unsupported_versions_and_downgrade_fail():
    msg = make_message(success_bundle()); msg["profile"] = "https://opendecisionevidence.org/profiles/odex-gax/0.2"; msg["message_digest"] = digest({k:v for k,v in msg.items() if k != "message_digest"})
    result = assess_message(msg, success_bundle(), registry(), evaluation_time=EVAL)
    assert "GAX-PROFILE-UNSUPPORTED-OR-DOWNGRADE" in result["errors"]


def test_odes_policy_contract_preserves_unavailable_authority(tmp_path):
    result, _ = run_ok(success_bundle(), tmp_path)
    ref = result["odes_reference"]
    assert "recipient_reliance_decision" in ref["recipient_validation"]
    assert ref["odes_package"]["provenance"]["execution_facts"]["effect_id"] == result["execution"]["effect_id"]
