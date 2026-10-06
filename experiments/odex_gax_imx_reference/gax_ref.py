from __future__ import annotations

import argparse
import json
import hashlib
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROFILE = "urn:cognous:profiles:odex-gax-imx-refund-exchange:0.1.0"
PROTOCOL_VERSION = "0.1.0"
SUPPORTED_TYPES = {"PROPOSE", "REQUEST", "REPORT", "REFUSE", "NOT_UNDERSTOOD"}


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_bytes(value)).hexdigest()


def parse_time(value: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp must be a string")
    text = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def records(bundle: dict[str, Any], record_type: str) -> list[dict[str, Any]]:
    return [r for r in bundle.get("records", []) if r.get("record_type") == record_type]


def first_record(bundle: dict[str, Any], record_type: str) -> dict[str, Any] | None:
    values = records(bundle, record_type)
    return values[0] if values else None


def data(record: dict[str, Any] | None) -> dict[str, Any]:
    return deepcopy(record.get("data", {})) if isinstance(record, dict) else {}


def proposal_from_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    proposal = data(first_record(bundle, "runtime_proposal"))
    if not proposal:
        raise ValueError("runtime_proposal is required")
    return proposal


def decision_from_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    decision = data(first_record(bundle, "runtime_decision"))
    if not decision:
        raise ValueError("runtime_decision is required")
    return decision


def proposal_commitment(proposal: dict[str, Any]) -> str:
    return digest(proposal)


def operation_commitment(proposal: dict[str, Any]) -> str:
    relevant = {
        "actor": proposal.get("actor"),
        "principal": proposal.get("principal"),
        "manifest_id": proposal.get("manifest_id"),
        "manifest_version": proposal.get("manifest_version"),
        "action_id": proposal.get("action_id"),
        "adapter_id": proposal.get("adapter_id"),
        "target": proposal.get("target"),
        "payload": proposal.get("payload"),
        "payload_commitment": proposal.get("payload_commitment"),
        "requested_permissions": proposal.get("requested_permissions"),
        "amount": proposal.get("amount"),
        "unit": proposal.get("unit"),
    }
    return digest(relevant)


@dataclass
class LocalRegistry:
    trusted_senders: set[str]
    trusted_recipients: set[str]
    recipient: str
    relying_purpose: str = "refund_execution_assessment"

    def sender_trusted(self, sender: str) -> bool:
        return sender in self.trusted_senders

    def recipient_known(self, recipient: str) -> bool:
        return recipient in self.trusted_recipients


@dataclass
class DestinationState:
    effects: dict[str, dict[str, Any]] = field(default_factory=dict)

    def apply(self, effect_id: str, operation: dict[str, Any], state: str = "applied") -> dict[str, Any]:
        if effect_id in self.effects:
            return {"newly_executed": False, "effect_id": effect_id, "state": self.effects[effect_id]["state"]}
        self.effects[effect_id] = {"operation": deepcopy(operation), "state": state, "digest": digest(operation)}
        return {"newly_executed": True, "effect_id": effect_id, "state": state}


def make_message(bundle: dict[str, Any], *, message_id: str = "msg-refund-001", recipient: str = "refund-recipient", sender: str = "refund-sender", message_type: str = "PROPOSE", created_at: str = "2026-08-08T00:00:00Z", expires_at: str = "2026-08-09T00:00:00Z") -> dict[str, Any]:
    proposal = proposal_from_bundle(bundle)
    content = {"summary": "Bounded synthetic refund proposal", "note": "Narrative content is not authority."}
    message = {
        "message_id": message_id,
        "conversation_id": "conv-refund-001",
        "profile": PROFILE,
        "protocol_version": PROTOCOL_VERSION,
        "message_type": message_type,
        "created_at": created_at,
        "expires_at": expires_at,
        "sender": sender,
        "recipient": recipient,
        "purpose": "refund_execution_assessment",
        "requested_action": {
            "action_id": proposal.get("action_id"),
            "target": proposal.get("target"),
            "payload": proposal.get("payload"),
            "amount": proposal.get("amount"),
            "unit": proposal.get("unit"),
            "actor": proposal.get("actor"),
            "principal": proposal.get("principal"),
        },
        "operation_commitment": operation_commitment(proposal),
        "proposal_commitment": proposal_commitment(proposal),
        "authority_refs": [{"type": "control_plane_decision", "ref": decision_from_bundle(bundle).get("decision_id", "unknown")}],
        "evidence_refs": [{"type": "replay_bundle", "ref": bundle.get("bundle_id", bundle.get("run_id", "unknown")), "digest": digest(bundle)}],
        "content": content,
        "content_digest": digest(content),
        "acknowledgement_requested": True,
    }
    message["message_digest"] = digest({k: v for k, v in message.items() if k != "message_digest"})
    return message


def assess_message(message: dict[str, Any], bundle: dict[str, Any], registry: LocalRegistry, *, evaluation_time: str, seen_messages: dict[str, str] | None = None) -> dict[str, Any]:
    seen_messages = seen_messages if seen_messages is not None else {}
    stages: dict[str, str] = {}
    errors: list[str] = []
    def fail(stage: str, code: str) -> dict[str, Any]:
        stages[stage] = "failed"
        errors.append(code)
        return {"permitted_handling": "REFUSE", "stages": stages, "errors": errors, "message_received": True, "message_understood": stage not in {"structure", "profile"}}

    required = ["message_id", "conversation_id", "profile", "protocol_version", "message_type", "created_at", "expires_at", "sender", "recipient", "purpose", "requested_action", "operation_commitment", "proposal_commitment", "authority_refs", "evidence_refs", "content", "content_digest", "message_digest"]
    if any(key not in message for key in required):
        return fail("structure", "GAX-SCHEMA-MISSING-REQUIRED")
    expected_message_digest = digest({k: v for k, v in message.items() if k != "message_digest"})
    if message["message_digest"] != expected_message_digest:
        return fail("structure", "GAX-INTEGRITY-MESSAGE-DIGEST")
    if message["content_digest"] != digest(message["content"]):
        return fail("structure", "GAX-INTEGRITY-CONTENT-DIGEST")
    stages["structure"] = "passed"
    if message["profile"] != PROFILE or message["protocol_version"] != PROTOCOL_VERSION:
        return fail("profile", "GAX-PROFILE-UNSUPPORTED-OR-DOWNGRADE")
    if message["message_type"] not in SUPPORTED_TYPES:
        return fail("profile", "GAX-PROFILE-UNSUPPORTED-MESSAGE-TYPE")
    stages["profile"] = "passed"
    prior = seen_messages.get(message["message_id"])
    if prior is not None and prior != message["message_digest"]:
        return fail("identity", "GAX-INTEGRITY-MESSAGE-ID-REUSE")
    if not registry.sender_trusted(message["sender"]):
        return fail("identity", "GAX-IDENTITY-UNKNOWN-SENDER")
    if message["recipient"] != registry.recipient or not registry.recipient_known(message["recipient"]):
        return fail("identity", "GAX-IDENTITY-WRONG-RECIPIENT")
    stages["identity"] = "passed"
    now = parse_time(evaluation_time)
    if parse_time(message["created_at"]) > now or parse_time(message["expires_at"]) <= now:
        return fail("freshness", "GAX-TASK-EXPIRED")
    stages["freshness"] = "passed"
    proposal = proposal_from_bundle(bundle)
    if message["proposal_commitment"] != proposal_commitment(proposal):
        return fail("reference_resolution", "GAX-REFERENCE-PROPOSAL-COMMITMENT-MISMATCH")
    if message["operation_commitment"] != operation_commitment(proposal):
        return fail("reference_resolution", "GAX-REFERENCE-OPERATION-COMMITMENT-MISMATCH")
    requested = message["requested_action"]
    for key in ("action_id", "target", "payload", "amount", "unit"):
        if requested.get(key) != proposal.get(key):
            return fail("reference_resolution", f"GAX-REFERENCE-REQUESTED-{key.upper()}-MISMATCH")
    stages["reference_resolution"] = "passed"
    decision = decision_from_bundle(bundle)
    result = str(decision.get("result") or decision.get("status") or "unknown").lower()
    if result in {"authorized", "allow", "granted"}:
        binding = decision.get("binding")
        if not isinstance(binding, dict):
            return fail("authority", "GAX-AUTHORITY-MISSING-BINDING")
        if binding.get("proposal_commitment") != proposal_commitment(proposal):
            return fail("authority", "GAX-AUTHORITY-PROPOSAL-COMMITMENT-MISMATCH")
        stages["authority"] = "authorized"
        permitted = "ACCEPT_FOR_EXECUTION"
    elif result in {"hold", "held", "requires_review"}:
        stages["authority"] = "held"
        permitted = "REFUSE_HUMAN_DECISION_REQUIRED"
    elif result in {"deny", "denied", "blocked", "reject"}:
        stages["authority"] = "denied"
        permitted = "REFUSE_AUTHORITY_INVALID"
    else:
        stages["authority"] = "unresolved"
        permitted = "DEFER"
    return {"permitted_handling": permitted, "stages": stages, "errors": errors, "message_received": True, "message_understood": True}


def execution_facts(bundle: dict[str, Any]) -> dict[str, Any]:
    decision = decision_from_bundle(bundle)
    proposal = proposal_from_bundle(bundle)
    effect_id = decision.get("effect_id") or (decision.get("binding") or {}).get("effect_id")
    destination_effects = [data(r) for r in records(bundle, "destination_effect")]
    observations = [data(r) for r in records(bundle, "effect_observation")]
    reconciliations = [data(r) for r in records(bundle, "reconciliation")]
    attempts = [data(r) for r in records(bundle, "destination_attempt")]
    cp_attempts = [data(r) for r in records(bundle, "control_plane_attempt_transition")]
    states = [x.get("state") or x.get("status") for x in destination_effects + observations + reconciliations if x.get("state") or x.get("status")]
    if "partial" in states:
        observed = "partial"
    elif "applied" in states or "success" in states:
        observed = "applied"
    elif states:
        observed = str(states[0])
    else:
        observed = "unavailable"
    ack = "unknown"
    if any(str(a.get("status", "")).lower() == "acknowledged" for a in cp_attempts):
        ack = "received"
    if any(str(a.get("acknowledgement", "")).lower() in {"lost", "unknown"} for a in cp_attempts):
        ack = "unknown"
    return {"effect_id": effect_id, "proposal_commitment": proposal_commitment(proposal), "attempts": {"control_plane": cp_attempts, "executor": attempts}, "acknowledgement": ack, "destination_observed": observed, "destination_effects": destination_effects, "observations": observations, "reconciliations": reconciliations}


def run_exchange(message: dict[str, Any], bundle: dict[str, Any], registry: LocalRegistry, destination: DestinationState, *, evaluation_time: str, seen_messages: dict[str, str] | None = None, authority_override: str | None = None) -> dict[str, Any]:
    seen_messages = seen_messages if seen_messages is not None else {}
    assessment = assess_message(message, bundle, registry, evaluation_time=evaluation_time, seen_messages=seen_messages)
    if assessment["errors"]:
        return {"assessment": assessment, "execution": {"attempted": False}, "destination_state": deepcopy(destination.effects)}
    seen_messages[message["message_id"]] = message["message_digest"]
    facts = execution_facts(bundle)
    if authority_override in {"revoked", "policy_changed"}:
        assessment["permitted_handling"] = "REFUSE_AUTHORITY_INVALID"
        assessment["stages"]["authority"] = authority_override
    if assessment["permitted_handling"] != "ACCEPT_FOR_EXECUTION":
        return {"assessment": assessment, "execution": {"attempted": False, "reason": assessment["permitted_handling"]}, "destination_state": deepcopy(destination.effects), "facts": facts}
    proposal = proposal_from_bundle(bundle)
    effect_id = facts["effect_id"] or digest({"proposal": proposal})
    state = "partial" if facts["destination_observed"] == "partial" else "applied"
    applied = destination.apply(effect_id, proposal, state=state)
    return {"assessment": assessment, "execution": {"attempted": True, **applied}, "destination_state": deepcopy(destination.effects), "facts": facts}


def make_successor_packet(previous: dict[str, Any], *, next_work: str = "reconcile_before_retry") -> dict[str, Any]:
    packet = {
        "packet_id": "successor-refund-001",
        "profile": "urn:cognous:profiles:odex-imx-continuity:0.1.0",
        "source_packet_id": previous.get("message_id") or previous.get("packet_id"),
        "source_commitment": digest(previous),
        "state_version": "state-1",
        "predecessor": previous.get("message_id") or previous.get("packet_id"),
        "decisions": previous.get("authority_refs", []),
        "evidence_refs": previous.get("evidence_refs", []),
        "pending_effects": [],
        "attempts": [],
        "unresolved_delivery": ["independent_verification_unavailable"],
        "conflicts": [],
        "missing_evidence": [],
        "supersessions": [],
        "corrections": [],
        "next_proposed_work": next_work,
        "loading_semantics": "load_only_no_authority_no_execution",
        "loss_report": []
    }
    packet["packet_digest"] = digest({k: v for k, v in packet.items() if k != "packet_digest"})
    return packet


def load_successor_packet(packet: dict[str, Any], destination: DestinationState, *, expected_predecessor: str | None = None) -> dict[str, Any]:
    if packet.get("packet_digest") != digest({k: v for k, v in packet.items() if k != "packet_digest"}):
        return {"loaded": False, "effect_created": False, "status": "integrity_failed"}
    if expected_predecessor and packet.get("predecessor") != expected_predecessor:
        return {"loaded": False, "effect_created": False, "status": "divergent_history"}
    if packet.get("state_version", "") < "state-1":
        return {"loaded": False, "effect_created": False, "status": "stale_or_rollback"}
    return {"loaded": True, "effect_created": False, "status": "loaded_for_reconciliation", "destination_state": deepcopy(destination.effects)}


def export_odes_reference(bundle: dict[str, Any]) -> dict[str, Any]:
    facts = execution_facts(bundle)
    decision = decision_from_bundle(bundle)
    return {"record_type": "odes_reference", "schema_version": "pder-v0.1", "decision_id": decision.get("decision_id"), "decision_result": decision.get("result"), "effect_id": facts.get("effect_id"), "destination_observed": facts.get("destination_observed"), "acknowledgement": facts.get("acknowledgement"), "digest": digest({"decision": decision, "facts": facts}), "limits": ["informational inspection only", "no issuer authentication", "no independent real-world effect verification"]}


def run_demo(manifest_path: str, replay_path: str, out_path: str) -> dict[str, Any]:
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    bundle = json.loads(Path(replay_path).read_text(encoding="utf-8"))
    _ = manifest
    registry = LocalRegistry({"refund-sender"}, {"refund-recipient"}, "refund-recipient")
    destination = DestinationState()
    seen: dict[str, str] = {}
    message = make_message(bundle)
    result = run_exchange(message, bundle, registry, destination, evaluation_time="2026-08-08T01:00:00Z", seen_messages=seen)
    successor = make_successor_packet(message)
    loaded = load_successor_packet(successor, destination, expected_predecessor=message["message_id"])
    output = {"message": message, "exchange_result": result, "odes_reference": export_odes_reference(bundle), "successor_packet": successor, "successor_load": loaded}
    Path(out_path).write_text(json.dumps(output, indent=2, sort_keys=True), encoding="utf-8")
    return output


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--replay", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    run_demo(args.manifest, args.replay, args.out)


if __name__ == "__main__":
    main()
