from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sqlite3
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROFILE = "urn:cognous:profiles:odex-gax-imx-refund-exchange:0.1.0"
PROTOCOL_VERSION = "0.1.0"
SUPPORTED_TYPES = {"PROPOSE", "REQUEST", "REPORT", "REFUSE", "NOT_UNDERSTOOD"}
INFORMATIONAL_TYPES = {"REPORT", "REFUSE", "NOT_UNDERSTOOD"}
PROFILE_AUTH_CONTEXT = "urn:cognous:alvorada:public-stack-profile:0.1.0"
INSTITUTION = "urn:cognous:institution:synthetic-customer-service"
ISSUER = "urn:cognous:principal:synthetic-governor"
ISSUER_ROLE = "urn:cognous:role:refund-governor"
POLICY_REF = "urn:cognous:policy:refund-policy"
POLICY_VERSION = "1.0"
TRUSTED_ACTOR = "urn:cognous:identity:refund-agent-1"
TRUSTED_PRINCIPAL = "urn:cognous:principal:refund-service"
TRUSTED_TARGET = "urn:cognous:synthetic-account:customer-001"
TRUSTED_PERMISSION = "refund.issue.routine"
TRUSTED_GRANT_ID = "urn:cognous:grant:routine-1"
EVAL = "2026-08-08T01:00:00Z"


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_bytes(value)).hexdigest()


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def records(bundle: dict[str, Any], record_type: str) -> list[dict[str, Any]]:
    return [r for r in bundle.get("records", []) if r.get("record_type") == record_type]


def first_record(bundle: dict[str, Any], record_type: str) -> dict[str, Any] | None:
    items = records(bundle, record_type)
    return items[0] if items else None


def data(record: dict[str, Any] | None) -> dict[str, Any]:
    return copy.deepcopy(record.get("data", {})) if isinstance(record, dict) else {}


def proposal_from_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    proposal = data(first_record(bundle, "runtime_proposal"))
    if not proposal:
        raise ValueError("runtime_proposal is required")
    return proposal


def _cp():
    from agent_control_plane.bounded import (
        ApprovalStatus, BoundedAuthorizationWorkflow, BoundedRecordStore, ConflictStatus,
        EffectObservation, EvidenceStatus, GrantStatus, IdentityStatus, MandateStatus,
        PolicyStatus, RoleMappingStatus, RuntimeProposal, SyntheticResolver, commitment,
    )
    return locals()


def cp_commitment(value: Any) -> str:
    return _cp()["commitment"](value)


def runtime_proposal_model(bundle: dict[str, Any]):
    raw = copy.deepcopy(proposal_from_bundle(bundle))
    raw.setdefault("authority_context_ref", PROFILE_AUTH_CONTEXT)
    raw.setdefault("requirement_id", "urn:cognous:authority-requirement:refund-routine-v1")
    raw.setdefault("evidence_refs", ["urn:cognous:evidence:refund-entitlement"])
    raw.setdefault("effects", 1)
    raw.setdefault("run_id", bundle.get("run_id") or "run-gax-imx")
    return _cp()["RuntimeProposal"].model_validate(raw)


def proposal_commitment(proposal: dict[str, Any] | None = None, *, model: Any | None = None) -> str:
    model = model or _cp()["RuntimeProposal"].model_validate(proposal)
    return cp_commitment(model.model_dump(mode="json", exclude_none=False))


def operation_commitment(proposal: dict[str, Any] | None = None, *, model: Any | None = None) -> str:
    model = model or _cp()["RuntimeProposal"].model_validate(proposal)
    p = model.model_dump(mode="json", exclude_none=False)
    return cp_commitment({"actor": p.get("actor"), "principal": p.get("principal"), "manifest_id": p.get("manifest_id"), "manifest_version": p.get("manifest_version"), "action_id": p.get("action_id"), "adapter_id": p.get("adapter_id"), "target": p.get("target"), "payload": p.get("payload"), "payload_commitment": p.get("payload_commitment"), "requested_permissions": p.get("requested_permissions"), "amount": p.get("amount"), "unit": p.get("unit"), "effects": p.get("effects"), "authority_context_ref": p.get("authority_context_ref"), "requirement_id": p.get("requirement_id")})


@dataclass
class LocalRegistry:
    trusted_senders: set[str]
    trusted_recipients: set[str]
    recipient: str
    relying_purpose: str = "refund_execution_assessment"
    def sender_trusted(self, sender: str) -> bool: return sender in self.trusted_senders
    def recipient_known(self, recipient: str) -> bool: return recipient in self.trusted_recipients


class TransactionalExchangeStore:
    def __init__(self, path: str | Path):
        self.path = Path(path); self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as con:
            con.execute("create table if not exists messages(message_id text primary key, message_digest text not null, conversation_id text not null)")
            con.execute("create table if not exists effects(effect_id text primary key, operation_digest text not null)")
            con.execute("create table if not exists lineage(packet_id text primary key, packet_digest text not null, conversation_id text not null, parent_packet_id text, state_version integer not null, source_commitment text not null)")
            con.execute("create table if not exists heads(conversation_id text primary key, packet_id text, state_version integer not null)")
    def seen_messages(self) -> dict[str, str]:
        with sqlite3.connect(self.path) as con: return {r[0]: r[1] for r in con.execute("select message_id,message_digest from messages")}
    def record_message(self, message: dict[str, Any]) -> tuple[bool, str | None]:
        with sqlite3.connect(self.path, isolation_level="IMMEDIATE") as con:
            row = con.execute("select message_digest from messages where message_id=?", (message["message_id"],)).fetchone()
            if row and row[0] != message["message_digest"]: return False, "GAX-INTEGRITY-MESSAGE-ID-REUSE"
            if row: return True, None
            con.execute("insert into messages values(?,?,?)", (message["message_id"], message["message_digest"], message["conversation_id"])); return False, None
    def bind_effect(self, effect_id: str, operation_hash: str) -> tuple[bool, str | None]:
        with sqlite3.connect(self.path, isolation_level="IMMEDIATE") as con:
            row = con.execute("select operation_digest from effects where effect_id=?", (effect_id,)).fetchone()
            if row and row[0] != operation_hash: return False, "GAX-EFFECT-ID-REUSE-WITH-DIFFERENT-OPERATION"
            if row: return True, None
            con.execute("insert into effects values(?,?)", (effect_id, operation_hash)); return False, None
    def accept_successor(self, packet: dict[str, Any], predecessor: dict[str, Any]) -> dict[str, Any]:
        if packet.get("packet_digest") != digest({k: v for k, v in packet.items() if k != "packet_digest"}): return {"loaded": False, "effect_created": False, "status": "packet_digest_mismatch"}
        if packet.get("source_packet_id") not in {predecessor.get("message_id"), predecessor.get("packet_id")}: return {"loaded": False, "effect_created": False, "status": "source_identity_mismatch"}
        if packet.get("source_commitment") != digest(predecessor): return {"loaded": False, "effect_created": False, "status": "source_commitment_mismatch"}
        if packet.get("profile") != PROFILE or packet.get("schema_version") != PROTOCOL_VERSION: return {"loaded": False, "effect_created": False, "status": "unsupported_profile_or_schema"}
        try: version = int(str(packet.get("state_version", "")).split("-")[-1])
        except Exception: return {"loaded": False, "effect_created": False, "status": "unsupported_state_version"}
        conversation_id = packet.get("conversation_id") or predecessor.get("conversation_id") or "default"; parent = packet.get("predecessor_packet_id")
        with sqlite3.connect(self.path, isolation_level="IMMEDIATE") as con:
            head = con.execute("select packet_id,state_version from heads where conversation_id=?", (conversation_id,)).fetchone()
            if head:
                if version <= int(head[1]):
                    existing = con.execute("select packet_digest from lineage where packet_id=?", (packet["packet_id"],)).fetchone()
                    if existing and existing[0] == packet["packet_digest"]: return {"loaded": True, "effect_created": False, "status": "duplicate_packet"}
                    return {"loaded": False, "effect_created": False, "status": "stale_or_rollback"}
                if parent and parent != head[0]: return {"loaded": False, "effect_created": False, "status": "undeclared_branch"}
            elif parent: return {"loaded": False, "effect_created": False, "status": "unknown_predecessor"}
            row = con.execute("select packet_digest from lineage where packet_id=?", (packet["packet_id"],)).fetchone()
            if row and row[0] != packet["packet_digest"]: return {"loaded": False, "effect_created": False, "status": "divergent_history"}
            con.execute("insert or ignore into lineage values(?,?,?,?,?,?)", (packet["packet_id"], packet["packet_digest"], conversation_id, parent, version, packet["source_commitment"])); con.execute("insert or replace into heads values(?,?,?)", (conversation_id, packet["packet_id"], version))
            return {"loaded": True, "effect_created": False, "status": "loaded_for_reconciliation"}
DurableExchangeStore = TransactionalExchangeStore


class MoltbotSqliteDestination:
    def __init__(self, path: str | Path):
        self.path = Path(path); self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as con:
            con.execute("create table if not exists effects(effect_id text primary key, grant_id text, operation_digest text, target text, amount real, unit text, payload text, state text)"); con.execute("create table if not exists grant_counts(grant_id text primary key, count integer not null)")
    def apply(self, *, effect_id: str, grant_id: str, max_effects: int, target: str, amount: float, unit: str, payload: dict, lose_ack: bool = False, partial: bool = False) -> dict[str, Any]:
        op_digest = digest({"target": target, "amount": amount, "unit": unit, "payload": payload})
        with sqlite3.connect(self.path, isolation_level="IMMEDIATE") as con:
            prior = con.execute("select operation_digest,target,amount,unit,payload,state from effects where effect_id=?", (effect_id,)).fetchone()
            if prior:
                if prior[0] != op_digest: raise RuntimeError("same effect_id reused with different operation content")
                return {"duplicate": True, "effect": {"effect_id": effect_id, "grant_id": grant_id, "target": prior[1], "amount": prior[2], "unit": prior[3], "payload": json.loads(prior[4]), "state": prior[5]}}
            used = con.execute("select count from grant_counts where grant_id=?", (grant_id,)).fetchone(); count = int(used[0]) if used else 0
            if count >= max_effects: raise RuntimeError("cumulative grant max_effects exhausted")
            state = "partial" if partial else "applied"
            con.execute("insert into effects values(?,?,?,?,?,?,?,?)", (effect_id, grant_id, op_digest, target, amount, unit, json.dumps(payload, sort_keys=True), state)); con.execute("insert or replace into grant_counts values(?,?)", (grant_id, count + 1))
            if lose_ack:
                con.commit(); raise TimeoutError("synthetic acknowledgement lost after durable Moltbot commit")
            return {"duplicate": False, "effect": {"effect_id": effect_id, "grant_id": grant_id, "target": target, "amount": amount, "unit": unit, "payload": copy.deepcopy(payload), "state": state}}
    def observe(self, effect_id: str):
        EffectObservation = _cp()["EffectObservation"]
        with sqlite3.connect(self.path) as con: row = con.execute("select grant_id,target,amount,unit,payload,state from effects where effect_id=?", (effect_id,)).fetchone()
        if not row: return EffectObservation(effect_id=effect_id, observed_at=datetime.now(timezone.utc).isoformat(), state="absent")
        state = "partial" if row[5] == "partial" else "applied"
        return EffectObservation(effect_id=effect_id, observed_at=datetime.now(timezone.utc).isoformat(), state=state, destination_state={"effect_id": effect_id, "grant_id": row[0], "target": row[1], "amount": row[2], "unit": row[3], "payload": json.loads(row[4]), "state": state})
    def snapshot(self) -> dict[str, Any]:
        with sqlite3.connect(self.path) as con: rows = con.execute("select effect_id,grant_id,target,amount,unit,payload,state from effects").fetchall()
        return {"effects": {r[0]: {"effect_id": r[0], "grant_id": r[1], "target": r[2], "amount": r[3], "unit": r[4], "payload": json.loads(r[5]), "state": r[6]} for r in rows}}


@dataclass
class DestinationState:
    path: Path | None = None
    _tmp: tempfile.TemporaryDirectory | None = field(default=None, init=False, repr=False)
    _destination: Any = field(default=None, init=False, repr=False)
    def __post_init__(self) -> None:
        if self.path is None: self._tmp = tempfile.TemporaryDirectory(); self.path = Path(self._tmp.name) / "moltbot_destination.sqlite"
        self._destination = MoltbotSqliteDestination(self.path)
    @property
    def adapter(self) -> MoltbotSqliteDestination: return self._destination
    @property
    def effects(self) -> dict[str, Any]: return copy.deepcopy(self._destination.snapshot()["effects"])
    def snapshot(self) -> dict[str, Any]: return self._destination.snapshot()


def _build_resolver(proposal: Any, *, now: datetime | None = None, active: bool = True, include_grant: bool = True):
    cp = _cp(); C = cp["commitment"]; now = now or parse_time(EVAL)
    ctx = {"schema_version": "0.1.0", "interface_status": "proposed_pending_governor_review", "context_id": PROFILE_AUTH_CONTEXT, "institution": {"institution_id": INSTITUTION, "authority_domain": "customer-refunds", "authority_basis_ref": "urn:cognous:authority-basis:synthetic", "authority_mode": "principle_inspired"}, "principal": TRUSTED_PRINCIPAL, "acting_identity": TRUSTED_ACTOR, "requirement": {"requirement_id": proposal.requirement_id, "governing_sources": [{"ref": POLICY_REF, "version": POLICY_VERSION}], "permissions": [{"action": "refund_issue_routine", "targets": [TRUSTED_TARGET], "data_scopes": [TRUSTED_PERMISSION], "max_amount": 100.0, "unit": "USD", "max_effects": 1}], "approvals": [{"role_id": "urn:cognous:role:customer-service-supervisor", "independent_of_actor": True}], "evidence": [{"obligation_id": "urn:cognous:evidence:refund-entitlement", "kind": "authorization", "source_ref": "urn:cognous:source:entitlement", "max_age_seconds": 300, "required": True, "unknown_behavior": "hold_effect"}], "consequence": {"tier": "T1"}}, "conflicts": {"precedence_refs": [POLICY_REF]}, "supporting_evidence_refs": []}
    statuses = {}; approvals = {}; mandates = {}
    if include_grant:
        ctx["grant"] = {"grant_id": TRUSTED_GRANT_ID, "revision": "1", "requirement_id": proposal.requirement_id, "issuer": ISSUER, "issuer_role": ISSUER_ROLE, "issuance_record_ref": "urn:cognous:issuance:1", "grantee": TRUSTED_PRINCIPAL, "acting_identity": TRUSTED_ACTOR, "issued_at": now.isoformat(), "not_before": now.isoformat(), "expires_at": "2026-08-09T00:00:00+00:00", "status_ref": "urn:cognous:status:grant-1", "policy_versions": [{"ref": POLICY_REF, "version": POLICY_VERSION}], "permissions": [{"action": "refund_issue_routine", "targets": [TRUSTED_TARGET], "data_scopes": [TRUSTED_PERMISSION], "max_amount": 100.0, "unit": "USD", "max_effects": 1}], "delegation": {"parent_grant_id": None, "may_delegate": False, "remaining_depth": 0}, "approval_refs": ["urn:cognous:approval:t1-1"]}
        statuses[TRUSTED_GRANT_ID] = cp["GrantStatus"](grant_id=TRUSTED_GRANT_ID, revision="1", status="active" if active else "revoked", observed_at=now.isoformat(), version="status-v1", status_ref="urn:cognous:status:grant-1", authority_basis_ref="urn:cognous:authority-basis:synthetic", institution_id=INSTITUTION, authority_domain="customer-refunds")
        approvals["urn:cognous:approval:t1-1"] = cp["ApprovalStatus"](approval_ref="urn:cognous:approval:t1-1", role_id="urn:cognous:role:customer-service-supervisor", approver="urn:cognous:principal:reviewer-1", grant_id=TRUSTED_GRANT_ID, grant_revision="1", proposal_commitment=C(proposal.model_dump(mode="json", exclude_none=False)), policy_versions=[{"ref": POLICY_REF, "version": POLICY_VERSION}], observed_at=now.isoformat(), institution_id=INSTITUTION, authority_domain="customer-refunds")
        mandates[f"{ISSUER}|{ISSUER_ROLE}"] = cp["MandateStatus"](issuer=ISSUER, issuer_role=ISSUER_ROLE, issuance_record_ref="urn:cognous:issuance:1", mandate_valid=True, observed_at=now.isoformat(), institution_id=INSTITUTION, authority_domain="customer-refunds")
    aliases = {"customer-service-supervisor": ["urn:cognous:role:customer-service-supervisor"]}; mapping = cp["RoleMappingStatus"](institution_id=INSTITUTION, version="roles-v1", digest=C({"institution_id": INSTITUTION, "version": "roles-v1", "aliases": aliases}), aliases=aliases)
    return cp["SyntheticResolver"](contexts={PROFILE_AUTH_CONTEXT: ctx}, statuses=statuses, identities={TRUSTED_ACTOR: cp["IdentityStatus"](identity=TRUSTED_ACTOR, principal=TRUSTED_PRINCIPAL, authenticated=True, delegation_valid=True, observed_at=now.isoformat(), institution_id=INSTITUTION, authority_domain="customer-refunds")}, mandates=mandates, approvals=approvals, policies={POLICY_REF: cp["PolicyStatus"](ref=POLICY_REF, version=POLICY_VERSION, status="active", observed_at=now.isoformat(), institution_id=INSTITUTION, authority_domain="customer-refunds")}, conflicts={proposal.requirement_id: cp["ConflictStatus"](requirement_id=proposal.requirement_id, state="clear", observed_at=now.isoformat(), conflict_refs=[POLICY_REF], institution_id=INSTITUTION, authority_domain="customer-refunds")}, evidence={"urn:cognous:evidence:refund-entitlement": cp["EvidenceStatus"](obligation_id="urn:cognous:evidence:refund-entitlement", state="current", observed_at=now.isoformat(), source_ref="urn:cognous:source:entitlement", institution_id=INSTITUTION, authority_domain="customer-refunds")}, role_mappings={INSTITUTION: mapping})


def make_message(bundle: dict[str, Any], *, message_id: str = "msg-refund-001", recipient: str = "refund-recipient", sender: str = "refund-sender", message_type: str = "PROPOSE", created_at: str = "2026-08-08T00:00:00Z", expires_at: str = "2026-08-09T00:00:00Z") -> dict[str, Any]:
    proposal = runtime_proposal_model(bundle); p = proposal.model_dump(mode="json", exclude_none=False); content = {"summary": "Bounded synthetic refund proposal", "note": "Narrative content is not authority."}
    msg = {"message_id": message_id, "conversation_id": "conv-refund-001", "profile": PROFILE, "protocol_version": PROTOCOL_VERSION, "message_type": message_type, "created_at": created_at, "expires_at": expires_at, "sender": sender, "recipient": recipient, "purpose": "refund_execution_assessment", "requested_action": {"action_id": p["action_id"], "target": p["target"], "payload": p["payload"], "amount": p["amount"], "unit": p["unit"], "actor": p["actor"], "principal": p["principal"], "adapter_id": p["adapter_id"], "requested_permissions": p["requested_permissions"]}, "operation_commitment": operation_commitment(model=proposal), "proposal_commitment": proposal_commitment(model=proposal), "authority_refs": [{"type": "authority_context", "ref": p.get("authority_context_ref")}, {"type": "control_plane_current_authorization_required", "ref": "re-resolve-before-effect"}], "evidence_refs": [{"type": "replay_bundle", "ref": bundle.get("bundle_id", bundle.get("run_id", "unknown")), "digest": digest(bundle)}], "content": content, "content_digest": digest(content), "acknowledgement_requested": True}
    msg["message_digest"] = digest({k: v for k, v in msg.items() if k != "message_digest"}); return msg


def assess_message(message: dict[str, Any], bundle: dict[str, Any], registry: LocalRegistry, *, evaluation_time: str, seen_messages: dict[str, str] | None = None) -> dict[str, Any]:
    stages = {}; errors = []
    def fail(stage: str, code: str, handling: str = "REFUSE"):
        stages[stage] = "failed"; errors.append(code); return {"permitted_handling": handling, "stages": stages, "errors": errors, "message_received": True, "message_understood": stage not in {"structure", "profile"}}
    required = ["message_id", "conversation_id", "profile", "protocol_version", "message_type", "created_at", "expires_at", "sender", "recipient", "purpose", "requested_action", "operation_commitment", "proposal_commitment", "authority_refs", "evidence_refs", "content", "content_digest", "message_digest"]
    if any(k not in message for k in required): return fail("structure", "GAX-SCHEMA-MISSING-REQUIRED", "NOT_UNDERSTOOD")
    if message["message_digest"] != digest({k: v for k, v in message.items() if k != "message_digest"}) or message["content_digest"] != digest(message["content"]): return fail("structure", "GAX-INTEGRITY-DIGEST", "NOT_UNDERSTOOD")
    stages["structure"] = "passed"
    if message["profile"] != PROFILE or message["protocol_version"] != PROTOCOL_VERSION or message["message_type"] not in SUPPORTED_TYPES: return fail("profile", "GAX-PROFILE-UNSUPPORTED-OR-DOWNGRADE", "NOT_UNDERSTOOD")
    stages["profile"] = "passed"; prior = (seen_messages or {}).get(message["message_id"])
    if prior and prior != message["message_digest"]: return fail("identity_binding", "GAX-INTEGRITY-MESSAGE-ID-REUSE")
    if not registry.sender_trusted(message["sender"]): return fail("identity_binding", "GAX-IDENTITY-SENDER-UNTRUSTED")
    if message["recipient"] != registry.recipient or not registry.recipient_known(message["recipient"]): return fail("identity_binding", "GAX-IDENTITY-WRONG-RECIPIENT")
    stages["identity_binding"] = "passed"
    if message["purpose"] != registry.relying_purpose: return fail("purpose", "GAX-PURPOSE-UNSUPPORTED")
    if parse_time(message["expires_at"]) <= parse_time(evaluation_time): return fail("freshness", "GAX-FRESHNESS-EXPIRED")
    stages["freshness"] = "passed"; proposal = runtime_proposal_model(bundle); p = proposal.model_dump(mode="json", exclude_none=False)
    expected_req = {"action_id": p["action_id"], "target": p["target"], "payload": p["payload"], "amount": p["amount"], "unit": p["unit"], "actor": p["actor"], "principal": p["principal"], "adapter_id": p["adapter_id"], "requested_permissions": p["requested_permissions"]}
    if message["requested_action"] != expected_req or message["proposal_commitment"] != proposal_commitment(model=proposal) or message["operation_commitment"] != operation_commitment(model=proposal): return fail("reference_resolution", "GAX-REFERENCE-OPERATION-BINDING-MISMATCH")
    if not any(r.get("ref") == p.get("authority_context_ref") for r in message["authority_refs"]): return fail("authority_reference", "GAX-AUTHORITY-REFERENCE-MISMATCH")
    stages["reference_resolution"] = "passed"; stages["authority_reference"] = "present_not_authorizing"
    if message["message_type"] in INFORMATIONAL_TYPES:
        stages["communicative_act"] = "informational_no_effect"; return {"permitted_handling": "INFORMATIONAL_ONLY", "stages": stages, "errors": errors, "message_received": True, "message_understood": True}
    stages["communicative_act"] = "assessment_required"; return {"permitted_handling": "ACCEPT_FOR_ASSESSMENT", "stages": stages, "errors": errors, "message_received": True, "message_understood": True}


def _model_dict(value: Any) -> dict[str, Any]: return value.model_dump(mode="json", exclude_none=False) if hasattr(value, "model_dump") else dict(value)

def _operation_for_replay(proposal: Any, decision: Any) -> dict[str, Any]:
    p = _model_dict(proposal); d = _model_dict(decision); binding = d.get("binding") or {}; op = {k: p.get(k) for k in ("actor", "principal", "manifest_id", "manifest_version", "manifest_digest", "action_id", "adapter_id", "target", "payload", "payload_commitment", "requested_permissions", "amount", "unit", "effects", "requirement_id")}
    op.update({"authority_context_id": p.get("authority_context_ref"), "proposal_commitment": proposal_commitment(model=proposal), "grant_id": binding.get("grant_id"), "grant_revision": binding.get("grant_revision"), "effective_max_effects": binding.get("effective_max_effects") or 1, "institution_id": binding.get("institution_id", INSTITUTION), "authority_domain": binding.get("authority_domain", "customer-refunds")}); return op


def _reconstruction_from_run(manifest: dict[str, Any], proposal: Any, decision: Any, record: Any, destination: DestinationState, *, original_bundle: dict[str, Any], decision_source: str = "current_control_plane") -> dict[str, Any]:
    p = _model_dict(proposal); d = _model_dict(decision); attempts = [_model_dict(a) for a in record.attempts]; observations = [_model_dict(o) for o in record.observations]; effect_id = d.get("effect_id"); dest_effect = destination.effects.get(effect_id, {}) if effect_id else {}; recs = [{"record_id": "current-runtime-proposal", "record_type": "runtime_proposal", "producer_profile_id": "control-plane", "data": p}, {"record_id": "current-runtime-decision", "record_type": "runtime_decision", "producer_profile_id": "control-plane", "data": d}]
    if d.get("result") == "authorized" and attempts:
        op = _operation_for_replay(proposal, decision); op_digest = digest(op); aid = attempts[-1]["attempt_id"]; obs = observations[-1] if observations else {"effect_id": effect_id, "state": "unknown"}
        recs += [{"record_id": "current-control-plane-attempt", "record_type": "control_plane_attempt_transition", "producer_profile_id": "control-plane", "data": attempts[-1]}, {"record_id": "current-execution-envelope", "record_type": "execution_envelope", "producer_profile_id": "moltbot-safe", "data": {"version": "0.2.0", "envelope_id": "env-" + effect_id, "decision_id": d.get("decision_id"), "effect_id": effect_id, "attempt_id": aid, "operation": op}}, {"record_id": "current-destination-attempt", "record_type": "destination_attempt", "producer_profile_id": "moltbot-safe", "data": {"attempt_id": aid, "effect_id": effect_id, "decision_id": d.get("decision_id"), "operation_digest": op_digest, "status": attempts[-1].get("status")}}, {"record_id": "current-execution-result", "record_type": "execution_result", "producer_profile_id": "moltbot-safe", "data": {"attempt_id": aid, "effect_id": effect_id, "decision_id": d.get("decision_id"), "status": attempts[-1].get("status"), "acknowledged": attempts[-1].get("status") == "acknowledged", "newly_executed": not bool(attempts[-1].get("acknowledgement", {}).get("duplicate")), "observed_state": obs.get("state", "unknown"), "observation": obs}}]
        if dest_effect: recs.append({"record_id": "current-destination-effect", "record_type": "destination_effect", "producer_profile_id": "moltbot-safe", "data": {"effect_id": effect_id, "operation_digest": op_digest, "grant_id": op.get("grant_id"), "target": dest_effect.get("target"), "amount": dest_effect.get("amount"), "unit": dest_effect.get("unit"), "payload_json": json.dumps(dest_effect.get("payload"), sort_keys=True, separators=(",", ":")), "state": dest_effect.get("state")}})
        for idx, o in enumerate(observations): recs.append({"record_id": f"current-effect-observation-{idx}", "record_type": "effect_observation", "producer_profile_id": "control-plane", "data": o})
    return {"reconstruction_bundle_version": "0.2.0", "bundle_id": "gax-imx-current-" + digest({"decision": d})[7:19], "run_id": record.run_id, "producer_profiles": [{"profile_id": "control-plane", "repository": "cogno-us/cognous-agent-control-plane", "revision": "283500652d47a692fb0b99a1172a6d5faffbd9a7"}, {"profile_id": "moltbot-safe", "repository": "cogno-us/moltbot-safe", "revision": "6b0ba1185bcd390f71df947dda349415e4105f5f"}], "records": recs, "input_history": {"bundle_digest": digest(original_bundle), "preserved_as_history_only": True}, "decision_source": decision_source}


def execution_facts(bundle: dict[str, Any]) -> dict[str, Any]:
    cps = records(bundle, "control_plane_attempt_transition"); dest = records(bundle, "destination_effect"); obs = records(bundle, "effect_observation"); dec = data(first_record(bundle, "runtime_decision")); states = [data(r).get("state") or data(r).get("status") for r in dest + obs]; observed = "partial" if "partial" in states else "applied" if "applied" in states else "unknown" if states else "unavailable"; return {"decision_id": dec.get("decision_id"), "effect_id": dec.get("effect_id"), "attempts": {"control_plane": [data(r).get("attempt_id") for r in cps], "executor": [data(r).get("attempt_id") for r in records(bundle, "destination_attempt")]}, "destination_observed": observed, "independent_verification": None, "pending_effects": [] if observed == "applied" else [dec.get("effect_id")], "unresolved_delivery": observed in {"partial", "unknown", "unavailable"}}


def _run_replay(bundle: dict[str, Any]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for r in bundle.get("records", []): grouped.setdefault(r["record_type"], []).append(data(r))
    from agent_replay_bundle.importers import import_bounded_workflow
    cp = {"run_id": bundle.get("run_id"), "decisions": grouped.get("runtime_decision", []), "attempts": grouped.get("control_plane_attempt_transition", []), "observations": grouped.get("effect_observation", []), "reconciliations": grouped.get("reconciliation", [])}; proposal = grouped.get("runtime_proposal", [None])[0]; moltbot = None
    if grouped.get("execution_envelope"): moltbot = {"execution_envelope": grouped["execution_envelope"][0], "execution_result": grouped.get("execution_result", [{}])[0], "attempts": grouped.get("destination_attempt", []), "attempt_events": grouped.get("destination_attempt_event", []), "effects": grouped.get("destination_effect", [])}
    rec = import_bounded_workflow(cp, proposal=proposal, moltbot_export=moltbot); return {"status": getattr(rec, "status", "unknown")}


def export_odes_reference(manifest: dict[str, Any], bundle: dict[str, Any]) -> dict[str, Any]:
    from odes import evaluate_recipient_package, export_cognous_stack_package
    package = export_cognous_stack_package(manifest, bundle, relying_party="recipient.example.org", purpose="audit", expires_at="2027-01-01T00:00:00Z"); package.setdefault("provenance", {})["execution_facts"] = execution_facts(bundle); policy = {"now": "2026-10-06T00:00:00Z", "purpose": "audit", "relying_party": "recipient.example.org", "status_inputs": {}, "supported_profiles": ["odes_cognous_stack_export_0_1"], "trusted_digests": [], "trusted_key_refs": [], "evaluation_scope": "audit", "status_max_age_seconds": 300}; return {"odes_package": package, "recipient_validation": evaluate_recipient_package(package, policy)}


def run_exchange(message: dict[str, Any], bundle: dict[str, Any], registry: LocalRegistry, destination: DestinationState, *, evaluation_time: str, manifest: dict[str, Any], store_path: str | Path, resolver: Any | None = None, mutate_resolver_after_decision=None, lose_ack: bool = False, partial_delivery: bool = False) -> dict[str, Any]:
    if resolver is None: raise ValueError("trusted resolver is required; incoming proposals never construct authority")
    store = TransactionalExchangeStore(store_path); duplicate, err = store.record_message(message)
    if err: return {"assessment": {"permitted_handling": "REFUSE", "errors": [err], "stages": {"identity_binding": "failed"}}, "execution": {"attempted": False, "reason": err}}
    assessment = assess_message(message, bundle, registry, evaluation_time=evaluation_time, seen_messages=store.seen_messages())
    if assessment["permitted_handling"] != "ACCEPT_FOR_ASSESSMENT": return {"assessment": assessment, "execution": {"attempted": False, "reason": "message_not_execution_eligible"}}
    proposal = runtime_proposal_model(bundle); op_hash = operation_commitment(model=proposal); cp = _cp(); record_store = cp["BoundedRecordStore"]((Path(store_path).parent / "control_plane_run.json"), proposal.run_id or "run-gax-imx"); flow = cp["BoundedAuthorizationWorkflow"](manifest=manifest, resolver=resolver, destination=destination.adapter, records=record_store); decision = flow.decide(proposal, now=parse_time(evaluation_time))
    if decision.result != "authorized":
        assessment["stages"]["authority"] = decision.result; return {"assessment": assessment, "execution": {"attempted": False, "reason": ";".join(decision.reasons), "decision_id": decision.decision_id}, "current_reconstruction_bundle": _reconstruction_from_run(manifest, proposal, decision, record_store.load(), destination, original_bundle=bundle)}
    if mutate_resolver_after_decision: mutate_resolver_after_decision(resolver)
    same_effect, err = store.bind_effect(decision.effect_id, op_hash)
    if err: assessment["errors"].append(err); return {"assessment": assessment, "execution": {"attempted": False, "reason": err, "effect_id": decision.effect_id}}
    try: attempt, observed = flow.execute(proposal, decision, adapter_id=proposal.adapter_id, now=parse_time(evaluation_time), lose_ack=lose_ack, partial=partial_delivery)
    except Exception as exc: return {"assessment": assessment, "execution": {"attempted": False, "reason": str(exc), "effect_id": decision.effect_id}}
    current = _reconstruction_from_run(manifest, proposal, decision, record_store.load(), destination, original_bundle=bundle); replay = _run_replay(current); odes = export_odes_reference(manifest, current); facts = execution_facts(current); successor = make_successor_packet(message, facts=facts, state_version=1, current_bundle=current); assessment["stages"]["authority"] = "authorized"
    return {"assessment": assessment, "execution": {"attempted": True, "attempt_status": attempt.status, "newly_executed": not same_effect and not attempt.acknowledgement.get("duplicate", False), "destination_observed": observed.state, "effect_id": decision.effect_id, "decision_id": decision.decision_id, "attempt_id": attempt.attempt_id}, "current_reconstruction_bundle": current, "replay_validation": replay, "odes_reference": odes, "successor_packet": successor}


def make_successor_packet(message: dict[str, Any], *, facts: dict[str, Any], state_version: int = 1, current_bundle: dict[str, Any] | None = None) -> dict[str, Any]:
    packet = {"packet_id": f"succ-{message['message_id']}-{state_version}", "profile": PROFILE, "schema_version": PROTOCOL_VERSION, "conversation_id": message.get("conversation_id"), "source_packet_id": message["message_id"], "source_commitment": digest(message), "predecessor_packet_id": None, "state_version": f"state-{state_version}", "relevant_decisions": [facts.get("decision_id")], "pending_effects": facts.get("pending_effects", []), "attempts": facts.get("attempts", {}), "unresolved_delivery": facts.get("unresolved_delivery"), "missing_evidence": [] if current_bundle else ["current_bundle_unavailable"], "next_proposed_work": "reconcile before any further execution"}; packet["packet_digest"] = digest({k: v for k, v in packet.items() if k != "packet_digest"}); return packet

def load_successor_packet(packet: dict[str, Any], destination: DestinationState, *, predecessor: dict[str, Any], store_path: str | Path) -> dict[str, Any]: return TransactionalExchangeStore(store_path).accept_successor(packet, predecessor)

def run_demo(manifest_path: str, replay_path: str, out_path: str) -> dict[str, Any]:
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8")); bundle = json.loads(Path(replay_path).read_text(encoding="utf-8")); tmp = Path(out_path).parent; dest = DestinationState(tmp / "demo_destination.sqlite"); proposal = runtime_proposal_model(bundle); resolver = _build_resolver(proposal, now=parse_time(EVAL)); result = run_exchange(make_message(bundle), bundle, LocalRegistry({"refund-sender"}, {"refund-recipient"}, "refund-recipient"), dest, evaluation_time=EVAL, manifest=manifest, store_path=tmp / "demo_exchange.sqlite", resolver=resolver); Path(out_path).write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8"); return result

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(); parser.add_argument("--manifest", required=True); parser.add_argument("--replay", required=True); parser.add_argument("--out", required=True); args = parser.parse_args(argv); run_demo(args.manifest, args.replay, args.out); return 0

if __name__ == "__main__": raise SystemExit(main())
