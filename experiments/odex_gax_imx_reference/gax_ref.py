from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

PROFILE = "urn:cognous:profiles:odex-gax-imx-refund-exchange:0.1.0"
PROTOCOL_VERSION = "0.1.0"
SUPPORTED_TYPES = {"PROPOSE", "REQUEST", "REPORT", "REFUSE", "NOT_UNDERSTOOD"}
EXECUTION_ELIGIBLE_TYPES = {"PROPOSE", "REQUEST"}

NOW = datetime(2026, 10, 5, 19, 0, tzinfo=timezone.utc)
PROFILE_AUTH_CONTEXT = "urn:cognous:alvorada:public-stack-profile:0.1.0"
INSTITUTION = "urn:cognous:institution:synthetic-customer-service"
ISSUER = "urn:cognous:principal:synthetic-governor"
ISSUER_ROLE = "urn:cognous:role:refund-governor"
POLICY_REF = "urn:cognous:policy:refund-policy"
POLICY_VERSION = "1.0"


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
    values = records(bundle, record_type)
    return values[0] if values else None


def data(record: dict[str, Any] | None) -> dict[str, Any]:
    return copy.deepcopy(record.get("data", {})) if isinstance(record, dict) else {}


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


def _cp():
    from agent_control_plane.bounded import (
        ApprovalStatus, BoundedAuthorizationWorkflow, BoundedRecordStore, ConflictStatus,
        EvidenceStatus, GrantStatus, IdentityStatus, LocalRefundDestination, MandateStatus,
        PolicyStatus, RoleMappingStatus, RuntimeProposal, SyntheticResolver, commitment,
    )
    return locals()


def cp_commitment(value: Any) -> str:
    return _cp()["commitment"](value)


def runtime_proposal_model(bundle: dict[str, Any]):
    RuntimeProposal = _cp()["RuntimeProposal"]
    raw = proposal_from_bundle(bundle)
    normalized = copy.deepcopy(raw)
    normalized.setdefault("authority_context_ref", PROFILE_AUTH_CONTEXT)
    normalized.setdefault("requirement_id", "urn:cognous:authority-requirement:refund-routine-v1")
    normalized.setdefault("evidence_refs", ["urn:cognous:evidence:refund-entitlement"])
    normalized.setdefault("effects", 1)
    normalized.setdefault("run_id", bundle.get("run_id") or "run-gax-imx")
    return RuntimeProposal.model_validate(normalized)


def proposal_commitment(proposal: dict[str, Any] | None = None, *, model: Any | None = None) -> str:
    model = model or _cp()["RuntimeProposal"].model_validate(proposal)
    return cp_commitment(model.model_dump(mode="json", exclude_none=False))


def operation_commitment(proposal: dict[str, Any] | None = None, *, model: Any | None = None) -> str:
    model = model or _cp()["RuntimeProposal"].model_validate(proposal)
    p = model.model_dump(mode="json", exclude_none=False)
    return cp_commitment({
        "actor": p.get("actor"), "principal": p.get("principal"), "manifest_id": p.get("manifest_id"),
        "manifest_version": p.get("manifest_version"), "action_id": p.get("action_id"),
        "adapter_id": p.get("adapter_id"), "target": p.get("target"), "payload": p.get("payload"),
        "payload_commitment": p.get("payload_commitment"), "requested_permissions": p.get("requested_permissions"),
        "amount": p.get("amount"), "unit": p.get("unit"), "effects": p.get("effects"),
        "authority_context_ref": p.get("authority_context_ref"), "requirement_id": p.get("requirement_id"),
    })


def validate_replay_source(manifest: dict[str, Any], bundle: dict[str, Any]) -> dict[str, Any]:
    from agent_replay_bundle.importers import import_bounded_workflow
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in bundle.get("records", []):
        grouped.setdefault(record.get("record_type"), []).append(data(record))
    proposal = grouped.get("runtime_proposal", [None])[0]
    cp = {
        "run_id": bundle.get("run_id"),
        "decisions": grouped.get("runtime_decision", []),
        "attempts": grouped.get("control_plane_attempt_transition", []),
        "observations": grouped.get("effect_observation", []),
        "reconciliations": grouped.get("reconciliation", []),
    }
    has_execution = any(grouped.get(kind) for kind in ("execution_envelope", "execution_result", "destination_attempt", "destination_effect"))
    moltbot = None
    if has_execution:
        if len(grouped.get("execution_envelope", [])) != 1 or len(grouped.get("execution_result", [])) != 1:
            raise ValueError("execution evidence requires one execution_envelope and one execution_result")
        moltbot = {
            "execution_envelope": grouped["execution_envelope"][0],
            "execution_result": grouped["execution_result"][0],
            "attempts": grouped.get("destination_attempt", []),
            "attempt_events": grouped.get("destination_attempt_event", []),
            "effects": grouped.get("destination_effect", []),
        }
    reconstructed = import_bounded_workflow(cp, proposal=proposal, moltbot_export=moltbot)
    return {"status": getattr(reconstructed, "status", "unknown")}


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
class DurableExchangeStore:
    path: Path

    def __post_init__(self) -> None:
        self.path = Path(self.path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self._write({"messages": {}, "effects": {}, "lineage": {"accepted": {}, "latest_version": 0}})

    def _load(self) -> dict[str, Any]:
        return json.loads(self.path.read_text(encoding="utf-8"))

    def _write(self, value: dict[str, Any]) -> None:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(tmp, self.path)

    def seen_messages(self) -> dict[str, str]:
        return {k: v["message_digest"] for k, v in self._load()["messages"].items()}

    def record_message(self, message: dict[str, Any]) -> tuple[bool, str | None]:
        state = self._load()
        prior = state["messages"].get(message["message_id"])
        if prior and prior["message_digest"] != message["message_digest"]:
            return False, "GAX-INTEGRITY-MESSAGE-ID-REUSE"
        duplicate = prior is not None
        state["messages"][message["message_id"]] = {"message_digest": message["message_digest"], "conversation_id": message["conversation_id"]}
        self._write(state)
        return duplicate, None

    def bind_effect(self, effect_id: str, operation_hash: str) -> tuple[bool, str | None]:
        state = self._load()
        prior = state["effects"].get(effect_id)
        if prior and prior["operation_digest"] != operation_hash:
            return False, "GAX-EFFECT-ID-REUSE-WITH-DIFFERENT-OPERATION"
        duplicate = prior is not None
        state["effects"][effect_id] = {"operation_digest": operation_hash}
        self._write(state)
        return duplicate, None

    def accept_successor(self, packet: dict[str, Any], predecessor: dict[str, Any]) -> dict[str, Any]:
        state = self._load()
        if packet.get("source_commitment") != digest(predecessor):
            return {"loaded": False, "effect_created": False, "status": "source_commitment_mismatch"}
        try:
            version = int(str(packet.get("state_version", "")).split("-")[-1])
        except Exception:
            return {"loaded": False, "effect_created": False, "status": "unsupported_state_version"}
        latest = int(state["lineage"].get("latest_version") or 0)
        if version <= latest:
            return {"loaded": False, "effect_created": False, "status": "stale_or_rollback"}
        source_id = packet.get("source_packet_id")
        existing = state["lineage"]["accepted"].get(source_id)
        if existing and existing != packet["packet_digest"]:
            return {"loaded": False, "effect_created": False, "status": "divergent_history"}
        state["lineage"]["accepted"][source_id] = packet["packet_digest"]
        state["lineage"]["latest_version"] = version
        self._write(state)
        return {"loaded": True, "effect_created": False, "status": "loaded_for_reconciliation"}


@dataclass
class DestinationState:
    path: Path | None = None
    _tmp: tempfile.TemporaryDirectory | None = field(default=None, init=False, repr=False)
    _destination: Any = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        LocalRefundDestination = _cp()["LocalRefundDestination"]
        if self.path is None:
            self._tmp = tempfile.TemporaryDirectory()
            self.path = Path(self._tmp.name) / "destination.json"
        self._destination = LocalRefundDestination(self.path)

    @property
    def effects(self) -> dict[str, Any]:
        return copy.deepcopy(self._destination.snapshot().get("effects", {}))

    def snapshot(self) -> dict[str, Any]:
        return self._destination.snapshot()


def make_message(bundle: dict[str, Any], *, message_id: str = "msg-refund-001", recipient: str = "refund-recipient", sender: str = "refund-sender", message_type: str = "PROPOSE", created_at: str = "2026-08-08T00:00:00Z", expires_at: str = "2026-08-09T00:00:00Z") -> dict[str, Any]:
    proposal = runtime_proposal_model(bundle)
    p = proposal.model_dump(mode="json", exclude_none=False)
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
        "requested_action": {"action_id": p.get("action_id"), "target": p.get("target"), "payload": p.get("payload"), "amount": p.get("amount"), "unit": p.get("unit"), "actor": p.get("actor"), "principal": p.get("principal"), "adapter_id": p.get("adapter_id")},
        "operation_commitment": operation_commitment(model=proposal),
        "proposal_commitment": proposal_commitment(model=proposal),
        "authority_refs": [{"type": "authority_context", "ref": p.get("authority_context_ref")}, {"type": "control_plane_current_authorization_required", "ref": "re-resolve-before-effect"}],
        "evidence_refs": [{"type": "replay_bundle", "ref": bundle.get("bundle_id", bundle.get("run_id", "unknown")), "digest": digest(bundle)}, *[{"type": "proposal_evidence", "ref": ref} for ref in (p.get("evidence_refs") or [])]],
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

    def fail(stage: str, code: str, handling: str = "REFUSE") -> dict[str, Any]:
        stages[stage] = "failed"; errors.append(code)
        return {"permitted_handling": handling, "stages": stages, "errors": errors, "message_received": True, "message_understood": stage not in {"structure", "profile"}}

    required = ["message_id", "conversation_id", "profile", "protocol_version", "message_type", "created_at", "expires_at", "sender", "recipient", "purpose", "requested_action", "operation_commitment", "proposal_commitment", "authority_refs", "evidence_refs", "content", "content_digest", "message_digest"]
    if any(key not in message for key in required):
        return fail("structure", "GAX-SCHEMA-MISSING-REQUIRED", "NOT_UNDERSTOOD")
    if message["message_digest"] != digest({k: v for k, v in message.items() if k != "message_digest"}):
        return fail("structure", "GAX-INTEGRITY-MESSAGE-DIGEST", "NOT_UNDERSTOOD")
    if message["content_digest"] != digest(message["content"]):
        return fail("structure", "GAX-INTEGRITY-CONTENT-DIGEST", "NOT_UNDERSTOOD")
    stages["structure"] = "passed"
    if message["profile"] != PROFILE or message["protocol_version"] != PROTOCOL_VERSION or message["message_type"] not in SUPPORTED_TYPES:
        return fail("profile", "GAX-PROFILE-UNSUPPORTED-OR-DOWNGRADE", "NOT_UNDERSTOOD")
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
    proposal = runtime_proposal_model(bundle)
    p = proposal.model_dump(mode="json", exclude_none=False)
    if message["proposal_commitment"] != proposal_commitment(model=proposal):
        return fail("reference_resolution", "GAX-REFERENCE-PROPOSAL-COMMITMENT-MISMATCH")
    if message["operation_commitment"] != operation_commitment(model=proposal):
        return fail("reference_resolution", "GAX-REFERENCE-OPERATION-COMMITMENT-MISMATCH")
    for key in ("action_id", "target", "payload", "amount", "unit", "actor", "principal", "adapter_id"):
        if message["requested_action"].get(key) != p.get(key):
            return fail("reference_resolution", f"GAX-REFERENCE-REQUESTED-{key.upper()}-MISMATCH")
    authority_refs = {(x.get("type"), x.get("ref")) for x in message.get("authority_refs", []) if isinstance(x, dict)}
    if ("authority_context", p.get("authority_context_ref")) not in authority_refs:
        return fail("reference_resolution", "GAX-REFERENCE-AUTHORITY-CONTEXT-MISMATCH")
    evidence_refs = {(x.get("type"), x.get("ref")) for x in message.get("evidence_refs", []) if isinstance(x, dict)}
    for ref in p.get("evidence_refs") or []:
        if ("proposal_evidence", ref) not in evidence_refs:
            return fail("reference_resolution", "GAX-REFERENCE-EVIDENCE-MISMATCH")
    stages["reference_resolution"] = "passed"
    if message["purpose"] != registry.relying_purpose:
        return fail("purpose", "GAX-PURPOSE-NOT-PERMITTED")
    stages["purpose"] = "passed"
    if message["message_type"] not in EXECUTION_ELIGIBLE_TYPES:
        stages["communicative_act"] = "informational_no_effect"
        return {"permitted_handling": "INFORMATIONAL_ONLY", "stages": stages, "errors": errors, "message_received": True, "message_understood": True}
    stages["communicative_act"] = "assessment_required"
    return {"permitted_handling": "ACCEPT_FOR_ASSESSMENT", "stages": stages, "errors": errors, "message_received": True, "message_understood": True}


def _build_resolver(proposal: Any, *, status: str = "active"):
    c = _cp(); commitment = c["commitment"]
    GrantStatus = c["GrantStatus"]; ApprovalStatus = c["ApprovalStatus"]; IdentityStatus = c["IdentityStatus"]; MandateStatus = c["MandateStatus"]; PolicyStatus = c["PolicyStatus"]; ConflictStatus = c["ConflictStatus"]; EvidenceStatus = c["EvidenceStatus"]; RoleMappingStatus = c["RoleMappingStatus"]; SyntheticResolver = c["SyntheticResolver"]
    role = "urn:cognous:role:customer-service-supervisor"; action_name = "refund_issue_routine"; grant_id = "urn:cognous:grant:routine-1"
    context = {
        "schema_version": "0.1.0", "interface_status": "proposed_pending_governor_review", "context_id": "urn:cognous:authority-context:pilot-1",
        "institution": {"institution_id": INSTITUTION, "authority_domain": "customer-refunds", "authority_basis_ref": "urn:cognous:authority-basis:synthetic", "authority_mode": "principle_inspired"},
        "principal": proposal.principal, "acting_identity": proposal.actor,
        "accountable_human_role": {"role_id": "urn:cognous:role:accountable-human", "human_principal": "urn:cognous:principal:human-1", "mandate_ref": "urn:cognous:mandate:human-1"},
        "requirement": {"requirement_id": proposal.requirement_id, "governing_sources": [{"ref": POLICY_REF, "version": POLICY_VERSION, "provision": "refund", "standing": "local_policy"}], "permissions": [{"action": action_name, "targets": [proposal.target], "data_scopes": list(proposal.requested_permissions), "max_amount": max(100.0, float(proposal.amount)), "unit": proposal.unit, "max_effects": 1}], "approvals": [{"role_id": role, "independent_of_actor": True}], "evidence": [{"obligation_id": "urn:cognous:evidence:refund-entitlement", "kind": "authorization", "source_ref": "urn:cognous:source:entitlement", "max_age_seconds": 300, "required": True, "unknown_behavior": "hold_effect"}], "consequence": {"tier": "T1", "rationale": "Synthetic test tier.", "profile_ref": "urn:cognous:consequence:T1"}},
        "conflicts": {"precedence_refs": [POLICY_REF], "incompatible_role_pairs": [], "escalation_ref": "urn:cognous:procedure:escalation", "appeal_ref": "urn:cognous:procedure:appeal", "continuity_ref": "urn:cognous:procedure:continuity", "remedy_ref": "urn:cognous:procedure:remedy"},
        "supporting_evidence_refs": [], "change": {"kind": "none", "proposal_ref": None, "adoption_record_ref": None},
        "grant": {"grant_id": grant_id, "revision": "1", "requirement_id": proposal.requirement_id, "issuer": ISSUER, "issuer_role": ISSUER_ROLE, "issuance_record_ref": "urn:cognous:issuance:1", "grantee": proposal.principal, "acting_identity": proposal.actor, "issued_at": (NOW - timedelta(hours=1)).isoformat(), "not_before": (NOW - timedelta(minutes=30)).isoformat(), "expires_at": (NOW + timedelta(hours=1)).isoformat(), "status_ref": "urn:cognous:status:grant-1", "policy_versions": [{"ref": POLICY_REF, "version": POLICY_VERSION}], "permissions": [{"action": action_name, "targets": [proposal.target], "data_scopes": list(proposal.requested_permissions), "max_amount": max(100.0, float(proposal.amount)), "unit": proposal.unit, "max_effects": 1}], "delegation": {"parent_grant_id": None, "may_delegate": False, "remaining_depth": 0}, "approval_refs": ["urn:cognous:approval:t1-1"]},
    }
    aliases = {"customer-service-supervisor": [role]}
    mapping = RoleMappingStatus(institution_id=INSTITUTION, version="roles-v1", digest=commitment({"institution_id": INSTITUTION, "version": "roles-v1", "aliases": aliases}), aliases=aliases)
    return SyntheticResolver(
        contexts={PROFILE_AUTH_CONTEXT: context},
        statuses={grant_id: GrantStatus(grant_id=grant_id, revision="1", status=status, observed_at=NOW.isoformat(), version="status-v1", status_ref=context["grant"]["status_ref"], authority_basis_ref="urn:cognous:authority-basis:synthetic", institution_id=INSTITUTION, authority_domain="customer-refunds")},
        identities={proposal.actor: IdentityStatus(identity=proposal.actor, principal=proposal.principal, authenticated=True, delegation_valid=True, observed_at=NOW.isoformat(), institution_id=INSTITUTION, authority_domain="customer-refunds", chain=[])},
        mandates={f"{ISSUER}|{ISSUER_ROLE}": MandateStatus(issuer=ISSUER, issuer_role=ISSUER_ROLE, issuance_record_ref=context["grant"]["issuance_record_ref"], mandate_valid=True, observed_at=NOW.isoformat(), institution_id=INSTITUTION, authority_domain="customer-refunds")},
        approvals={"urn:cognous:approval:t1-1": ApprovalStatus(approval_ref="urn:cognous:approval:t1-1", role_id=role, approver="urn:cognous:principal:reviewer-1", grant_id=grant_id, grant_revision="1", proposal_commitment=proposal_commitment(model=proposal), policy_versions=copy.deepcopy(context["grant"]["policy_versions"]), observed_at=NOW.isoformat(), institution_id=INSTITUTION, authority_domain="customer-refunds")},
        policies={POLICY_REF: PolicyStatus(ref=POLICY_REF, version=POLICY_VERSION, status="active", observed_at=NOW.isoformat(), institution_id=INSTITUTION, authority_domain="customer-refunds")},
        conflicts={proposal.requirement_id: ConflictStatus(requirement_id=proposal.requirement_id, state="clear", observed_at=NOW.isoformat(), conflict_refs=[POLICY_REF], institution_id=INSTITUTION, authority_domain="customer-refunds")},
        role_mappings={INSTITUTION: mapping},
        evidence={"urn:cognous:evidence:refund-entitlement": EvidenceStatus(obligation_id="urn:cognous:evidence:refund-entitlement", state="current", observed_at=NOW.isoformat(), source_ref="urn:cognous:source:entitlement", institution_id=INSTITUTION, authority_domain="customer-refunds")},
    )


def _workflow(manifest: dict[str, Any], resolver: Any, destination: DestinationState, store_dir: Path):
    c = _cp()
    return c["BoundedAuthorizationWorkflow"](manifest=manifest, resolver=resolver, destination=destination._destination, records=c["BoundedRecordStore"](store_dir / "bounded-run.json", "run-gax-imx"))


def execution_facts(bundle: dict[str, Any]) -> dict[str, Any]:
    decision = decision_from_bundle(bundle); effect_id = decision.get("effect_id") or (decision.get("binding") or {}).get("effect_id")
    destination_effects = [data(r) for r in records(bundle, "destination_effect")]
    observations = [data(r) for r in records(bundle, "effect_observation")]
    reconciliations = [data(r) for r in records(bundle, "reconciliation")]
    attempts = [data(r) for r in records(bundle, "destination_attempt")]
    cp_attempts = [data(r) for r in records(bundle, "control_plane_attempt_transition")]
    states = [x.get("state") or x.get("status") or x.get("result") for x in destination_effects + observations + reconciliations if x.get("state") or x.get("status") or x.get("result")]
    observed = "partial" if "partial" in states else ("applied" if "applied" in states or "success" in states else (str(states[0]) if states else "unavailable"))
    ack = "received" if any(isinstance(a.get("acknowledgement"), dict) and a.get("acknowledgement") for a in cp_attempts) else "unknown"
    if any(str(a.get("status", "")).lower() in {"unknown", "acknowledgement_lost", "lost"} or str(a.get("acknowledgement", "")).lower() in {"lost", "unknown"} for a in cp_attempts):
        ack = "unknown"
    return {"effect_id": effect_id, "attempts": {"control_plane": cp_attempts, "executor": attempts}, "acknowledgement": ack, "destination_observed": observed, "destination_effects": destination_effects, "observations": observations, "reconciliations": reconciliations, "independent_verification": None}


def run_exchange(message: dict[str, Any], bundle: dict[str, Any], registry: LocalRegistry, destination: DestinationState, *, evaluation_time: str, manifest: dict[str, Any], store_path: str | Path | None = None, resolver: Any | None = None, mutate_resolver_after_decision: Callable[[Any], None] | None = None, lose_ack: bool = False, partial_delivery: bool = False) -> dict[str, Any]:
    store_dir = Path(store_path).parent if store_path else Path(tempfile.mkdtemp())
    exchange_store = DurableExchangeStore(Path(store_path) if store_path else store_dir / "exchange-store.json")
    assessment = assess_message(message, bundle, registry, evaluation_time=evaluation_time, seen_messages=exchange_store.seen_messages())
    if assessment["errors"]:
        return {"assessment": assessment, "execution": {"attempted": False}, "destination_state": destination.snapshot()}
    duplicate_message, err = exchange_store.record_message(message)
    if err:
        assessment["errors"].append(err); assessment["permitted_handling"] = "REFUSE"
        return {"assessment": assessment, "execution": {"attempted": False, "reason": err}, "destination_state": destination.snapshot()}
    if assessment["permitted_handling"] != "ACCEPT_FOR_ASSESSMENT":
        return {"assessment": assessment, "execution": {"attempted": False, "reason": assessment["permitted_handling"]}, "destination_state": destination.snapshot(), "facts": execution_facts(bundle)}
    replay_report = validate_replay_source(manifest, bundle)
    proposal = runtime_proposal_model(bundle)
    resolver = resolver or _build_resolver(proposal)
    flow = _workflow(manifest, resolver, destination, store_dir)
    decision = flow.decide(proposal, now=parse_time(evaluation_time))
    assessment["stages"]["authority"] = decision.result
    if decision.result != "authorized":
        return {"assessment": assessment, "execution": {"attempted": False, "reason": decision.result, "reasons": decision.reasons}, "destination_state": destination.snapshot(), "replay_validation": replay_report, "facts": execution_facts(bundle)}
    if mutate_resolver_after_decision:
        mutate_resolver_after_decision(resolver)
    duplicate_effect, effect_err = exchange_store.bind_effect(decision.effect_id, operation_commitment(model=proposal))
    if effect_err:
        assessment["errors"].append(effect_err); assessment["permitted_handling"] = "REFUSE"
        return {"assessment": assessment, "execution": {"attempted": False, "reason": effect_err}, "destination_state": destination.snapshot(), "replay_validation": replay_report}
    try:
        attempt, observation = flow.execute(proposal, decision, adapter_id=proposal.adapter_id, now=parse_time(evaluation_time), lose_ack=lose_ack, partial=partial_delivery)
    except PermissionError as exc:
        assessment["permitted_handling"] = "REFUSE_AUTHORITY_INVALID"; assessment["errors"].append(str(exc))
        return {"assessment": assessment, "execution": {"attempted": False, "reason": str(exc)}, "destination_state": destination.snapshot(), "replay_validation": replay_report}
    return {"assessment": assessment, "execution": {"attempted": True, "attempt_status": attempt.status, "effect_id": attempt.effect_id, "newly_executed": not bool(getattr(attempt, "acknowledgement", {}).get("reconciled_existing")), "destination_observed": observation.state, "duplicate_message": duplicate_message}, "destination_state": destination.snapshot(), "replay_validation": replay_report, "facts": execution_facts(bundle)}


def make_successor_packet(previous: dict[str, Any], *, facts: dict[str, Any] | None = None, next_work: str = "reconcile_before_retry", state_version: int = 1) -> dict[str, Any]:
    facts = facts or {}
    packet = {"packet_id": "successor-refund-001", "profile": "urn:cognous:profiles:odex-imx-continuity:0.1.0", "schema_version": "0.1.0", "source_packet_id": previous.get("message_id") or previous.get("packet_id"), "source_commitment": digest(previous), "state_version": f"state-{state_version}", "predecessor": previous.get("message_id") or previous.get("packet_id"), "decisions": previous.get("authority_refs", []), "evidence_refs": previous.get("evidence_refs", []), "pending_effects": [facts.get("effect_id")] if facts.get("destination_observed") in {"partial", "unknown", "unavailable"} and facts.get("effect_id") else [], "attempts": facts.get("attempts", {}), "unresolved_delivery": [facts.get("destination_observed")] if facts.get("destination_observed") in {"partial", "unknown", "unavailable"} else [], "conflicts": [], "missing_evidence": ["independent_verification"] if facts.get("independent_verification") is None else [], "supersessions": [], "corrections": [], "next_proposed_work": next_work, "loading_semantics": "load_only_no_authority_no_execution", "loss_report": []}
    packet["packet_digest"] = digest({k: v for k, v in packet.items() if k != "packet_digest"})
    return packet


def load_successor_packet(packet: dict[str, Any], destination: DestinationState, *, predecessor: dict[str, Any], store_path: str | Path | None = None) -> dict[str, Any]:
    if packet.get("profile") != "urn:cognous:profiles:odex-imx-continuity:0.1.0" or packet.get("schema_version") != "0.1.0":
        return {"loaded": False, "effect_created": False, "status": "unsupported_profile_or_schema"}
    if packet.get("packet_digest") != digest({k: v for k, v in packet.items() if k != "packet_digest"}):
        return {"loaded": False, "effect_created": False, "status": "integrity_failed"}
    store = DurableExchangeStore(Path(store_path) if store_path else Path(tempfile.mkdtemp()) / "exchange-store.json")
    result = store.accept_successor(packet, predecessor)
    result["destination_state"] = destination.snapshot()
    return result


def export_odes_reference(manifest: dict[str, Any], bundle: dict[str, Any]) -> dict[str, Any]:
    from odes.exporter import export_cognous_stack_package
    from odes.recipient_validator import evaluate_recipient_package
    package = export_cognous_stack_package(manifest, bundle, relying_party="recipient.example.org", purpose="refund_execution_assessment", expires_at="2027-01-01T00:00:00Z")
    validation = evaluate_recipient_package(package, {"supported_profiles": [package["profile"]["implementation_profile"]], "trusted_digests": [package["package_digest"]], "allow_unauthenticated_informational_inspection": True, "relying_party": "recipient.example.org", "purpose": "refund_execution_assessment", "now": "2026-08-08T01:00:00Z", "status_inputs": {"record_id": package["record"]["decision_id"], "authority_basis": package["record"]["authority"]["authority_basis"], "evaluation_scope": "refund_execution_assessment", "evaluated_at": "2026-08-08T01:00:00Z", "evidence_freshness": "current", "authority_valid_at_decision_verified": True, "freshness": "current", "revoked": False, "superseded": False}})
    return {"odes_package": package, "recipient_validation": validation}


def run_demo(manifest_path: str, replay_path: str, out_path: str) -> dict[str, Any]:
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8")); bundle = json.loads(Path(replay_path).read_text(encoding="utf-8"))
    registry = LocalRegistry({"refund-sender"}, {"refund-recipient"}, "refund-recipient")
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp); destination = DestinationState(tmp_path / "destination.json"); message = make_message(bundle)
        result = run_exchange(message, bundle, registry, destination, evaluation_time="2026-08-08T01:00:00Z", manifest=manifest, store_path=tmp_path / "exchange-store.json")
        successor = make_successor_packet(message, facts=result.get("facts")); loaded = load_successor_packet(successor, destination, predecessor=message, store_path=tmp_path / "exchange-store.json")
    output = {"message": message, "exchange_result": result, "odes_reference": export_odes_reference(manifest, bundle), "successor_packet": successor, "successor_load": loaded}
    Path(out_path).write_text(json.dumps(output, indent=2, sort_keys=True), encoding="utf-8")
    return output


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--manifest", required=True); parser.add_argument("--replay", required=True); parser.add_argument("--out", required=True)
    args = parser.parse_args(argv); run_demo(args.manifest, args.replay, args.out)


if __name__ == "__main__":
    main()
