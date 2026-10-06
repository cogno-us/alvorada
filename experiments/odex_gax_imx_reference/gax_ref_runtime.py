from __future__ import annotations

import argparse
import dataclasses
import importlib
import importlib.util
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

from . import gax_ref as base

PROFILE = base.PROFILE
PROTOCOL_VERSION = base.PROTOCOL_VERSION
EVAL = base.EVAL

LocalRegistry = base.LocalRegistry
assess_message = base.assess_message
digest = base.digest
make_message = base.make_message
parse_time = base.parse_time
runtime_proposal_model = base.runtime_proposal_model
_build_resolver = base._build_resolver

CP_REVISION = "283500652d47a692fb0b99a1172a6d5faffbd9a7"
MOLTBOT_REVISION = "6b0ba1185bcd390f71df947dda349415e4105f5f"
MANIFEST_REVISION = "46c950bed37fe3812000895430bc0312d29e37ce"
REPLAY_REVISION = "f12648313cedc2cf06145d397fa56cdea18cc800"
ODES_REVISION = "b3a2f1e72df88cd24d93d1b7d69963f43139e749"
ALVORADA_REVISION = "fb3d97938969a89e149e8ff8db2756091d1233fc"


def _repo_path(env_name: str, default: str) -> Path:
    return Path(os.environ.get(env_name, default)).resolve()


def load_actual_pinned_moltbot_helpers():
    cp_root = _repo_path("MOLTBOT_SAFE_CONTROL_PLANE_ROOT", "upstream/control-plane")
    molt_root = _repo_path("MOLTBOT_SAFE_ROOT", "upstream/moltbot-safe")
    manifest_path = _repo_path(
        "MOLTBOT_SAFE_MANIFEST_FIXTURE",
        os.environ.get(
            "UPSTREAM_MANIFEST_EXAMPLE",
            "upstream/manifest/examples/refund_integration_v1_1.manifest.json",
        ),
    )
    missing = [str(p) for p in (cp_root, molt_root, manifest_path) if not p.exists()]
    if missing:
        raise RuntimeError("pinned upstream checkout is unavailable: " + ", ".join(missing))
    for path in (str(cp_root / "src"), str(molt_root)):
        if path not in sys.path:
            sys.path.insert(0, path)
    os.environ["MOLTBOT_SAFE_CONTROL_PLANE_ROOT"] = str(cp_root)
    os.environ["MOLTBOT_SAFE_MANIFEST_FIXTURE"] = str(manifest_path)
    helper_path = molt_root / "tests" / "test_safe_executor.py"
    spec = importlib.util.spec_from_file_location("alvorada_actual_pinned_moltbot_helpers", helper_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("unable to load pinned Moltbot helper module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def actual_executor_classes() -> dict[str, Any]:
    h = load_actual_pinned_moltbot_helpers()
    adapter_mod = importlib.import_module("engine.control_plane_adapter")
    return {
        "PinnedControlPlaneExecutor": h.PinnedControlPlaneExecutor,
        "ControlPlaneRefundDestinationAdapter": adapter_mod.ControlPlaneRefundDestinationAdapter,
        "DurableRefundDestination": h.DurableRefundDestination,
        "ExecutionEnvelope": h.ExecutionEnvelope,
        "ExecutionOperation": h.ExecutionOperation,
        "LocalExecutionPolicy": h.LocalExecutionPolicy,
    }


def _sqlite_rows(path: Path, table: str) -> list[dict[str, Any]]:
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY rowid")]


def _effect_rows(destination: Any) -> list[dict[str, Any]]:
    return _sqlite_rows(Path(destination.path), "effects")


def _export_sources(workflow: Any, proposal: Any, request: Any, result: Any, destination: Any) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    cp_record = workflow.records.load().model_dump(mode="json")
    all_effects = _sqlite_rows(Path(destination.path), "effects")
    all_attempts = _sqlite_rows(Path(destination.path), "attempts")
    all_events = _sqlite_rows(Path(destination.path), "attempt_events")
    current_effect = request.effect_id
    current_decision = request.decision_id
    current_attempt_ids: set[str] = set()
    if getattr(result, "attempt_id", None):
        current_attempt_ids.add(str(result.attempt_id))
    selected_attempts: list[dict[str, Any]] = []
    for row in all_attempts:
        if row.get("effect_id") != current_effect:
            continue
        if row.get("decision_id") == current_decision or row.get("attempt_id") in current_attempt_ids:
            selected_attempts.append(row)
            if row.get("attempt_id"):
                current_attempt_ids.add(str(row["attempt_id"]))
    selected_events = [row for row in all_events if row.get("attempt_id") in current_attempt_ids]
    moltbot = {
        "execution_envelope": dataclasses.asdict(request),
        "execution_result": dataclasses.asdict(result),
        "effects": [row for row in all_effects if row.get("effect_id") == current_effect],
        "attempts": selected_attempts,
        "attempt_events": selected_events,
    }
    return cp_record, proposal.model_dump(mode="json", exclude_none=False), moltbot


def import_replay_bundle(cp_record: dict[str, Any], proposal: dict[str, Any] | None, moltbot: dict[str, Any] | None = None) -> Any:
    from agent_replay_bundle.importers import import_bounded_workflow

    return import_bounded_workflow(cp_record, proposal=proposal, moltbot_export=moltbot)


def reconstruction_dict(reconstructed: Any) -> dict[str, Any]:
    return reconstructed.model_dump(mode="json") if hasattr(reconstructed, "model_dump") else dict(reconstructed)


def export_odes_reference(manifest: dict[str, Any], reconstruction_bundle: dict[str, Any]) -> dict[str, Any]:
    from odes import evaluate_recipient_package, export_cognous_stack_package

    package = export_cognous_stack_package(
        manifest,
        reconstruction_bundle,
        relying_party="recipient.example.org",
        purpose="audit",
        expires_at="2027-01-01T00:00:00Z",
    )
    policy = {
        "now": "2026-10-06T00:00:00Z",
        "purpose": "audit",
        "relying_party": "recipient.example.org",
        "status_inputs": {},
        "supported_profiles": [package["profile"]["implementation_profile"]],
        "trusted_digests": [],
        "trusted_key_refs": [],
        "evaluation_scope": "audit",
        "status_max_age_seconds": 300,
        "allow_unauthenticated_informational_inspection": False,
    }
    validation = evaluate_recipient_package(package, policy)
    return {
        "odes_package": package,
        "recipient_validation": validation,
        "recipient_policy": policy,
        "exchange_metadata": {"note": "additional exchange metadata is outside the digest-covered ODES package"},
    }


def execution_facts(bundle: dict[str, Any]) -> dict[str, Any]:
    package = export_odes_reference(_manifest(), bundle)["odes_package"]
    return dict(package.get("provenance", {}).get("execution_facts", {}))


def _manifest() -> dict[str, Any]:
    path = _repo_path(
        "UPSTREAM_MANIFEST_EXAMPLE",
        "upstream/manifest/examples/refund_integration_v1_1.manifest.json",
    )
    return json.loads(path.read_text(encoding="utf-8"))


def _artifact_facts(odes_reference: dict[str, Any]) -> dict[str, Any]:
    facts = dict(odes_reference["odes_package"].get("provenance", {}).get("execution_facts", {}))
    effects = facts.get("destination_effects") or []
    effect_ids = [item.get("effect_id") for item in effects if isinstance(item, dict) and item.get("effect_id")]
    destination = facts.get("destination_observed")
    ack = facts.get("acknowledgement_summary")
    facts["pending_effects"] = effect_ids if destination == "partial" else []
    facts["unresolved_delivery"] = bool(destination == "partial" or ack == "unknown")
    return facts


def _pipeline(
    manifest: dict[str, Any],
    cp_record: dict[str, Any],
    proposal: dict[str, Any] | None,
    moltbot: dict[str, Any] | None,
    *,
    predecessor: dict[str, Any] | None = None,
    predecessor_packet_id: str | None = None,
    state_version: int = 1,
) -> dict[str, Any]:
    reconstructed = import_replay_bundle(cp_record, proposal, moltbot)
    bundle = reconstruction_dict(reconstructed)
    odes = export_odes_reference(manifest, bundle)
    facts = _artifact_facts(odes)
    successor = None
    if predecessor is not None:
        successor = make_successor_packet(
            predecessor,
            facts=facts,
            state_version=state_version,
            current_bundle=bundle,
            predecessor_packet_id=predecessor_packet_id,
        )
    return {"reconstruction": reconstructed, "reconstruction_bundle": bundle, "odes_reference": odes, "successor_packet": successor, "execution_facts": facts}


def _build_request_from_decision(h: Any, proposal: Any, resolver: Any, decision: Any) -> Any:
    context = resolver.authority_context(proposal.authority_context_ref or "")
    institution = context.get("institution") if isinstance(context, dict) else {}
    binding = decision.binding
    if binding is None:
        raise ValueError("authorized execution requires a decision authorization binding")
    operation = h.ExecutionOperation(
        actor=proposal.actor,
        principal=proposal.principal,
        institution_id=institution["institution_id"],
        authority_domain=institution["authority_domain"],
        manifest_id=proposal.manifest_id,
        manifest_version=proposal.manifest_version,
        manifest_digest=proposal.manifest_digest,
        proposal_commitment=h.commitment(proposal.model_dump(mode="json", exclude_none=False)),
        action_id=proposal.action_id,
        adapter_id=proposal.adapter_id,
        target=proposal.target,
        payload=json.loads(json.dumps(proposal.payload)),
        payload_commitment=proposal.payload_commitment,
        requested_permissions=tuple(proposal.requested_permissions),
        amount=proposal.amount,
        unit=proposal.unit,
        effects=proposal.effects,
        authority_context_id=proposal.authority_context_ref,
        requirement_id=proposal.requirement_id,
        grant_id=binding.grant_id,
        grant_revision=binding.grant_revision,
        effective_max_effects=binding.effective_max_effects,
    )
    return h.ExecutionEnvelope(h.EXECUTION_ENVELOPE_VERSION, decision.decision_id, decision.effect_id, operation)


def _run_current_request(
    *,
    message: dict[str, Any],
    bundle: dict[str, Any],
    manifest: dict[str, Any],
    destination: Any,
    store_path: Path,
    resolver: Any,
    evaluation_time: str,
    mutate_resolver_after_decision=None,
    lose_ack: bool = False,
    partial_delivery: bool = False,
) -> dict[str, Any]:
    h = load_actual_pinned_moltbot_helpers()
    helper = h._load_pinned_helpers()
    now = parse_time(evaluation_time)
    proposal = runtime_proposal_model(bundle)
    record_suffix = digest({"message_id": message["message_id"], "message_digest": message.get("message_digest"), "evaluation_time": evaluation_time})[-16:]
    record_store = helper.BoundedRecordStore(store_path.parent / f"control-plane-{message['message_id']}-{record_suffix}.json", proposal.run_id or "run-gax-imx")
    cp_destination = helper.LocalRefundDestination(store_path.parent / f"cp-placeholder-{message['message_id']}-{record_suffix}.json")
    workflow = helper.BoundedAuthorizationWorkflow(manifest=manifest, resolver=resolver, destination=cp_destination, records=record_store)
    decision = workflow.decide(proposal, now=now)

    if decision.result != "authorized":
        pipe = _pipeline(manifest, record_store.load().model_dump(mode="json"), proposal.model_dump(mode="json", exclude_none=False), None, predecessor=message)
        return {"status": decision.result, "decision": decision, "result": None, "destination_effects": {}, **pipe}

    if mutate_resolver_after_decision:
        mutate_resolver_after_decision(resolver)

    request = _build_request_from_decision(h, proposal, resolver, decision)
    executor = h.PinnedControlPlaneExecutor(workflow=workflow, destination=destination, policy=h.policy(request.operation))
    simulate = "partial" if partial_delivery else ("lost_ack" if lose_ack else None)
    result = executor.execute(envelope=request, proposal=proposal, decision=decision, now=now, simulate=simulate)
    cp_record, p, moltbot = _export_sources(workflow, proposal, request, result, destination)
    pipe = _pipeline(manifest, cp_record, p, moltbot, predecessor=message)
    return {"status": result.status, "result": result, "decision": decision, "request": request, "destination": destination, "destination_effects": {row["effect_id"]: row for row in _effect_rows(destination)}, **pipe}


def run_actual_outcome(tmp_path: Path, outcome: str) -> dict[str, Any]:
    h = load_actual_pinned_moltbot_helpers()
    helper = h._load_pinned_helpers()
    manifest = helper.manifest()
    bundle = json.loads(Path(os.environ["UPSTREAM_REPLAY_SUCCESS_EXAMPLE"]).read_text(encoding="utf-8")) if os.environ.get("UPSTREAM_REPLAY_SUCCESS_EXAMPLE") else {}
    message = make_message(bundle) if bundle else {"message_id": "fixture-message", "conversation_id": "fixture-conversation"}
    if outcome == "hold":
        proposal = helper.proposal()
        resolver = helper.resolver_for(proposal)
        grant = resolver.contexts[helper.PROFILE]["grant"]
        resolver.statuses[grant["grant_id"]].status = "revoked"
        destination = helper.LocalRefundDestination(tmp_path / "cp-destination.json")
        records = helper.BoundedRecordStore(tmp_path / "cp-run.json", "run-1")
        workflow = helper.BoundedAuthorizationWorkflow(manifest=manifest, resolver=resolver, destination=destination, records=records)
        decision = workflow.decide(proposal, now=helper.NOW)
        pipe = _pipeline(manifest, workflow.records.load().model_dump(mode="json"), proposal.model_dump(mode="json", exclude_none=False), None, predecessor=message)
        return {"status": decision.result, "decision": decision, "destination_effects": {}, **pipe}

    helper, proposal, resolver, workflow, decision, destination, executor, request = h._integrated(tmp_path)
    if outcome == "success":
        result = executor.execute(envelope=request, proposal=proposal, decision=decision, now=helper.NOW)
    elif outcome == "denied_after_decision":
        grant = resolver.contexts[helper.PROFILE]["grant"]
        resolver.statuses[grant["grant_id"]].status = "revoked"
        result = executor.execute(envelope=request, proposal=proposal, decision=decision, now=helper.NOW)
    elif outcome == "lost_ack":
        result = executor.execute(envelope=request, proposal=proposal, decision=decision, now=helper.NOW, simulate="lost_ack")
    elif outcome == "restart_reconciliation":
        executor.execute(envelope=request, proposal=proposal, decision=decision, now=helper.NOW, simulate="lost_ack")
        destination = h.DurableRefundDestination(destination.root)
        executor = h.PinnedControlPlaneExecutor(workflow=workflow, destination=destination, policy=h.policy(request.operation))
        result = executor.execute(envelope=request, proposal=proposal, decision=decision, now=helper.NOW)
    elif outcome == "duplicate_delivery":
        executor.execute(envelope=request, proposal=proposal, decision=decision, now=helper.NOW)
        result = executor.execute(envelope=request, proposal=proposal, decision=decision, now=helper.NOW)
    elif outcome == "partial":
        result = executor.execute(envelope=request, proposal=proposal, decision=decision, now=helper.NOW, simulate="partial")
    else:
        raise ValueError(f"unsupported outcome: {outcome}")

    cp, p, m = _export_sources(workflow, proposal, request, result, destination)
    pipe = _pipeline(manifest, cp, p, m, predecessor=message)
    return {"status": result.status, "result": result, "decision": decision, "request": request, "destination": destination, "destination_effects": {row["effect_id"]: row for row in _effect_rows(destination)}, **pipe}


def _duplicate_redelivery_result(message: dict[str, Any], assessment: dict[str, Any], destination: Any) -> dict[str, Any]:
    effects = _effect_rows(destination) if destination is not None else []
    observed = effects[0]["state"] if effects else "unavailable"
    effect_id = effects[0]["effect_id"] if effects else None
    return {
        "assessment": assessment,
        "execution": {
            "attempted": False,
            "attempt_status": "reconciled" if effects else "duplicate_message",
            "newly_executed": False,
            "destination_observed": observed,
            "effect_id": effect_id,
            "decision_id": None,
            "attempt_id": None,
        },
        "successor_packet": make_successor_packet(message, facts={"decision_id": None, "pending_effects": [], "attempts": {}, "unresolved_delivery": False}, current_bundle={}) if effects else None,
    }


def run_exchange(message: dict[str, Any], bundle: dict[str, Any], registry: LocalRegistry, destination: Any, *, evaluation_time: str, manifest: dict[str, Any], store_path: str | Path, resolver: Any | None = None, mutate_resolver_after_decision=None, lose_ack: bool = False, partial_delivery: bool = False) -> dict[str, Any]:
    store_path = Path(store_path)
    store = TransactionalExchangeStore(store_path)
    duplicate, err = store.record_message(message)
    if err:
        return {"assessment": {"permitted_handling": "REFUSE", "errors": [err], "stages": {"identity_binding": "failed"}}, "execution": {"attempted": False, "reason": err}, "successor_packet": None}
    assessment = assess_message(message, bundle, registry, evaluation_time=evaluation_time, seen_messages=store.seen_messages())
    if assessment["permitted_handling"] != "ACCEPT_FOR_ASSESSMENT":
        return {"assessment": assessment, "execution": {"attempted": False, "reason": "message_not_execution_eligible"}, "successor_packet": None}
    if duplicate:
        return _duplicate_redelivery_result(message, assessment, destination)
    if resolver is None:
        assessment["errors"].append("GAX-AUTHORITY-TRUSTED-RESOLVER-REQUIRED")
        return {"assessment": assessment, "execution": {"attempted": False, "reason": "trusted_resolver_required"}, "successor_packet": None}
    if destination is None:
        assessment["errors"].append("GAX-EXECUTION-DESTINATION-REQUIRED")
        return {"assessment": assessment, "execution": {"attempted": False, "reason": "destination_required"}, "successor_packet": None}
    artifacts = _run_current_request(message=message, bundle=bundle, manifest=manifest, destination=destination, store_path=store_path, resolver=resolver, evaluation_time=evaluation_time, mutate_resolver_after_decision=mutate_resolver_after_decision, lose_ack=lose_ack, partial_delivery=partial_delivery)
    result = artifacts.get("result")
    status = artifacts["status"]
    assessment["stages"]["authority"] = "authorized" if status in {"executed", "unknown", "partial", "reconciled", "observed"} else status
    return {"assessment": assessment, "execution": {"attempted": bool(getattr(result, "attempted", status == "executed")), "attempt_status": status, "newly_executed": bool(getattr(result, "newly_executed", False)), "destination_observed": getattr(result, "observed_state", artifacts["execution_facts"].get("destination_observed")), "effect_id": getattr(result, "effect_id", getattr(artifacts["decision"], "effect_id", None)), "decision_id": getattr(result, "decision_id", getattr(artifacts["decision"], "decision_id", None)), "attempt_id": getattr(result, "attempt_id", None)}, "current_reconstruction_bundle": artifacts["reconstruction_bundle"], "odes_reference": artifacts["odes_reference"], "successor_packet": artifacts["successor_packet"], "execution_facts": artifacts["execution_facts"]}


class TransactionalExchangeStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS messages(message_id TEXT PRIMARY KEY, message_digest TEXT NOT NULL, conversation_id TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS lineage(packet_id TEXT PRIMARY KEY, packet_digest TEXT NOT NULL, conversation_id TEXT NOT NULL, parent_packet_id TEXT, state_version INTEGER NOT NULL, source_packet_id TEXT NOT NULL, source_commitment TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS heads(conversation_id TEXT PRIMARY KEY, packet_id TEXT NOT NULL, packet_digest TEXT NOT NULL, state_version INTEGER NOT NULL);
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def seen_messages(self) -> dict[str, str]:
        with self._connect() as conn:
            return {row["message_id"]: row["message_digest"] for row in conn.execute("SELECT message_id,message_digest FROM messages")}

    def record_message(self, message: dict[str, Any]) -> tuple[bool, str | None]:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT message_digest FROM messages WHERE message_id=?", (message["message_id"],)).fetchone()
            if row and row["message_digest"] != message["message_digest"]:
                conn.execute("ROLLBACK")
                return False, "GAX-INTEGRITY-MESSAGE-ID-REUSE"
            if row:
                conn.execute("COMMIT")
                return True, None
            conn.execute("INSERT INTO messages VALUES(?,?,?)", (message["message_id"], message["message_digest"], message["conversation_id"]))
            conn.execute("COMMIT")
            return False, None

    @staticmethod
    def _material_digest(packet: dict[str, Any]) -> str:
        return digest({k: v for k, v in packet.items() if k != "packet_digest"})

    def accept_successor(self, packet: dict[str, Any], predecessor: dict[str, Any]) -> dict[str, Any]:
        if packet.get("packet_digest") != self._material_digest(packet):
            return {"loaded": False, "effect_created": False, "status": "packet_digest_mismatch"}
        if packet.get("profile") != PROFILE or packet.get("schema_version") != PROTOCOL_VERSION:
            return {"loaded": False, "effect_created": False, "status": "unsupported_profile_or_schema"}
        if packet.get("conversation_id") != predecessor.get("conversation_id"):
            return {"loaded": False, "effect_created": False, "status": "conversation_mismatch"}
        if packet.get("source_packet_id") not in {predecessor.get("message_id"), predecessor.get("packet_id")}:
            return {"loaded": False, "effect_created": False, "status": "source_identity_mismatch"}
        predecessor_material_digest = self._material_digest(predecessor)
        supplied_predecessor_digest = predecessor.get("packet_digest")
        predecessor_digest = supplied_predecessor_digest or predecessor_material_digest
        if packet.get("source_commitment") != predecessor_material_digest:
            return {"loaded": False, "effect_created": False, "status": "source_commitment_mismatch"}
        try:
            version = int(str(packet.get("state_version", "")).split("-")[-1])
        except Exception:
            return {"loaded": False, "effect_created": False, "status": "unsupported_state_version"}
        conversation_id = packet["conversation_id"]
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            head = conn.execute("SELECT packet_id,packet_digest,state_version FROM heads WHERE conversation_id=?", (conversation_id,)).fetchone()
            parent = packet.get("predecessor_packet_id")
            if head is not None:
                if not parent:
                    conn.execute("ROLLBACK")
                    return {"loaded": False, "effect_created": False, "status": "missing_predecessor_packet_id"}
                if parent != head["packet_id"]:
                    conn.execute("ROLLBACK")
                    return {"loaded": False, "effect_created": False, "status": "divergent_branch"}
                if packet.get("source_packet_id") != head["packet_id"]:
                    conn.execute("ROLLBACK")
                    return {"loaded": False, "effect_created": False, "status": "source_not_accepted_head"}
                if supplied_predecessor_digest and supplied_predecessor_digest != predecessor_material_digest:
                    conn.execute("ROLLBACK")
                    return {"loaded": False, "effect_created": False, "status": "predecessor_digest_mismatch"}
                if predecessor_digest != head["packet_digest"]:
                    conn.execute("ROLLBACK")
                    return {"loaded": False, "effect_created": False, "status": "predecessor_content_not_accepted_head"}
                if version <= int(head["state_version"]):
                    conn.execute("ROLLBACK")
                    return {"loaded": False, "effect_created": False, "status": "stale_or_rollback"}
            elif parent:
                parent_row = conn.execute("SELECT conversation_id FROM lineage WHERE packet_id=?", (parent,)).fetchone()
                conn.execute("ROLLBACK")
                if parent_row and parent_row["conversation_id"] != conversation_id:
                    return {"loaded": False, "effect_created": False, "status": "conversation_mismatch"}
                return {"loaded": False, "effect_created": False, "status": "unknown_predecessor"}
            existing = conn.execute("SELECT packet_digest FROM lineage WHERE packet_id=?", (packet["packet_id"],)).fetchone()
            if existing and existing["packet_digest"] != packet["packet_digest"]:
                conn.execute("ROLLBACK")
                return {"loaded": False, "effect_created": False, "status": "divergent_history"}
            conn.execute("INSERT OR IGNORE INTO lineage VALUES(?,?,?,?,?,?,?)", (packet["packet_id"], packet["packet_digest"], conversation_id, parent, version, packet["source_packet_id"], packet["source_commitment"]))
            conn.execute("INSERT OR REPLACE INTO heads VALUES(?,?,?,?)", (conversation_id, packet["packet_id"], packet["packet_digest"], version))
            conn.execute("COMMIT")
            return {"loaded": True, "effect_created": False, "status": "loaded_for_reconciliation"}

DurableExchangeStore = TransactionalExchangeStore


def make_successor_packet(predecessor: dict[str, Any], *, facts: dict[str, Any], state_version: int = 1, current_bundle: dict[str, Any] | None = None, predecessor_packet_id: str | None = None) -> dict[str, Any]:
    source_id = predecessor.get("packet_id") or predecessor.get("message_id")
    source_commitment = digest({k: v for k, v in predecessor.items() if k != "packet_digest"})
    packet = {"packet_id": f"succ-{source_id}-{state_version}", "profile": PROFILE, "schema_version": PROTOCOL_VERSION, "conversation_id": predecessor.get("conversation_id"), "source_packet_id": source_id, "source_commitment": source_commitment, "predecessor_packet_id": predecessor_packet_id, "state_version": f"state-{state_version}", "relevant_decisions": [facts.get("decision_id")], "pending_effects": facts.get("pending_effects", []), "attempts": facts.get("attempts", {}), "unresolved_delivery": facts.get("unresolved_delivery"), "missing_evidence": [] if current_bundle else ["current_bundle_unavailable"], "next_proposed_work": "reconcile before any further execution"}
    packet["packet_digest"] = digest({k: v for k, v in packet.items() if k != "packet_digest"})
    return packet


def load_successor_packet(packet: dict[str, Any], destination: Any, *, predecessor: dict[str, Any], store_path: str | Path) -> dict[str, Any]:
    return TransactionalExchangeStore(store_path).accept_successor(packet, predecessor)


def run_demo(manifest_path: str, replay_path: str, out_path: str) -> dict[str, Any]:
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    bundle = json.loads(Path(replay_path).read_text(encoding="utf-8"))
    tmp = Path(out_path).parent
    proposal = runtime_proposal_model(bundle)
    resolver = _build_resolver(proposal, now=parse_time(EVAL))
    h = load_actual_pinned_moltbot_helpers()
    destination = h.DurableRefundDestination(tmp / "demo-moltbot-state")
    result = run_exchange(make_message(bundle), bundle, LocalRegistry({"refund-sender"}, {"refund-recipient"}, "refund-recipient"), destination, evaluation_time=EVAL, manifest=manifest, store_path=tmp / "demo_exchange.sqlite", resolver=resolver)
    Path(out_path).write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--replay", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    run_demo(args.manifest, args.replay, args.out)
    return 0
