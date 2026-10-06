from __future__ import annotations

import argparse
import dataclasses
import importlib
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
MOLTBOT_REVISION = "894e1c115cb91229c474a906c51ea9af7999e675"
MOLTBOT_PRODUCER_PROFILE_VERSION = "1.0.0"
GAX_RESULT_PROFILE = "cognous.gax.transport-result"
GAX_RESULT_PROFILE_VERSION = "1.0.0"
MANIFEST_REVISION = "46c950bed37fe3812000895430bc0312d29e37ce"
REPLAY_REVISION = "f12648313cedc2cf06145d397fa56cdea18cc800"
ODES_REVISION = "b3a2f1e72df88cd24d93d1b7d69963f43139e749"
ALVORADA_REVISION = "fb3d97938969a89e149e8ff8db2756091d1233fc"


def _repo_path(env_name: str, default: str) -> Path:
    return Path(os.environ.get(env_name, default)).resolve()


def load_public_executor_runtime():
    from .executor_runtime import load_public_executor_runtime as _load

    return _load()


# Test/source-compatibility alias. This no longer loads any test module.
load_actual_pinned_moltbot_helpers = load_public_executor_runtime


def actual_executor_classes() -> dict[str, Any]:
    h = load_public_executor_runtime()
    return {
        "PinnedControlPlaneExecutor": h.PinnedControlPlaneExecutor,
        "ControlPlaneRefundDestinationAdapter": importlib.import_module(
            "engine.control_plane_adapter"
        ).ControlPlaneRefundDestinationAdapter,
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


def _asdict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return dict(value)
    return dict(value.__dict__)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _json_loads(value: str | None) -> Any:
    return json.loads(value) if value else None


def _manifest() -> dict[str, Any]:
    path = _repo_path("UPSTREAM_MANIFEST_EXAMPLE", "upstream/manifest/examples/refund_integration_v1_1.manifest.json")
    return json.loads(path.read_text(encoding="utf-8"))


def _export_sources(workflow: Any, proposal: Any, request: Any, result: Any, destination: Any) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    h = load_public_executor_runtime()
    producer = h.export_execution_producer_record(
        envelope=request,
        result=result,
        destination=destination,
        repository_revision=MOLTBOT_REVISION,
        provenance_state="source_asserted",
    )
    h.validate_execution_producer_record(producer)
    cp_record = workflow.records.load().model_dump(mode="json")
    return cp_record, proposal.model_dump(mode="json", exclude_none=False), producer


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
    return {"odes_package": package, "recipient_validation": validation, "recipient_policy": policy, "exchange_metadata": {"note": "additional exchange metadata is outside the digest-covered ODES package"}}


def _artifact_result(
    *,
    artifacts: dict[str, Any],
    decision_id: str | None,
    effect_id: str | None,
    attempt_id: str | None,
    original: bool,
    lineage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    reconstruction = artifacts["reconstruction_bundle"]
    odes = artifacts["odes_reference"]
    successor = artifacts["successor_packet"]
    return {
        "result_profile": GAX_RESULT_PROFILE,
        "result_profile_version": GAX_RESULT_PROFILE_VERSION,
        "retention_state": "complete_original" if original else "complete_regenerated_derivative",
        "original_artifacts": original,
        "lineage": lineage,
        "producer_refs": {
            "decision_id": decision_id,
            "effect_id": effect_id,
            "executor_attempt_id": attempt_id,
            "executor_repository_revision": MOLTBOT_REVISION,
            "executor_producer_profile_version": MOLTBOT_PRODUCER_PROFILE_VERSION,
            "reconstruction_bundle_id": reconstruction.get("bundle_id"),
            "odes_package_digest": (odes.get("odes_package") or {}).get("package_digest"),
            "successor_packet_id": (successor or {}).get("packet_id"),
        },
        "commitments": {
            "reconstruction_bundle": digest(reconstruction),
            "odes_package": (odes.get("odes_package") or {}).get("package_digest"),
            "successor_packet": (successor or {}).get("packet_digest"),
        },
        "reconstruction_bundle": reconstruction,
        "odes_reference": odes,
        "successor_packet": successor,
    }


def _validate_artifact_result(result: dict[str, Any]) -> None:
    if result.get("result_profile") != GAX_RESULT_PROFILE:
        raise ValueError("unsupported GAX result profile")
    if result.get("result_profile_version") != GAX_RESULT_PROFILE_VERSION:
        raise ValueError("unsupported GAX result profile version")
    reconstruction = result.get("reconstruction_bundle")
    odes = result.get("odes_reference")
    successor = result.get("successor_packet")
    if not isinstance(reconstruction, dict) or not isinstance(odes, dict):
        raise ValueError("retained GAX result is incomplete")
    commitments = result.get("commitments") or {}
    if commitments.get("reconstruction_bundle") != digest(reconstruction):
        raise ValueError("retained reconstruction digest mismatch")
    package = odes.get("odes_package") or {}
    if commitments.get("odes_package") != package.get("package_digest"):
        raise ValueError("retained ODES package digest mismatch")
    if successor is not None and commitments.get("successor_packet") != successor.get("packet_digest"):
        raise ValueError("retained successor digest mismatch")
    refs = result.get("producer_refs") or {}
    if refs.get("reconstruction_bundle_id") != reconstruction.get("bundle_id"):
        raise ValueError("retained reconstruction identity mismatch")
    if refs.get("successor_packet_id") != (successor or {}).get("packet_id"):
        raise ValueError("retained successor identity mismatch")


def _artifact_facts(odes_reference: dict[str, Any]) -> dict[str, Any]:
    facts = dict(odes_reference["odes_package"].get("provenance", {}).get("execution_facts", {}))
    effects = facts.get("destination_effects") or []
    effect_ids = [item.get("effect_id") for item in effects if isinstance(item, dict) and item.get("effect_id")]
    destination = facts.get("destination_observed")
    ack = facts.get("acknowledgement_summary")
    facts["pending_effects"] = effect_ids if destination == "partial" else []
    facts["unresolved_delivery"] = bool(destination == "partial" or (ack == "unknown" and (effect_ids or destination in {"applied", "partial", "unknown"})))
    return facts


def _pipeline(manifest: dict[str, Any], cp_record: dict[str, Any], proposal: dict[str, Any] | None, moltbot: dict[str, Any] | None, *, predecessor: dict[str, Any] | None = None, predecessor_packet_id: str | None = None, state_version: int = 1) -> dict[str, Any]:
    reconstructed = import_replay_bundle(cp_record, proposal, moltbot)
    bundle = reconstruction_dict(reconstructed)
    odes = export_odes_reference(manifest, bundle)
    facts = _artifact_facts(odes)
    successor = None
    if predecessor is not None:
        successor = make_successor_packet(predecessor, facts=facts, state_version=state_version, current_bundle=bundle, predecessor_packet_id=predecessor_packet_id)
    return {"reconstruction": reconstructed, "reconstruction_bundle": bundle, "odes_reference": odes, "successor_packet": successor, "execution_facts": facts}


def execution_facts(bundle: dict[str, Any]) -> dict[str, Any]:
    return dict(export_odes_reference(_manifest(), bundle)["odes_package"].get("provenance", {}).get("execution_facts", {}))


def _build_request_from_decision(proposal: Any, resolver: Any, decision: Any) -> Any:
    h = load_public_executor_runtime()
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


class Obj:
    def __init__(self, values: dict[str, Any]):
        self.__dict__.update(values)


def _validate_bound_association(association: dict[str, Any]) -> str | None:
    proposal = association.get("proposal")
    decision_id = association.get("decision_id")
    effect_id = association.get("effect_id")
    if not proposal:
        return "stored_proposal_unavailable"
    producer = association.get("moltbot")
    if not producer:
        return None
    try:
        load_public_executor_runtime().validate_execution_producer_record(producer)
    except Exception:
        return "stored_executor_producer_contract_invalid"
    envelope = producer.get("execution_envelope") or {}
    result = producer.get("execution_result") or {}
    operation = envelope.get("operation") or {}
    if envelope.get("decision_id") != decision_id or envelope.get("effect_id") != effect_id:
        return "stored_envelope_identity_mismatch"
    if result.get("decision_id") != decision_id or result.get("effect_id") != effect_id:
        return "stored_result_identity_mismatch"
    h = load_public_executor_runtime()
    if operation.get("proposal_commitment") != h.cp_commitment(proposal):
        return "stored_operation_proposal_commitment_mismatch"
    for row in producer.get("effects") or []:
        if row.get("effect_id") != effect_id:
            return "stored_effect_identity_mismatch"
    for row in producer.get("attempts") or []:
        if row.get("effect_id") != effect_id or row.get("decision_id") != decision_id:
            return "stored_attempt_identity_mismatch"
    return None


def _execution_response(assessment: dict[str, Any], status: str, result: Any, decision: Any, artifacts: dict[str, Any], *, attempted_override: bool | None = None, newly_executed_override: bool | None = None) -> dict[str, Any]:
    assessment["stages"]["authority"] = "authorized" if status in {"executed", "unknown", "partial", "reconciled", "observed"} else status
    attempted = bool(getattr(result, "attempted", status == "executed"))
    newly_executed = bool(getattr(result, "newly_executed", False))
    if attempted_override is not None:
        attempted = attempted_override
    if newly_executed_override is not None:
        newly_executed = newly_executed_override
    return {
        "assessment": assessment,
        "execution": {
            "attempted": attempted,
            "attempt_status": status,
            "newly_executed": newly_executed,
            "destination_observed": getattr(result, "observed_state", artifacts["execution_facts"].get("destination_observed")),
            "effect_id": getattr(result, "effect_id", getattr(decision, "effect_id", None)),
            "decision_id": getattr(result, "decision_id", getattr(decision, "decision_id", None)),
            "attempt_id": getattr(result, "attempt_id", None),
        },
        "current_reconstruction_bundle": artifacts["reconstruction_bundle"],
        "odes_reference": artifacts["odes_reference"],
        "successor_packet": artifacts["successor_packet"],
        "execution_facts": artifacts["execution_facts"],
    }


def _artifacts_from_association(manifest: dict[str, Any], message: dict[str, Any], association: dict[str, Any]) -> dict[str, Any]:
    return _pipeline(manifest, association["cp_record"], association["proposal"], association.get("moltbot"), predecessor=message)


def _duplicate_redelivery_result(message: dict[str, Any], assessment: dict[str, Any], manifest: dict[str, Any], association: dict[str, Any], store: "TransactionalExchangeStore") -> dict[str, Any]:
    retained = store.artifact_result_for_message(message)
    problem = _validate_bound_association(association)
    if problem:
        assessment["errors"].append("GAX-EXECUTION-STORED-WORKFLOW-INTEGRITY-FAILED")
        return {"assessment": assessment, "execution": {"attempted": False, "reason": problem, "attempt_status": "unresolved_duplicate"}, "successor_packet": None}
    if retained is not None:
        artifacts = {
            "reconstruction_bundle": retained["reconstruction_bundle"],
            "odes_reference": retained["odes_reference"],
            "successor_packet": retained.get("successor_packet"),
            "execution_facts": _artifact_facts(retained["odes_reference"]),
        }
    else:
        artifacts = _artifacts_from_association(manifest, message, association)
    result_data = association.get("result") or {}
    status = association.get("status") or result_data.get("status") or "unresolved"
    if status == "executed":
        status = "reconciled"
    result = Obj({**result_data, "status": status, "newly_executed": False}) if result_data else None
    decision = Obj({"decision_id": association.get("decision_id"), "effect_id": association.get("effect_id")})
    response = _execution_response(assessment, status, result, decision, artifacts, attempted_override=False, newly_executed_override=False)
    response["artifact_result"] = retained
    return response


def _recover_checkpoint(message: dict[str, Any], assessment: dict[str, Any], manifest: dict[str, Any], store: "TransactionalExchangeStore", checkpoint: dict[str, Any], destination: Any | None) -> dict[str, Any]:
    problem = _validate_bound_association(checkpoint)
    if problem:
        assessment["errors"].append("GAX-EXECUTION-CHECKPOINT-INTEGRITY-FAILED")
        return {"assessment": assessment, "execution": {"attempted": False, "reason": problem, "attempt_status": "hold"}, "successor_packet": None}
    if checkpoint.get("moltbot") and checkpoint.get("result"):
        retained = store.artifact_result_for_message(message)
        if retained is None:
            artifacts = _artifacts_from_association(manifest, message, checkpoint)
            retained = _artifact_result(
                artifacts=artifacts,
                decision_id=checkpoint.get("decision_id"),
                effect_id=checkpoint.get("effect_id"),
                attempt_id=checkpoint.get("attempt_id"),
                original=False,
                lineage={
                    "relationship": "regenerated_from_retained_source_records",
                    "original_artifacts": "unavailable",
                    "replacement_effect_executed": False,
                },
            )
            store.record_artifact_result(message, retained)
        else:
            artifacts = {
                "reconstruction_bundle": retained["reconstruction_bundle"],
                "odes_reference": retained["odes_reference"],
                "successor_packet": retained.get("successor_packet"),
                "execution_facts": _artifact_facts(retained["odes_reference"]),
            }
        store.record_workflow(message, {"proposal_record": checkpoint["proposal"], "decision": checkpoint["decision"], "request": checkpoint.get("request"), "result": checkpoint.get("result"), "status": checkpoint.get("status") or checkpoint["result"].get("status"), "cp_record": checkpoint["cp_record"], "moltbot_record": checkpoint.get("moltbot")})
        status = checkpoint.get("status") or checkpoint["result"].get("status") or "reconciled"
        if status == "executed":
            status = "reconciled"
        result = Obj({**checkpoint["result"], "status": status, "newly_executed": False})
        decision = Obj({"decision_id": checkpoint.get("decision_id"), "effect_id": checkpoint.get("effect_id")})
        response = _execution_response(assessment, status, result, decision, artifacts, attempted_override=False, newly_executed_override=False)
        response["artifact_result"] = retained
        return response
    if destination is not None:
        rows = [r for r in _effect_rows(destination) if r.get("effect_id") == checkpoint.get("effect_id")]
        if rows:
            assessment["errors"].append("GAX-EXECUTION-CHECKPOINT-DESTINATION-OBSERVED-WITHOUT-RETAINED-ATTEMPT")
            return {"assessment": assessment, "execution": {"attempted": False, "reason": "destination_observed_without_replayable_attempt", "attempt_status": "hold", "effect_id": checkpoint.get("effect_id"), "decision_id": checkpoint.get("decision_id")}, "successor_packet": None}
    assessment["errors"].append("GAX-EXECUTION-CHECKPOINT-UNRESOLVED")
    return {"assessment": assessment, "execution": {"attempted": False, "reason": "checkpoint_without_dispatch_evidence", "attempt_status": "hold", "effect_id": checkpoint.get("effect_id"), "decision_id": checkpoint.get("decision_id")}, "successor_packet": None}


def _run_current_request(*, message: dict[str, Any], bundle: dict[str, Any], manifest: dict[str, Any], destination: Any, store_path: Path, resolver: Any, evaluation_time: str, mutate_resolver_after_decision=None, lose_ack: bool = False, partial_delivery: bool = False, store: "TransactionalExchangeStore" | None = None, fault_after_dispatch: bool = False, fault_evidence_once: bool = False) -> dict[str, Any]:
    h = load_public_executor_runtime()
    now = parse_time(evaluation_time)
    proposal = runtime_proposal_model(bundle)
    record_suffix = digest({"message_id": message["message_id"], "message_digest": message.get("message_digest"), "evaluation_time": evaluation_time})[-16:]
    record_store = h.BoundedRecordStore(store_path.parent / f"control-plane-{message['message_id']}-{record_suffix}.json", proposal.run_id or "run-gax-imx")
    cp_destination = h.LocalRefundDestination(store_path.parent / f"cp-placeholder-{message['message_id']}-{record_suffix}.json")
    workflow = h.BoundedAuthorizationWorkflow(manifest=manifest, resolver=resolver, destination=cp_destination, records=record_store)
    decision = workflow.decide(proposal, now=now)
    proposal_record = proposal.model_dump(mode="json", exclude_none=False)
    cp_record = record_store.load().model_dump(mode="json")

    if decision.result != "authorized":
        pipe = _pipeline(manifest, cp_record, proposal_record, None, predecessor=message)
        return {"status": decision.result, "decision": decision, "result": None, "destination_effects": {}, "proposal_record": proposal_record, "cp_record": cp_record, "moltbot_record": None, **pipe}

    if mutate_resolver_after_decision:
        mutate_resolver_after_decision(resolver)

    request = _build_request_from_decision(proposal, resolver, decision)
    if store is not None:
        store.record_dispatch_checkpoint(message, {"status": "bound", "proposal_record": proposal_record, "decision": _asdict(decision), "request": _asdict(request), "cp_record": cp_record})
    from .executor_runtime import manifest_execution_policy
    executor = h.PinnedControlPlaneExecutor(
        workflow=workflow,
        destination=destination,
        policy=manifest_execution_policy(manifest, proposal, resolver),
    )
    simulate = "partial" if partial_delivery else ("lost_ack" if lose_ack else None)
    result = executor.execute(envelope=request, proposal=proposal, decision=decision, now=now, simulate=simulate)
    cp_record, p, moltbot = _export_sources(workflow, proposal, request, result, destination)
    checkpoint_payload = {"status": result.status, "proposal_record": p, "decision": _asdict(decision), "request": _asdict(request), "result": _asdict(result), "cp_record": cp_record, "moltbot_record": moltbot}
    if store is not None:
        store.record_dispatch_checkpoint(message, checkpoint_payload)
    if fault_after_dispatch:
        return {"status": "interrupted_after_dispatch", "result": result, "decision": decision, "request": request, "destination": destination, "destination_effects": {row["effect_id"]: row for row in _effect_rows(destination)}, "proposal_record": p, "cp_record": cp_record, "moltbot_record": moltbot, "fault": "after_dispatch_before_workflow_persistence"}
    if fault_evidence_once and store is not None and not store.consume_fault_once(message["message_id"], "evidence_export"):
        return {"status": "evidence_export_failed", "result": result, "decision": decision, "request": request, "destination": destination, "destination_effects": {row["effect_id"]: row for row in _effect_rows(destination)}, "proposal_record": p, "cp_record": cp_record, "moltbot_record": moltbot, "fault": "replay_odes_export_failed_after_dispatch"}
    pipe = _pipeline(manifest, cp_record, p, moltbot, predecessor=message)
    return {"status": result.status, "result": result, "decision": decision, "request": request, "destination": destination, "destination_effects": {row["effect_id"]: row for row in _effect_rows(destination)}, "proposal_record": p, "cp_record": cp_record, "moltbot_record": moltbot, **pipe}


def run_exchange(message: dict[str, Any], bundle: dict[str, Any], registry: LocalRegistry, destination: Any, *, evaluation_time: str, manifest: dict[str, Any], store_path: str | Path, resolver: Any | None = None, mutate_resolver_after_decision=None, lose_ack: bool = False, partial_delivery: bool = False, fault_after_dispatch: bool = False, fault_evidence_once: bool = False) -> dict[str, Any]:
    store_path = Path(store_path)
    store = TransactionalExchangeStore(store_path)
    duplicate, err = store.record_message(message)
    if err:
        return {"assessment": {"permitted_handling": "REFUSE", "errors": [err], "stages": {"identity_binding": "failed"}}, "execution": {"attempted": False, "reason": err}, "successor_packet": None}
    seen_messages = store.seen_messages()
    if seen_messages.get(message["message_id"]) == message["message_digest"]:
        seen_messages = {key: value for key, value in seen_messages.items() if key != message["message_id"]}
    assessment = assess_message(message, bundle, registry, evaluation_time=evaluation_time, seen_messages=seen_messages)
    if assessment["permitted_handling"] != "ACCEPT_FOR_ASSESSMENT":
        return {"assessment": assessment, "execution": {"attempted": False, "reason": "message_not_execution_eligible"}, "successor_packet": None}

    association = store.workflow_for_message(message)
    if duplicate and association is not None:
        return _duplicate_redelivery_result(message, assessment, manifest, association, store)
    checkpoint = store.checkpoint_for_message(message)
    if duplicate and checkpoint is not None:
        return _recover_checkpoint(message, assessment, manifest, store, checkpoint, destination)
    if duplicate and (resolver is None or destination is None):
        assessment["errors"].append("GAX-EXECUTION-RECEIPT-WITHOUT-COMPLETED-WORKFLOW")
        return {"assessment": assessment, "execution": {"attempted": False, "reason": "receipt_without_completed_workflow", "attempt_status": "unresolved_duplicate"}, "successor_packet": None}
    if resolver is None:
        assessment["errors"].append("GAX-AUTHORITY-TRUSTED-RESOLVER-REQUIRED")
        return {"assessment": assessment, "execution": {"attempted": False, "reason": "trusted_resolver_required"}, "successor_packet": None}
    if destination is None:
        assessment["errors"].append("GAX-EXECUTION-DESTINATION-REQUIRED")
        return {"assessment": assessment, "execution": {"attempted": False, "reason": "destination_required"}, "successor_packet": None}

    artifacts = _run_current_request(message=message, bundle=bundle, manifest=manifest, destination=destination, store_path=store_path, resolver=resolver, evaluation_time=evaluation_time, mutate_resolver_after_decision=mutate_resolver_after_decision, lose_ack=lose_ack, partial_delivery=partial_delivery, store=store, fault_after_dispatch=fault_after_dispatch, fault_evidence_once=fault_evidence_once)
    result = artifacts.get("result")
    status = artifacts["status"]
    if artifacts.get("fault") == "after_dispatch_before_workflow_persistence":
        return {"assessment": assessment, "execution": {"attempted": True, "attempt_status": "interrupted_after_dispatch", "newly_executed": getattr(result, "newly_executed", False), "effect_id": getattr(result, "effect_id", None), "decision_id": getattr(result, "decision_id", None), "attempt_id": getattr(result, "attempt_id", None), "reason": artifacts["fault"]}, "successor_packet": None}
    if artifacts.get("fault") == "replay_odes_export_failed_after_dispatch":
        return {"assessment": assessment, "execution": {"attempted": True, "attempt_status": "evidence_export_failed", "newly_executed": getattr(result, "newly_executed", False), "effect_id": getattr(result, "effect_id", None), "decision_id": getattr(result, "decision_id", None), "attempt_id": getattr(result, "attempt_id", None), "reason": artifacts["fault"]}, "successor_packet": None}
    artifact_result = _artifact_result(
        artifacts=artifacts,
        decision_id=getattr(result, "decision_id", getattr(artifacts["decision"], "decision_id", None)),
        effect_id=getattr(result, "effect_id", getattr(artifacts["decision"], "effect_id", None)),
        attempt_id=getattr(result, "attempt_id", None),
        original=True,
    )
    store.record_artifact_result(message, artifact_result)
    store.record_workflow(message, artifacts)
    response = _execution_response(assessment, status, result, artifacts["decision"], artifacts)
    response["artifact_result"] = artifact_result
    return response


class TransactionalExchangeStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS messages(message_id TEXT PRIMARY KEY, message_digest TEXT NOT NULL, conversation_id TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS message_workflows(message_id TEXT PRIMARY KEY,message_digest TEXT NOT NULL,conversation_id TEXT NOT NULL,status TEXT NOT NULL,proposal_commitment TEXT,decision_id TEXT,effect_id TEXT,attempt_id TEXT,proposal_json TEXT NOT NULL,decision_json TEXT NOT NULL,request_json TEXT,result_json TEXT,cp_record_json TEXT NOT NULL,moltbot_json TEXT);
                CREATE TABLE IF NOT EXISTS dispatch_checkpoints(message_id TEXT PRIMARY KEY,message_digest TEXT NOT NULL,conversation_id TEXT NOT NULL,state TEXT NOT NULL,proposal_commitment TEXT,decision_id TEXT,effect_id TEXT,attempt_id TEXT,proposal_json TEXT NOT NULL,decision_json TEXT NOT NULL,request_json TEXT,result_json TEXT,cp_record_json TEXT NOT NULL,moltbot_json TEXT,faults_json TEXT);
                CREATE TABLE IF NOT EXISTS lineage(packet_id TEXT PRIMARY KEY, packet_digest TEXT NOT NULL, conversation_id TEXT NOT NULL, parent_packet_id TEXT, state_version INTEGER NOT NULL, source_packet_id TEXT NOT NULL, source_commitment TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS heads(conversation_id TEXT PRIMARY KEY, packet_id TEXT NOT NULL, packet_digest TEXT NOT NULL, state_version INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS artifact_results(
                    message_id TEXT PRIMARY KEY,
                    message_digest TEXT NOT NULL,
                    conversation_id TEXT NOT NULL,
                    result_profile TEXT NOT NULL,
                    result_profile_version TEXT NOT NULL,
                    retention_state TEXT NOT NULL,
                    artifact_result_json TEXT NOT NULL
                );
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

    def _association_payload(self, row: sqlite3.Row) -> dict[str, Any]:
        return {"status": row["status"] if "status" in row.keys() else row["state"], "proposal_commitment": row["proposal_commitment"], "decision_id": row["decision_id"], "effect_id": row["effect_id"], "attempt_id": row["attempt_id"], "proposal": _json_loads(row["proposal_json"]), "decision": _json_loads(row["decision_json"]), "request": _json_loads(row["request_json"]), "result": _json_loads(row["result_json"]), "cp_record": _json_loads(row["cp_record_json"]), "moltbot": _json_loads(row["moltbot_json"])}

    def record_dispatch_checkpoint(self, message: dict[str, Any], artifacts: dict[str, Any]) -> None:
        proposal = artifacts["proposal_record"]
        decision = _asdict(artifacts.get("decision"))
        request = _asdict(artifacts.get("request")) if artifacts.get("request") is not None else None
        result = _asdict(artifacts.get("result")) if artifacts.get("result") is not None else None
        state = "dispatched" if result else "bound"
        status = artifacts.get("status") or (result or {}).get("status") or decision.get("result") or state
        decision_id = (result or {}).get("decision_id") or decision.get("decision_id")
        effect_id = (result or {}).get("effect_id") or decision.get("effect_id")
        attempt_id = (result or {}).get("attempt_id")
        proposal_commitment = (request or {}).get("operation", {}).get("proposal_commitment")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT message_digest FROM messages WHERE message_id=?", (message["message_id"],)).fetchone()
            if row is None or row["message_digest"] != message["message_digest"]:
                conn.execute("ROLLBACK")
                raise RuntimeError("message receipt must be recorded before dispatch checkpoint")
            prior = conn.execute("SELECT decision_id,effect_id,proposal_commitment FROM dispatch_checkpoints WHERE message_id=?", (message["message_id"],)).fetchone()
            if prior and (prior["decision_id"] != decision_id or prior["effect_id"] != effect_id or prior["proposal_commitment"] != proposal_commitment):
                conn.execute("ROLLBACK")
                raise RuntimeError("dispatch checkpoint identity cannot be replaced")
            conn.execute("""INSERT OR REPLACE INTO dispatch_checkpoints(message_id,message_digest,conversation_id,state,proposal_commitment,decision_id,effect_id,attempt_id,proposal_json,decision_json,request_json,result_json,cp_record_json,moltbot_json,faults_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,COALESCE((SELECT faults_json FROM dispatch_checkpoints WHERE message_id=?),'{}'))""", (message["message_id"], message["message_digest"], message["conversation_id"], state, proposal_commitment, decision_id, effect_id, attempt_id, _json_dumps(proposal), _json_dumps(decision), _json_dumps(request) if request is not None else None, _json_dumps(result) if result is not None else None, _json_dumps(artifacts["cp_record"]), _json_dumps(artifacts.get("moltbot_record")) if artifacts.get("moltbot_record") is not None else None, message["message_id"]))
            conn.execute("COMMIT")

    def checkpoint_for_message(self, message: dict[str, Any]) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM dispatch_checkpoints WHERE message_id=?", (message["message_id"],)).fetchone()
        if row is None or row["message_digest"] != message["message_digest"] or row["conversation_id"] != message["conversation_id"]:
            return None
        out = self._association_payload(row)
        out["state"] = row["state"]
        return out

    def consume_fault_once(self, message_id: str, fault_name: str) -> bool:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT faults_json FROM dispatch_checkpoints WHERE message_id=?", (message_id,)).fetchone()
            faults = _json_loads(row["faults_json"]) if row and row["faults_json"] else {}
            already = bool(faults.get(fault_name))
            if not already:
                faults[fault_name] = True
                conn.execute("UPDATE dispatch_checkpoints SET faults_json=? WHERE message_id=?", (_json_dumps(faults), message_id))
            conn.execute("COMMIT")
            return already

    def record_workflow(self, message: dict[str, Any], artifacts: dict[str, Any]) -> None:
        proposal = artifacts["proposal_record"]
        decision = _asdict(artifacts.get("decision"))
        request = _asdict(artifacts.get("request")) if artifacts.get("request") is not None else None
        result = _asdict(artifacts.get("result")) if artifacts.get("result") is not None else None
        status = artifacts.get("status") or (result or {}).get("status") or decision.get("result") or "unresolved"
        decision_id = (result or {}).get("decision_id") or decision.get("decision_id")
        effect_id = (result or {}).get("effect_id") or decision.get("effect_id")
        attempt_id = (result or {}).get("attempt_id")
        proposal_commitment = (request or {}).get("operation", {}).get("proposal_commitment")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT message_digest FROM messages WHERE message_id=?", (message["message_id"],)).fetchone()
            if row is None or row["message_digest"] != message["message_digest"]:
                conn.execute("ROLLBACK")
                raise RuntimeError("message receipt must be recorded before workflow association")
            conn.execute("""INSERT OR REPLACE INTO message_workflows(message_id,message_digest,conversation_id,status,proposal_commitment,decision_id,effect_id,attempt_id,proposal_json,decision_json,request_json,result_json,cp_record_json,moltbot_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (message["message_id"], message["message_digest"], message["conversation_id"], status, proposal_commitment, decision_id, effect_id, attempt_id, _json_dumps(proposal), _json_dumps(decision), _json_dumps(request) if request is not None else None, _json_dumps(result) if result is not None else None, _json_dumps(artifacts["cp_record"]), _json_dumps(artifacts.get("moltbot_record")) if artifacts.get("moltbot_record") is not None else None))
            conn.execute("COMMIT")

    def record_artifact_result(self, message: dict[str, Any], artifact_result: dict[str, Any]) -> None:
        _validate_artifact_result(artifact_result)
        encoded = _json_dumps(artifact_result)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            receipt = conn.execute(
                "SELECT message_digest,conversation_id FROM messages WHERE message_id=?",
                (message["message_id"],),
            ).fetchone()
            if receipt is None or receipt["message_digest"] != message["message_digest"]:
                conn.execute("ROLLBACK")
                raise RuntimeError("message receipt must be recorded before artifact retention")
            existing = conn.execute(
                "SELECT artifact_result_json FROM artifact_results WHERE message_id=?",
                (message["message_id"],),
            ).fetchone()
            if existing is not None and existing["artifact_result_json"] != encoded:
                conn.execute("ROLLBACK")
                raise RuntimeError("retained original artifacts cannot be replaced")
            conn.execute(
                """INSERT OR IGNORE INTO artifact_results(
                    message_id,message_digest,conversation_id,result_profile,
                    result_profile_version,retention_state,artifact_result_json
                ) VALUES(?,?,?,?,?,?,?)""",
                (
                    message["message_id"],
                    message["message_digest"],
                    message["conversation_id"],
                    artifact_result["result_profile"],
                    artifact_result["result_profile_version"],
                    artifact_result["retention_state"],
                    encoded,
                ),
            )
            conn.execute("COMMIT")

    def artifact_result_for_message(self, message: dict[str, Any]) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM artifact_results WHERE message_id=?",
                (message["message_id"],),
            ).fetchone()
        if row is None:
            return None
        if row["message_digest"] != message["message_digest"] or row["conversation_id"] != message["conversation_id"]:
            return None
        result = _json_loads(row["artifact_result_json"])
        _validate_artifact_result(result)
        return result

    def workflow_for_message(self, message: dict[str, Any]) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM message_workflows WHERE message_id=?", (message["message_id"],)).fetchone()
        if row is None or row["message_digest"] != message["message_digest"] or row["conversation_id"] != message["conversation_id"]:
            return None
        return self._association_payload(row)

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


def run_actual_outcome(tmp_path: Path, outcome: str) -> dict[str, Any]:
    """Synthetic test fixture only; production run_exchange requires caller resolver."""
    manifest = _manifest()
    bundle = json.loads(Path(os.environ["UPSTREAM_REPLAY_SUCCESS_EXAMPLE"]).read_text(encoding="utf-8"))
    proposal = runtime_proposal_model(bundle)
    resolver = _build_resolver(proposal, now=parse_time(EVAL))
    h = load_public_executor_runtime()
    destination = h.DurableRefundDestination(tmp_path / "moltbot-state")
    message = make_message(bundle)
    store = tmp_path / "fixture-exchange.sqlite"

    if outcome == "hold":
        grant = resolver.contexts[proposal.authority_context_ref]["grant"]
        resolver.statuses[grant["grant_id"]].status = "revoked"
        return run_exchange(message, bundle, LocalRegistry({"refund-sender"}, {"refund-recipient"}, "refund-recipient"), destination, evaluation_time=EVAL, manifest=manifest, store_path=store, resolver=resolver)
    mutate = None
    lose_ack = outcome in {"lost_ack", "restart_reconciliation"}
    partial = outcome == "partial"
    if outcome == "denied_after_decision":
        def mutate(res):
            grant = res.contexts[proposal.authority_context_ref]["grant"]
            res.statuses[grant["grant_id"]].status = "revoked"
    first = run_exchange(
        message,
        bundle,
        LocalRegistry({"refund-sender"}, {"refund-recipient"}, "refund-recipient"),
        destination,
        evaluation_time=EVAL,
        manifest=manifest,
        store_path=store,
        resolver=resolver,
        mutate_resolver_after_decision=mutate,
        lose_ack=lose_ack,
        partial_delivery=partial,
    )
    if outcome in {"duplicate_delivery", "restart_reconciliation"}:
        return run_exchange(
            message,
            bundle,
            LocalRegistry({"refund-sender"}, {"refund-recipient"}, "refund-recipient"),
            destination,
            evaluation_time=EVAL,
            manifest=manifest,
            store_path=store,
            resolver=resolver,
        )
    if outcome not in {"success", "denied_after_decision", "lost_ack", "partial"}:
        raise ValueError(f"unsupported outcome: {outcome}")
    return first

def run_demo(manifest_path: str, replay_path: str, out_path: str) -> dict[str, Any]:
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    bundle = json.loads(Path(replay_path).read_text(encoding="utf-8"))
    tmp = Path(out_path).parent
    proposal = runtime_proposal_model(bundle)
    resolver = _build_resolver(proposal, now=parse_time(EVAL))
    h = load_public_executor_runtime()
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
