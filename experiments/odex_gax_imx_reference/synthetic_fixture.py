from __future__ import annotations

import json
import os
from pathlib import Path

from .executor_runtime import load_public_executor_runtime, manifest_execution_policy
from .gax_ref_runtime import (
    EVAL,
    _build_resolver,
    _effect_rows,
    _export_sources,
    _pipeline,
    make_message,
    parse_time,
    runtime_proposal_model,
)


def _json_env(name: str) -> dict:
    return json.loads(Path(os.environ[name]).read_text(encoding="utf-8"))


def _integrated(tmp_path: Path):
    manifest = _json_env("UPSTREAM_MANIFEST_EXAMPLE")
    bundle = _json_env("UPSTREAM_REPLAY_SUCCESS_EXAMPLE")
    proposal = runtime_proposal_model(bundle)
    resolver = _build_resolver(proposal, now=parse_time(EVAL))
    h = load_public_executor_runtime()
    records = h.BoundedRecordStore(tmp_path / "cp-run.json", proposal.run_id or "run-1")
    cp_destination = h.LocalRefundDestination(tmp_path / "cp-placeholder.json")
    workflow = h.BoundedAuthorizationWorkflow(
        manifest=manifest,
        resolver=resolver,
        destination=cp_destination,
        records=records,
    )
    decision = workflow.decide(proposal, now=parse_time(EVAL))
    if decision.result != "authorized" or decision.binding is None:
        raise RuntimeError("synthetic fixture expected authorized decision")
    context = resolver.authority_context(proposal.authority_context_ref or "")
    binding = decision.binding
    operation = h.ExecutionOperation(
        actor=proposal.actor,
        principal=proposal.principal,
        institution_id=context["institution"]["institution_id"],
        authority_domain=context["institution"]["authority_domain"],
        manifest_id=proposal.manifest_id,
        manifest_version=proposal.manifest_version,
        manifest_digest=proposal.manifest_digest,
        proposal_commitment=h.cp_commitment(proposal.model_dump(mode="json", exclude_none=False)),
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
    request = h.ExecutionEnvelope(
        h.EXECUTION_ENVELOPE_VERSION,
        decision.decision_id,
        decision.effect_id,
        operation,
    )
    destination = h.DurableRefundDestination(tmp_path / "moltbot-state")
    executor = h.PinnedControlPlaneExecutor(
        workflow=workflow,
        destination=destination,
        policy=manifest_execution_policy(manifest, proposal, resolver),
    )
    return manifest, bundle, h, proposal, resolver, workflow, decision, destination, executor, request


def run_actual_outcome(tmp_path: Path, outcome: str) -> dict:
    """Synthetic integration fixture. Not a production authority source."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    manifest = _json_env("UPSTREAM_MANIFEST_EXAMPLE")
    bundle = _json_env("UPSTREAM_REPLAY_SUCCESS_EXAMPLE")
    message = make_message(bundle)

    if outcome == "hold":
        proposal = runtime_proposal_model(bundle)
        resolver = _build_resolver(proposal, now=parse_time(EVAL))
        grant = resolver.contexts[proposal.authority_context_ref]["grant"]
        resolver.statuses[grant["grant_id"]].status = "revoked"
        h = load_public_executor_runtime()
        records = h.BoundedRecordStore(tmp_path / "cp-run.json", proposal.run_id or "run-1")
        workflow = h.BoundedAuthorizationWorkflow(
            manifest=manifest,
            resolver=resolver,
            destination=h.LocalRefundDestination(tmp_path / "cp-placeholder.json"),
            records=records,
        )
        decision = workflow.decide(proposal, now=parse_time(EVAL))
        cp = workflow.records.load().model_dump(mode="json")
        pipe = _pipeline(
            manifest,
            cp,
            proposal.model_dump(mode="json", exclude_none=False),
            None,
            predecessor=message,
        )
        return {
            "status": decision.result,
            "decision": decision,
            "destination_effects": {},
            **pipe,
        }

    (
        _manifest,
        _bundle,
        h,
        proposal,
        resolver,
        workflow,
        decision,
        destination,
        executor,
        request,
    ) = _integrated(tmp_path)

    if outcome == "success":
        result = executor.execute(
            envelope=request, proposal=proposal, decision=decision, now=parse_time(EVAL)
        )
    elif outcome == "denied_after_decision":
        grant = resolver.contexts[proposal.authority_context_ref]["grant"]
        resolver.statuses[grant["grant_id"]].status = "revoked"
        result = executor.execute(
            envelope=request, proposal=proposal, decision=decision, now=parse_time(EVAL)
        )
    elif outcome == "lost_ack":
        result = executor.execute(
            envelope=request,
            proposal=proposal,
            decision=decision,
            now=parse_time(EVAL),
            simulate="lost_ack",
        )
    elif outcome == "restart_reconciliation":
        executor.execute(
            envelope=request,
            proposal=proposal,
            decision=decision,
            now=parse_time(EVAL),
            simulate="lost_ack",
        )
        destination = h.DurableRefundDestination(destination.root)
        executor = h.PinnedControlPlaneExecutor(
            workflow=workflow,
            destination=destination,
            policy=manifest_execution_policy(manifest, proposal, resolver),
        )
        result = executor.execute(
            envelope=request, proposal=proposal, decision=decision, now=parse_time(EVAL)
        )
    elif outcome == "duplicate_delivery":
        executor.execute(
            envelope=request, proposal=proposal, decision=decision, now=parse_time(EVAL)
        )
        result = executor.execute(
            envelope=request, proposal=proposal, decision=decision, now=parse_time(EVAL)
        )
    elif outcome == "partial":
        result = executor.execute(
            envelope=request,
            proposal=proposal,
            decision=decision,
            now=parse_time(EVAL),
            simulate="partial",
        )
    else:
        raise ValueError(f"unsupported outcome: {outcome}")

    cp, p, m = _export_sources(workflow, proposal, request, result, destination)
    pipe = _pipeline(manifest, cp, p, m, predecessor=message)
    return {
        "status": result.status,
        "result": result,
        "decision": decision,
        "request": request,
        "destination": destination,
        "destination_effects": {row["effect_id"]: row for row in _effect_rows(destination)},
        **pipe,
    }
