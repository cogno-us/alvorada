from __future__ import annotations

import json
import os
from pathlib import Path

from experiments.odex_gax_imx_reference.gax_ref_runtime import (
    _build_resolver,
    load_public_executor_runtime,
    parse_time,
    runtime_proposal_model,
)
from experiments.odex_gax_imx_reference.executor_runtime import manifest_execution_policy

EVAL = "2026-08-08T01:00:00Z"


def _load_json_env(name: str) -> dict:
    value = os.environ[name]
    return json.loads(Path(value).read_text(encoding="utf-8"))


def integrated(tmp_path):
    manifest = _load_json_env("UPSTREAM_MANIFEST_EXAMPLE")
    bundle = _load_json_env("UPSTREAM_REPLAY_SUCCESS_EXAMPLE")
    proposal = runtime_proposal_model(bundle)
    resolver = _build_resolver(proposal, now=parse_time(EVAL))
    h = load_public_executor_runtime()
    cp_destination = h.LocalRefundDestination(tmp_path / "cp-placeholder.json")
    records = h.BoundedRecordStore(tmp_path / "cp-run.json", proposal.run_id or "run-1")
    workflow = h.BoundedAuthorizationWorkflow(
        manifest=manifest,
        resolver=resolver,
        destination=cp_destination,
        records=records,
    )
    decision = workflow.decide(proposal, now=parse_time(EVAL))
    assert decision.result == "authorized"
    context = resolver.authority_context(proposal.authority_context_ref or "")
    binding = decision.binding
    assert binding is not None
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
    return h, proposal, resolver, workflow, decision, destination, executor, request


def policy_for(operation, proposal, resolver):
    return manifest_execution_policy(_load_json_env("UPSTREAM_MANIFEST_EXAMPLE"), proposal, resolver)
