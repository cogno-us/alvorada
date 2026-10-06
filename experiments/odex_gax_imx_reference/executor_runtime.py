from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

MOLTBOT_PRODUCER_PROFILE = "cognous.moltbot-safe.executor"
MOLTBOT_PRODUCER_PROFILE_VERSION = "1.0.0"
MOLTBOT_PROPOSED_REVISION = "894e1c115cb91229c474a906c51ea9af7999e675"


def _repo_path(env_name: str, default: str) -> Path:
    return Path(os.environ.get(env_name, default)).resolve()


def load_public_executor_runtime() -> SimpleNamespace:
    """Load only supported runtime modules from pinned upstream checkouts."""
    cp_root = _repo_path("MOLTBOT_SAFE_CONTROL_PLANE_ROOT", "upstream/control-plane")
    molt_root = _repo_path("MOLTBOT_SAFE_ROOT", "upstream/moltbot-safe")
    missing = [str(p) for p in (cp_root, molt_root) if not p.exists()]
    if missing:
        raise RuntimeError("pinned upstream checkout is unavailable: " + ", ".join(missing))
    for path in (str(cp_root / "src"), str(molt_root)):
        if path not in sys.path:
            sys.path.insert(0, path)

    cp = importlib.import_module("agent_control_plane.bounded")
    safe = importlib.import_module("engine.safe_executor")
    adapter = importlib.import_module("engine.control_plane_adapter")
    producer = importlib.import_module("engine.producer_profile")
    return SimpleNamespace(
        BoundedAuthorizationWorkflow=cp.BoundedAuthorizationWorkflow,
        BoundedRecordStore=cp.BoundedRecordStore,
        LocalRefundDestination=cp.LocalRefundDestination,
        cp_commitment=cp.commitment,
        PinnedControlPlaneExecutor=adapter.PinnedControlPlaneExecutor,
        DurableRefundDestination=safe.DurableRefundDestination,
        ExecutionEnvelope=safe.ExecutionEnvelope,
        ExecutionOperation=safe.ExecutionOperation,
        LocalExecutionPolicy=safe.LocalExecutionPolicy,
        LocalDestinationExecutor=safe.LocalDestinationExecutor,
        EXECUTION_ENVELOPE_VERSION=safe.EXECUTION_ENVELOPE_VERSION,
        snapshot_envelope=safe.snapshot_envelope,
        executor_commitment=safe.commitment,
        export_execution_producer_record=producer.export_execution_producer_record,
        validate_execution_producer_record=producer.validate_execution_producer_record,
        producer_profile=producer.EXECUTOR_PRODUCER_PROFILE,
        producer_profile_version=producer.EXECUTOR_PRODUCER_PROFILE_VERSION,
    )


def manifest_execution_policy(manifest: dict[str, Any], proposal: Any, resolver: Any):
    """Build execution bounds from the accepted manifest and trusted resolver.

    This is not an authority resolver. It only translates already-declared
    manifest limits plus resolver-established institution/domain into the local
    executor policy.
    """
    runtime = load_public_executor_runtime()
    action = next(
        (item for item in manifest.get("actions", []) if item.get("action_id") == proposal.action_id),
        None,
    )
    if not isinstance(action, dict):
        raise PermissionError("manifest action is unavailable")
    tool_name = action.get("tool_name")
    tool = next(
        (item for item in manifest.get("tools", []) if item.get("tool_name") == tool_name),
        None,
    )
    if not isinstance(tool, dict) or tool.get("allowed") is not True:
        raise PermissionError("manifest tool is unavailable or disabled")
    context = resolver.authority_context(proposal.authority_context_ref or "")
    if not isinstance(context, dict):
        raise PermissionError("trusted authority context unavailable")
    institution = context.get("institution") or {}
    institution_id = institution.get("institution_id")
    authority_domain = institution.get("authority_domain")
    if not institution_id or not authority_domain:
        raise PermissionError("trusted institution/domain unavailable")

    targets = (action.get("target_policy") or {}).get("allowed_targets") or []
    if not targets:
        raise PermissionError("manifest target allowlist is empty")
    limits = action.get("effect_limits") or {}
    unit = limits.get("unit")
    max_amount = limits.get("max_amount")
    max_effects = limits.get("max_effects")
    if not isinstance(unit, str) or not unit:
        raise PermissionError("manifest unit bound unavailable")
    if not isinstance(max_amount, (int, float)) or isinstance(max_amount, bool):
        raise PermissionError("manifest amount bound unavailable")
    if type(max_effects) is not int or max_effects < 1:
        raise PermissionError("manifest effect bound unavailable")

    return runtime.LocalExecutionPolicy(
        allowed_institutions=frozenset({institution_id}),
        allowed_authority_domains=frozenset({authority_domain}),
        allowed_adapters=frozenset({tool.get("adapter_id")}),
        allowed_actions=frozenset({proposal.action_id}),
        allowed_target_prefixes=tuple(targets),
        allowed_units=frozenset({unit}),
        max_amount=float(max_amount),
        max_effects=max_effects,
    )
