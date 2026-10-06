from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from . import gax_ref as base


def build_synthetic_resolver(proposal: Any, *, now: Any = None):
    """Explicit synthetic authority fixture for tests and examples only."""
    return base._build_resolver(proposal, now=now)


def run_synthetic_outcome(*, tmp_path: Path, outcome: str) -> dict[str, Any]:
    """Exercise the public GAX/executor path with explicit synthetic authority."""
    from .gax_ref_runtime import (
        EVAL,
        LocalRegistry,
        load_moltbot_runtime,
        make_message,
        parse_time,
        run_exchange,
        runtime_proposal_model,
    )

    manifest_path = os.environ.get("UPSTREAM_MANIFEST_EXAMPLE")
    replay_path = os.environ.get("UPSTREAM_REPLAY_SUCCESS_EXAMPLE")
    if not manifest_path or not replay_path:
        raise RuntimeError("synthetic fixture requires pinned Manifest and Replay paths")
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    bundle = json.loads(Path(replay_path).read_text(encoding="utf-8"))
    proposal = runtime_proposal_model(bundle)
    resolver = build_synthetic_resolver(proposal, now=parse_time(EVAL))
    h = load_moltbot_runtime()
    destination = h.DurableRefundDestination(tmp_path / "moltbot-state")
    message = make_message(bundle, message_id=f"fixture-{outcome}")
    registry = LocalRegistry({"refund-sender"}, {"refund-recipient"}, "refund-recipient")
    store_path = tmp_path / "gax-exchange.sqlite"

    if outcome == "hold":
        grant = resolver.contexts[base.PROFILE_AUTH_CONTEXT]["grant"]
        resolver.statuses[grant["grant_id"]].status = "revoked"
        return run_exchange(
            message, bundle, registry, destination,
            evaluation_time=EVAL, manifest=manifest, store_path=store_path, resolver=resolver,
        )

    mutate = None
    lose_ack = outcome in {"lost_ack", "restart_reconciliation"}
    partial = outcome == "partial"
    if outcome == "denied_after_decision":
        def mutate(current):
            grant = current.contexts[base.PROFILE_AUTH_CONTEXT]["grant"]
            current.statuses[grant["grant_id"]].status = "revoked"
    first = run_exchange(
        message, bundle, registry, destination,
        evaluation_time=EVAL,
        manifest=manifest,
        store_path=store_path,
        resolver=resolver,
        mutate_resolver_after_decision=mutate,
        lose_ack=lose_ack,
        partial_delivery=partial,
    )
    if outcome in {"duplicate_delivery", "restart_reconciliation"}:
        return run_exchange(
            message, bundle, registry, destination,
            evaluation_time=EVAL,
            manifest=manifest,
            store_path=store_path,
            resolver=resolver,
        )
    if outcome in {"success", "denied_after_decision", "lost_ack", "partial"}:
        return first
    raise ValueError(f"unsupported synthetic outcome: {outcome}")
