from __future__ import annotations

from datetime import datetime
from typing import Any

from . import gax_ref


def build_synthetic_resolver(proposal: Any, *, now: datetime | None = None, active: bool = True):
    """Explicit synthetic authority fixture for examples and tests only."""
    return gax_ref._build_resolver(proposal, now=now, active=active)


def synthetic_refund_policy(operation: Any):
    """Explicit synthetic execution policy for the bounded refund fixture."""
    from engine.producer_contract import LocalExecutionPolicy

    return LocalExecutionPolicy(
        allowed_institutions=frozenset({operation.institution_id}),
        allowed_authority_domains=frozenset({operation.authority_domain}),
        allowed_adapters=frozenset({operation.adapter_id}),
        allowed_actions=frozenset({operation.action_id}),
        allowed_target_prefixes=("urn:cognous:synthetic-account:",),
        allowed_units=frozenset({"USD"}),
        max_amount=1000.0,
        max_effects=1,
    )


def synthetic_observation_policy():
    """Explicit local observation policy for bounded demos/tests only."""
    from agent_control_plane.bounded import ObservationPolicy
    return ObservationPolicy(max_age_seconds=60)


def synthetic_observation_clock():
    from .gax_ref import EVAL, parse_time
    return parse_time(EVAL)
