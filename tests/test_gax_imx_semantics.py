from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

from experiments.odex_gax_imx_reference.gax_ref_runtime import (
    FINAL_REVALIDATION_PASSED,
    OBSERVED_MISMATCH,
    REQUIRED_UNKNOWN,
    SOURCE_CONFLICT,
    LocalRegistry,
    _build_resolver,
    classify_observation_state,
    derive_active_failures,
    load_actual_pinned_moltbot_helpers,
    make_message,
    parse_time,
    run_exchange,
    runtime_proposal_model,
)

EVAL = "2026-08-08T01:00:00Z"


def load_env(name: str) -> dict:
    value = os.environ.get(name)
    assert value, f"missing {name}"
    return json.loads(Path(value).read_text(encoding="utf-8"))


def manifest() -> dict:
    return load_env("UPSTREAM_MANIFEST_EXAMPLE")


def success_bundle() -> dict:
    return load_env("UPSTREAM_REPLAY_SUCCESS_EXAMPLE")


def registry() -> LocalRegistry:
    return LocalRegistry({"refund-sender"}, {"refund-recipient"}, "refund-recipient")


def resolver_for(bundle: dict | None = None):
    proposal = runtime_proposal_model(bundle or success_bundle())
    return _build_resolver(proposal, now=parse_time(EVAL))


def destination_at(tmp_path: Path):
    return load_actual_pinned_moltbot_helpers().DurableRefundDestination(tmp_path / "moltbot-state")


def effect_count(destination) -> int:
    with sqlite3.connect(destination.path) as conn:
        return conn.execute("SELECT COUNT(*) FROM effects").fetchone()[0]


def run_current(tmp_path: Path, *, message_id: str = "m-semantic", resolver=None, destination=None, **kwargs):
    bundle = success_bundle()
    msg = make_message(bundle, message_id=message_id)
    dest = destination or destination_at(tmp_path)
    result = run_exchange(
        msg,
        bundle,
        registry(),
        dest,
        evaluation_time=EVAL,
        manifest=manifest(),
        store_path=tmp_path / "exchange.sqlite",
        resolver=resolver or resolver_for(bundle),
        **kwargs,
    )
    return msg, dest, result


def test_success_records_explicit_final_revalidation_event(tmp_path):
    _, _, result = run_current(tmp_path, message_id="m-final-revalidation")
    assert FINAL_REVALIDATION_PASSED in result["execution"]["lifecycle_events"]
    assert FINAL_REVALIDATION_PASSED in result["successor_packet"]["lifecycle_events"]
    assert result["execution"]["post_effect_state"] == "COMPLETE_UNVERIFIED"


def test_observed_mismatch_is_distinct_from_complete_unverified():
    authorized = {"target": "acct-a", "amount": 10.0, "unit": "USD", "payload": {"reason": "ok"}}
    observed_match = {"target": "acct-a", "amount": 10.0, "unit": "USD", "payload": {"reason": "ok"}}
    observed_mismatch = {"target": "acct-b", "amount": 10.0, "unit": "USD", "payload": {"reason": "ok"}}
    assert classify_observation_state(authorized=authorized, observed=observed_match, verification_available=False) == "COMPLETE_UNVERIFIED"
    assert classify_observation_state(authorized=authorized, observed=observed_mismatch, verification_available=False) == OBSERVED_MISMATCH


def test_active_failures_preserve_epistemic_set_separate_from_consequence():
    failures = derive_active_failures({"required_unknowns": ["identity"], "source_conflicts": ["policy"], "consequence": "HOLD"})
    assert REQUIRED_UNKNOWN in failures
    assert SOURCE_CONFLICT in failures


def test_required_unknown_hold_then_new_evidence_revalidates_and_continues(tmp_path):
    bundle = success_bundle()
    msg1 = make_message(bundle, message_id="m-required-unknown")
    dest = destination_at(tmp_path)
    unknown_resolver = resolver_for(bundle)
    obligation_id = next(iter(unknown_resolver.evidence))
    unknown_resolver.evidence[obligation_id].state = "unknown"
    held = run_exchange(
        msg1,
        bundle,
        registry(),
        dest,
        evaluation_time=EVAL,
        manifest=manifest(),
        store_path=tmp_path / "exchange.sqlite",
        resolver=unknown_resolver,
    )
    assert held["execution"]["attempted"] is False
    assert held["execution"]["attempt_status"] in {"hold", "held"}
    assert effect_count(dest) == 0

    msg2 = make_message(bundle, message_id="m-required-known")
    continued = run_exchange(
        msg2,
        bundle,
        registry(),
        dest,
        evaluation_time="2026-08-08T01:01:00Z",
        manifest=manifest(),
        store_path=tmp_path / "exchange.sqlite",
        resolver=resolver_for(bundle),
    )
    assert continued["execution"]["newly_executed"] is True
    assert continued["execution"]["attempt_status"] == "executed"
    assert FINAL_REVALIDATION_PASSED in continued["execution"]["lifecycle_events"]
    assert effect_count(dest) == 1


def test_partial_execution_then_revocation_does_not_grant_standing_permission(tmp_path):
    bundle = success_bundle()
    msg, dest, first = run_current(tmp_path, message_id="m-partial-revoked", partial_delivery=True)
    assert first["execution"]["destination_observed"] == "partial"
    assert effect_count(dest) == 1

    revoked = resolver_for(bundle)
    grant_id = next(iter(revoked.statuses))
    revoked.statuses[grant_id].status = "revoked"
    redelivery = run_exchange(
        msg,
        bundle,
        registry(),
        dest,
        evaluation_time="2026-08-08T01:02:00Z",
        manifest=manifest(),
        store_path=tmp_path / "exchange.sqlite",
        resolver=revoked,
    )
    assert redelivery["execution"]["newly_executed"] is False
    assert redelivery["execution"]["effect_id"] == first["execution"]["effect_id"]
    assert redelivery["successor_packet"]["pending_effects"] == [first["execution"]["effect_id"]]
    assert redelivery["successor_packet"]["unresolved_delivery"] is True
    assert effect_count(dest) == 1
