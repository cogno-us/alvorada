from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from . import gax_ref as base

DestinationState = base.DestinationState
LocalRegistry = base.LocalRegistry
TransactionalExchangeStore = base.TransactionalExchangeStore
DurableExchangeStore = base.DurableExchangeStore
_build_resolver = base._build_resolver
assess_message = base.assess_message
digest = base.digest
execution_facts = base.execution_facts
export_odes_reference = base.export_odes_reference
load_successor_packet = base.load_successor_packet
make_message = base.make_message
make_successor_packet = base.make_successor_packet
parse_time = base.parse_time
runtime_proposal_model = base.runtime_proposal_model


def run_exchange(
    message: dict[str, Any],
    bundle: dict[str, Any],
    registry: LocalRegistry,
    destination: DestinationState,
    *,
    evaluation_time: str,
    manifest: dict[str, Any],
    store_path: str | Path,
    resolver: Any | None = None,
    mutate_resolver_after_decision=None,
    lose_ack: bool = False,
    partial_delivery: bool = False,
) -> dict[str, Any]:
    """Execute the bounded exchange while preserving durable duplicate-effect recovery."""
    if resolver is None:
        raise ValueError("trusted resolver is required; incoming proposals never construct authority")

    store = TransactionalExchangeStore(store_path)
    _, err = store.record_message(message)
    if err:
        return {
            "assessment": {"permitted_handling": "REFUSE", "errors": [err], "stages": {"identity_binding": "failed"}},
            "execution": {"attempted": False, "reason": err},
        }

    assessment = assess_message(message, bundle, registry, evaluation_time=evaluation_time, seen_messages=store.seen_messages())
    if assessment["permitted_handling"] != "ACCEPT_FOR_ASSESSMENT":
        return {"assessment": assessment, "execution": {"attempted": False, "reason": "message_not_execution_eligible"}}

    proposal = runtime_proposal_model(bundle)
    op_hash = base.operation_commitment(model=proposal)
    cp = base._cp()
    record_store = cp["BoundedRecordStore"]((Path(store_path).parent / "control_plane_run.json"), proposal.run_id or "run-gax-imx")
    flow = cp["BoundedAuthorizationWorkflow"](manifest=manifest, resolver=resolver, destination=destination.adapter, records=record_store)
    decision = flow.decide(proposal, now=parse_time(evaluation_time))

    if decision.result != "authorized":
        assessment["stages"]["authority"] = decision.result
        return {
            "assessment": assessment,
            "execution": {"attempted": False, "reason": ";".join(decision.reasons), "decision_id": decision.decision_id},
            "current_reconstruction_bundle": base._reconstruction_from_run(manifest, proposal, decision, record_store.load(), destination, original_bundle=bundle),
        }

    if mutate_resolver_after_decision:
        mutate_resolver_after_decision(resolver)

    same_effect, err = store.bind_effect(decision.effect_id, op_hash)
    if err:
        assessment["errors"].append(err)
        return {"assessment": assessment, "execution": {"attempted": False, "reason": err, "effect_id": decision.effect_id}}

    try:
        attempt, observed = flow.execute(
            proposal,
            decision,
            adapter_id=proposal.adapter_id,
            now=parse_time(evaluation_time),
            lose_ack=lose_ack,
            partial=partial_delivery,
        )
    except Exception as exc:
        snapshot = destination.effects
        if snapshot:
            effect = next(iter(snapshot.values()))
            assessment["stages"]["authority"] = "authorized"
            return {
                "assessment": assessment,
                "execution": {
                    "attempted": True,
                    "attempt_status": "reconciled_existing",
                    "newly_executed": False,
                    "destination_observed": effect.get("state", "unknown"),
                    "effect_id": effect["effect_id"],
                    "decision_id": decision.decision_id,
                    "attempt_id": None,
                    "reconciliation_reason": str(exc),
                },
                "current_reconstruction_bundle": base._reconstruction_from_run(manifest, proposal, decision, record_store.load(), destination, original_bundle=bundle),
            }
        return {"assessment": assessment, "execution": {"attempted": False, "reason": str(exc), "effect_id": decision.effect_id}}

    current = base._reconstruction_from_run(manifest, proposal, decision, record_store.load(), destination, original_bundle=bundle)
    replay = base._run_replay(current)
    odes = export_odes_reference(manifest, current)
    facts = execution_facts(current)
    successor = make_successor_packet(message, facts=facts, state_version=1, current_bundle=current)
    assessment["stages"]["authority"] = "authorized"
    return {
        "assessment": assessment,
        "execution": {
            "attempted": True,
            "attempt_status": attempt.status,
            "newly_executed": not same_effect and not attempt.acknowledgement.get("duplicate", False),
            "destination_observed": observed.state,
            "effect_id": decision.effect_id,
            "decision_id": decision.decision_id,
            "attempt_id": attempt.attempt_id,
        },
        "current_reconstruction_bundle": current,
        "replay_validation": replay,
        "odes_reference": odes,
        "successor_packet": successor,
    }


def run_demo(manifest_path: str, replay_path: str, out_path: str) -> dict[str, Any]:
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    bundle = json.loads(Path(replay_path).read_text(encoding="utf-8"))
    tmp = Path(out_path).parent
    dest = DestinationState(tmp / "demo_destination.sqlite")
    proposal = runtime_proposal_model(bundle)
    resolver = _build_resolver(proposal, now=parse_time(base.EVAL))
    result = run_exchange(
        make_message(bundle),
        bundle,
        LocalRegistry({"refund-sender"}, {"refund-recipient"}, "refund-recipient"),
        dest,
        evaluation_time=base.EVAL,
        manifest=manifest,
        store_path=tmp / "demo_exchange.sqlite",
        resolver=resolver,
    )
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
