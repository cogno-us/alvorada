"""Experimental ODEX-GAX / ODEX-IMX refund exchange reference.

Import-time normalization keeps dispatch checkpoint recovery aligned with
retained executor result semantics. A durable checkpoint has its own state
(bound/dispatched), but redelivery responses must derive execution status from
retained lifecycle evidence, not from the checkpoint row state.
"""

from __future__ import annotations

from typing import Any

from . import gax_ref_runtime as _runtime


def _checkpoint_aware_association_payload(self: Any, row: Any) -> dict[str, Any]:
    result = _runtime._json_loads(row["result_json"])
    keys = row.keys()
    status = row["status"] if "status" in keys else ((result or {}).get("status") or row["state"])
    payload = {
        "status": status,
        "proposal_commitment": row["proposal_commitment"],
        "decision_id": row["decision_id"],
        "effect_id": row["effect_id"],
        "attempt_id": row["attempt_id"],
        "proposal": _runtime._json_loads(row["proposal_json"]),
        "decision": _runtime._json_loads(row["decision_json"]),
        "request": _runtime._json_loads(row["request_json"]),
        "result": result,
        "cp_record": _runtime._json_loads(row["cp_record_json"]),
        "moltbot": _runtime._json_loads(row["moltbot_json"]),
    }
    if "state" in keys:
        payload["state"] = row["state"]
    return payload


def _recover_checkpoint_with_result_status(
    message: dict[str, Any],
    assessment: dict[str, Any],
    manifest: dict[str, Any],
    store: Any,
    checkpoint: dict[str, Any],
    destination: Any | None,
) -> dict[str, Any]:
    problem = _runtime._validate_bound_association(checkpoint)
    if problem:
        assessment["errors"].append("GAX-EXECUTION-CHECKPOINT-INTEGRITY-FAILED")
        return {
            "assessment": assessment,
            "execution": {"attempted": False, "reason": problem, "attempt_status": "hold"},
            "successor_packet": None,
        }

    if checkpoint.get("moltbot") and checkpoint.get("result"):
        artifacts = _runtime._artifacts_from_association(manifest, message, checkpoint)
        result_status = (checkpoint.get("result") or {}).get("status")
        workflow_status = result_status or checkpoint.get("status") or "executed"
        response_status = workflow_status
        if response_status in {"executed", "dispatched"}:
            response_status = "reconciled"
        store.record_workflow(
            message,
            {
                "proposal_record": checkpoint["proposal"],
                "decision": checkpoint["decision"],
                "request": checkpoint.get("request"),
                "result": checkpoint.get("result"),
                "status": workflow_status,
                "cp_record": checkpoint["cp_record"],
                "moltbot_record": checkpoint.get("moltbot"),
            },
        )
        result = _runtime.Obj({**checkpoint["result"], "status": response_status, "newly_executed": False})
        decision = _runtime.Obj({"decision_id": checkpoint.get("decision_id"), "effect_id": checkpoint.get("effect_id")})
        return _runtime._execution_response(
            assessment,
            response_status,
            result,
            decision,
            artifacts,
            attempted_override=False,
            newly_executed_override=False,
        )

    if destination is not None:
        rows = [row for row in _runtime._effect_rows(destination) if row.get("effect_id") == checkpoint.get("effect_id")]
        if rows:
            assessment["errors"].append("GAX-EXECUTION-CHECKPOINT-DESTINATION-OBSERVED-WITHOUT-RETAINED-ATTEMPT")
            return {
                "assessment": assessment,
                "execution": {
                    "attempted": False,
                    "reason": "destination_observed_without_replayable_attempt",
                    "attempt_status": "hold",
                    "effect_id": checkpoint.get("effect_id"),
                    "decision_id": checkpoint.get("decision_id"),
                },
                "successor_packet": None,
            }
    assessment["errors"].append("GAX-EXECUTION-CHECKPOINT-UNRESOLVED")
    return {
        "assessment": assessment,
        "execution": {
            "attempted": False,
            "reason": "checkpoint_without_dispatch_evidence",
            "attempt_status": "hold",
            "effect_id": checkpoint.get("effect_id"),
            "decision_id": checkpoint.get("decision_id"),
        },
        "successor_packet": None,
    }


_runtime.TransactionalExchangeStore._association_payload = _checkpoint_aware_association_payload
_runtime._recover_checkpoint = _recover_checkpoint_with_result_status
