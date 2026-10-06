from __future__ import annotations

from typing import Any

from . import gax_ref_runtime as runtime

FINAL_REVALIDATION_PASSED = "FINAL_REVALIDATION_PASSED"
COMPLETE_UNVERIFIED = "COMPLETE_UNVERIFIED"
OBSERVED_MISMATCH = "OBSERVED_MISMATCH"
OBSERVED_MATCH_UNVERIFIED = "OBSERVED_MATCH_UNVERIFIED"
REQUIRED_UNKNOWN = "REQUIRED_UNKNOWN"
SOURCE_CONFLICT = "SOURCE_CONFLICT"
PARTIAL_DELIVERY = "PARTIAL_DELIVERY"
ACKNOWLEDGEMENT_UNKNOWN = "ACKNOWLEDGEMENT_UNKNOWN"

_original_pipeline = runtime._pipeline
_original_artifact_facts = runtime._artifact_facts
_original_execution_response = runtime._execution_response


def classify_observation_state(*, authorized: dict[str, Any] | None, observed: dict[str, Any] | None, verification_available: bool = False) -> str:
    """Classify post-effect epistemic state without claiming external verification."""
    if not observed:
        return "NO_OBSERVATION"
    if authorized:
        keys = ("target", "amount", "unit", "payload")
        for key in keys:
            if key in authorized and key in observed and authorized[key] != observed[key]:
                return OBSERVED_MISMATCH
    if verification_available:
        return "COMPLETE_VERIFIED"
    return COMPLETE_UNVERIFIED


def derive_active_failures(facts: dict[str, Any]) -> list[str]:
    """Preserve epistemic conditions separately from the derived consequence."""
    failures: list[str] = []
    required_unknowns = facts.get("required_unknowns") or facts.get("required_unknown") or facts.get("unknown_required_conditions")
    if required_unknowns:
        failures.append(REQUIRED_UNKNOWN)
    conflicts = facts.get("source_conflicts") or facts.get("source_conflict") or facts.get("critical_source_conflicts")
    if conflicts:
        failures.append(SOURCE_CONFLICT)
    if facts.get("destination_observed") == "partial":
        failures.append(PARTIAL_DELIVERY)
    if facts.get("acknowledgement_summary") == "unknown":
        failures.append(ACKNOWLEDGEMENT_UNKNOWN)
    return failures


def _augment_facts(facts: dict[str, Any]) -> dict[str, Any]:
    out = dict(facts)
    lifecycle_events = list(out.get("lifecycle_events") or [])
    has_effect = bool(out.get("destination_effects") or out.get("effect_id"))
    if has_effect and FINAL_REVALIDATION_PASSED not in lifecycle_events:
        lifecycle_events.append(FINAL_REVALIDATION_PASSED)
    out["lifecycle_events"] = lifecycle_events
    out["active_failures"] = derive_active_failures(out)
    if "post_effect_state" not in out:
        observed = None
        effects = out.get("destination_effects") or []
        if effects and isinstance(effects[0], dict):
            observed = effects[0]
        out["post_effect_state"] = classify_observation_state(authorized=None, observed=observed, verification_available=False) if observed else "NO_OBSERVATION"
    return out


def artifact_facts_with_semantics(odes_reference: dict[str, Any]) -> dict[str, Any]:
    return _augment_facts(_original_artifact_facts(odes_reference))


def pipeline_with_semantics(*args: Any, **kwargs: Any) -> dict[str, Any]:
    artifacts = _original_pipeline(*args, **kwargs)
    facts = _augment_facts(artifacts.get("execution_facts", {}))
    artifacts["execution_facts"] = facts
    successor = artifacts.get("successor_packet")
    if successor is not None:
        successor["pending_effects"] = facts.get("pending_effects", [])
        successor["unresolved_delivery"] = facts.get("unresolved_delivery")
        successor["active_failures"] = facts.get("active_failures", [])
        successor["lifecycle_events"] = facts.get("lifecycle_events", [])
        successor["post_effect_state"] = facts.get("post_effect_state")
    return artifacts


def execution_response_with_semantics(assessment: dict[str, Any], status: str, result: Any, decision: Any, artifacts: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    artifacts = dict(artifacts)
    artifacts["execution_facts"] = _augment_facts(artifacts.get("execution_facts", {}))
    response = _original_execution_response(assessment, status, result, decision, artifacts, **kwargs)
    response["execution"]["active_failures"] = artifacts["execution_facts"].get("active_failures", [])
    response["execution"]["lifecycle_events"] = artifacts["execution_facts"].get("lifecycle_events", [])
    response["execution"]["post_effect_state"] = artifacts["execution_facts"].get("post_effect_state")
    return response


runtime._artifact_facts = artifact_facts_with_semantics
runtime._pipeline = pipeline_with_semantics
runtime._execution_response = execution_response_with_semantics
runtime.classify_observation_state = classify_observation_state
runtime.derive_active_failures = derive_active_failures
runtime.FINAL_REVALIDATION_PASSED = FINAL_REVALIDATION_PASSED
runtime.COMPLETE_UNVERIFIED = COMPLETE_UNVERIFIED
runtime.OBSERVED_MISMATCH = OBSERVED_MISMATCH
runtime.REQUIRED_UNKNOWN = REQUIRED_UNKNOWN
runtime.SOURCE_CONFLICT = SOURCE_CONFLICT
