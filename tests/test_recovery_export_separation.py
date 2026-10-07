"""Recovery export regression: current denial stays separate from historical effect evidence."""
from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from test_gax_public_runtime_artifacts import _accepted_transport, EVAL, effect_count, run_once
from experiments.odex_gax_imx_reference.gax_ref_runtime import (
    TransactionalExchangeStore,
    digest,
    import_replay_bundle,
)


AUTHORITY_CHANGES = ("revocation", "expiry", "policy", "evidence", "approval")


def _cp_snapshot(root: Path) -> dict[str, bytes]:
    return {p.name: p.read_bytes() for p in sorted(root.glob("control-plane-*.json"))}


def _replace_first(mapping, **updates):
    key = next(iter(mapping))
    mapping[key] = mapping[key].model_copy(update=updates)


def _change_authority(resolver, change: str):
    if change == "revocation":
        _replace_first(resolver.statuses, status="revoked")
        return EVAL
    if change == "policy":
        _replace_first(resolver.policies, version="changed-policy-version")
        return EVAL
    if change == "evidence":
        stale = (datetime.fromisoformat(EVAL.replace("Z", "+00:00")) - timedelta(seconds=301)).isoformat()
        _replace_first(resolver.evidence, observed_at=stale)
        return EVAL
    if change == "approval":
        _replace_first(resolver.approvals, status="revoked")
        return EVAL
    if change == "expiry":
        recovery = "2026-08-09T00:00:00Z"
        for group in (
            resolver.statuses,
            resolver.identities,
            resolver.mandates,
            resolver.approvals,
            resolver.policies,
            resolver.conflicts,
            resolver.evidence,
        ):
            for key, value in list(group.items()):
                group[key] = value.model_copy(update={"observed_at": recovery})
        return recovery
    raise AssertionError(change)


@pytest.mark.parametrize("authority_change", AUTHORITY_CHANGES)
@pytest.mark.parametrize("effect_state", ("applied", "absent"))
def test_recovery_denial_is_separate_from_historical_execution(
    tmp_path, monkeypatch, authority_change, effect_state
):
    """Changed authority denies recovery without rewriting historical execution evidence."""
    options = {"lose_ack": True} if effect_state == "applied" else None
    transport, destination, message, handler = _accepted_transport(
        tmp_path, f"recovery-separation-{effect_state}-{authority_change}",
        exchange_options=options,
    )

    if effect_state == "absent":
        original_commit = destination.commit

        def interrupted_before_commit(snapshot, *, simulate=None):
            raise TimeoutError("synthetic interruption before destination commit")

        monkeypatch.setattr(destination, "commit", interrupted_before_commit)
        transport.deliver(message["message_id"], now=EVAL)
        monkeypatch.setattr(destination, "commit", original_commit)
    else:
        transport.deliver(message["message_id"], now=EVAL)

    original = transport.retained_artifacts(message["message_id"])["artifact_export"]
    assert original is not None
    effect_id = original["producer_refs"]["effect_id"]
    original_digest = digest(original)
    original_reconstruction_digest = original["producer_refs"]["reconstruction_digest"]
    before_count = effect_count(destination)
    before_cp = _cp_snapshot(tmp_path)

    recovery_time = _change_authority(handler.resolver, authority_change)

    observed = []
    original_observe = destination.observe_bound

    def count_observe(snapshot):
        observed.append(snapshot.effect_id)
        return original_observe(snapshot)

    monkeypatch.setattr(destination, "observe_bound", count_observe)
    recovered = handler.resume_original(message, delivery_time=recovery_time)

    assert recovered["execution"]["attempt_status"] == "denied"
    assert recovered["execution"]["attempted"] is False
    assert recovered["execution"]["newly_executed"] is False
    assert recovered["execution"]["effect_id"] == effect_id
    assert recovered["execution"]["decision_id"] == original["producer_refs"]["decision_id"]
    assert not recovered["execution"]["observation"]
    assert not recovered["execution"]["control_plane_evidence"]
    assert observed == []
    assert effect_count(destination) == before_count
    assert _cp_snapshot(tmp_path) == before_cp

    derivative = recovered["artifact_export"]
    assert derivative["state"] == "recovery_denied_derivative"
    assert derivative["lineage"]["relationship"] == "recovery_denial_derivative"
    assert derivative["lineage"]["source_artifact_commitment"] == original_digest
    assert derivative["lineage"]["source_checkpoint_commitment"]
    assert derivative["lineage"]["recovery_evaluated_at"] == recovery_time
    assert derivative["lineage"]["recovery_scope"] == (
        "current_authority_revalidation_before_destination_observation"
    )
    assert derivative["lineage"]["recovery_status"] == "denied"
    assert derivative["lineage"]["recovery_reason"]
    current = derivative["lineage"]["recovery_result"]
    assert current["status"] == "denied"
    assert current["decision_id"] == original["producer_refs"]["decision_id"]
    assert current["effect_id"] == effect_id
    assert current["error"] == derivative["lineage"]["recovery_reason"]
    assert not current["observation"]
    assert not current["control_plane_evidence"]
    assert derivative["lineage"]["destination_observation_performed"] is False
    assert derivative["lineage"]["replacement_dispatch_performed"] is False
    assert derivative["lineage"]["renewed_authorization"] is False
    assert derivative["lineage"]["effect_reexecution"] is False

    # Historical execution/effect evidence is copied verbatim into the derivative.
    assert derivative["reconstruction_bundle"] == original["reconstruction_bundle"]
    assert derivative["producer_refs"]["reconstruction_digest"] == original_reconstruction_digest
    assert derivative["producer_refs"]["effect_id"] == effect_id
    assert recovered["execution_facts"] == (
        original["odes"]["odes_package"]["provenance"]["execution_facts"]
    )

    # Original transport-retained evidence is immutable.
    assert transport.retained_artifacts(message["message_id"])["artifact_export"] == original
    assert digest(transport.retained_artifacts(message["message_id"])["artifact_export"]) == original_digest

    facts = recovered["execution_facts"]
    if effect_state == "applied":
        assert before_count == 1
        assert facts["destination_observed"] == "applied"
        assert facts["pending_effects"] == []
        assert facts["unresolved_delivery"] is False
        assert any(
            a["status"] == "unknown" and not a["acknowledgement"]
            for a in facts["control_plane_attempt_transitions"]
        )
    else:
        assert before_count == 0
        assert facts["pending_effects"] == [effect_id]
        assert facts["unresolved_delivery"] is True
        assert not any(r.get("retry_eligible") for r in facts.get("reconciliations", []))


def test_valid_authority_prior_absence_retains_fresh_reconciliation(
    tmp_path, monkeypatch
):
    """Observed absence is current recovery evidence and must not be collapsed."""
    transport, destination, message, handler = _accepted_transport(
        tmp_path, "observed-absence-recovery"
    )

    original_commit = destination.commit

    def interrupted_before_commit(snapshot, *, simulate=None):
        raise TimeoutError("synthetic interruption before destination commit")

    monkeypatch.setattr(destination, "commit", interrupted_before_commit)
    transport.deliver(message["message_id"], now=EVAL)
    monkeypatch.setattr(destination, "commit", original_commit)

    original = transport.retained_artifacts(message["message_id"])["artifact_export"]
    effect_id = original["producer_refs"]["effect_id"]
    original_digest = digest(original)
    assert effect_count(destination) == 0

    observations = []
    dispatches = []
    original_observe = destination.observe_bound

    def count_observe(snapshot):
        observations.append(snapshot.effect_id)
        return original_observe(snapshot)

    def count_commit(snapshot, *, simulate=None):
        dispatches.append(snapshot.effect_id)
        return original_commit(snapshot, simulate=simulate)

    monkeypatch.setattr(destination, "observe_bound", count_observe)
    monkeypatch.setattr(destination, "commit", count_commit)

    recovered = handler.resume_original(message, delivery_time=EVAL)

    assert observations == [effect_id]
    assert dispatches == []
    assert recovered["execution"]["attempt_status"] == "denied"
    assert recovered["execution"]["newly_executed"] is False
    assert recovered["execution"]["control_plane_evidence"]["reconciliation"]["result"] == "observed_absent"
    assert recovered["artifact_export"]["state"] == "reconciled_derivative"
    assert recovered["artifact_export"]["lineage"]["relationship"] == (
        "observation_reconciliation_derivative"
    )

    facts = recovered["execution_facts"]
    assert facts["reconciliations"][-1]["result"] == "observed_absent"
    assert facts["reconciliations"][-1]["retry_eligible"] is False
    assert facts["pending_effects"] == [effect_id]
    assert facts["unresolved_delivery"] is True
    assert effect_count(destination) == 0

    # Recovery evidence is derivative; the original retained transport artifact is immutable.
    assert transport.retained_artifacts(message["message_id"])["artifact_export"] == original
    assert digest(transport.retained_artifacts(message["message_id"])["artifact_export"]) == original_digest


def test_replay_still_rejects_denied_execution_with_effect_evidence(tmp_path):
    """The repair must not weaken Replay's denied/effect contradiction guard."""
    _, _, _, _, _, message, _ = run_once(tmp_path, "malformed-denied-effect")
    association = TransactionalExchangeStore(
        tmp_path / "exchange.sqlite"
    ).workflow_for_message(message)
    assert association is not None
    malformed = copy.deepcopy(association["moltbot"])
    assert malformed["effects"]
    malformed["execution_result"]["status"] = "denied"

    from agent_replay_bundle.importers import ImportContractError

    with pytest.raises(
        ImportContractError,
        match="denied execution result contradicts retained effect evidence",
    ):
        import_replay_bundle(
            association["cp_record"],
            association["proposal"],
            malformed,
        )
