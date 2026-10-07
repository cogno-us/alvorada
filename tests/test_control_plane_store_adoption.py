from __future__ import annotations

from pathlib import Path

import pytest

from experiments.odex_gax_imx_reference.gax_ref_runtime import (
    CP_REVISION,
    MOLTBOT_PRODUCER_PROFILE_VERSION,
    MOLTBOT_REVISION,
    REPLAY_REVISION,
    load_executor_runtime,
)


ACCEPTED_CONTROL_PLANE_PERSISTENCE = "248d899634d9db3518e831bc7ab568a48733f825"
ACCEPTED_REPLAY_COMPATIBILITY = "043830b56595cecddfa65c064afd1c0b95e64792"
ACCEPTED_EXECUTOR = "177354e959cc78c59c1a776f018cfbfbf28c927b"


def test_stage_level_selected_runtime_revisions_are_exact():
    """Stage-level evidence only: selected local producer revisions, not full GAX qualification."""
    assert CP_REVISION == ACCEPTED_CONTROL_PLANE_PERSISTENCE
    assert REPLAY_REVISION == ACCEPTED_REPLAY_COMPATIBILITY
    assert MOLTBOT_REVISION == ACCEPTED_EXECUTOR
    assert MOLTBOT_PRODUCER_PROFILE_VERSION == "2.0.0"


def test_stage_level_repaired_store_constructs_and_persists_across_restart(tmp_path: Path):
    """Stage-level evidence only: repaired same-host Control Plane record persistence."""
    runtime = load_executor_runtime()
    cp = runtime["cp"]
    path = tmp_path / "control-plane-record.json"

    store = cp.BoundedRecordStore(path, "run-store-adoption")
    decision = cp.RuntimeDecision(
        decision_id="decision-store-adoption",
        effect_id="effect-store-adoption",
        result="hold",
        reasons=["stage-level-persistence-evidence"],
        decided_at="2026-10-07T06:00:00Z",
        binding=None,
    )
    attempt = cp.EffectAttempt(
        attempt_id="cp-attempt-store-adoption",
        effect_id=decision.effect_id,
        decision_id=decision.decision_id,
        started_at="2026-10-07T06:00:01Z",
        status="unknown",
        acknowledgement={},
    )
    store.append_decision(decision)
    store.append_attempt(attempt)

    assert path.exists()
    assert path.with_name(path.name + ".lock").exists()

    restarted = cp.BoundedRecordStore(path, "run-store-adoption")
    record = restarted.load()
    assert record.run_id == "run-store-adoption"
    assert [item.decision_id for item in record.decisions] == ["decision-store-adoption"]
    assert [item.effect_id for item in record.decisions] == ["effect-store-adoption"]
    assert [item.attempt_id for item in record.attempts] == ["cp-attempt-store-adoption"]
    assert [item.effect_id for item in record.attempts] == ["effect-store-adoption"]
    assert [item.decision_id for item in record.attempts] == ["decision-store-adoption"]


def test_stage_level_replay_accepts_and_attributes_actual_persistence_revision(tmp_path: Path):
    """Stage-level evidence only: Replay decoder selection and source attribution."""
    runtime = load_executor_runtime()
    cp = runtime["cp"]
    path = tmp_path / "control-plane-replay.json"
    store = cp.BoundedRecordStore(path, "run-replay-adoption")
    store.append_decision(
        cp.RuntimeDecision(
            decision_id="decision-replay-adoption",
            effect_id="effect-replay-adoption",
            result="hold",
            reasons=["stage-level-replay-evidence"],
            decided_at="2026-10-07T06:01:00Z",
            binding=None,
        )
    )
    source = store.load().model_dump(mode="json")

    from agent_replay_bundle.importers import import_bounded_workflow

    bundle = import_bounded_workflow(
        source,
        proposal=None,
        moltbot_export=None,
        control_plane_revision=CP_REVISION,
    )
    exported = bundle.model_dump(mode="json")
    assert exported["metadata"]["control_plane_revision"] == CP_REVISION

    cp_profiles = [
        item for item in exported["producer_profiles"]
        if item["repository"] == "cogno-us/cognous-agent-control-plane"
    ]
    assert len(cp_profiles) == 1
    assert cp_profiles[0]["revision"] == CP_REVISION
    assert exported["import_reports"][0]["source_revision"] == CP_REVISION


def test_stage_level_replay_rejects_unsupported_control_plane_revision(tmp_path: Path):
    """Stage-level negative evidence: shape similarity cannot select an unsupported revision."""
    runtime = load_executor_runtime()
    cp = runtime["cp"]
    store = cp.BoundedRecordStore(tmp_path / "unsupported-revision.json", "run-unsupported")
    source = store.load().model_dump(mode="json")

    from agent_replay_bundle.importers import ImportContractError, import_bounded_workflow

    with pytest.raises(ImportContractError, match="unsupported Control Plane revision"):
        import_bounded_workflow(
            source,
            proposal=None,
            moltbot_export=None,
            control_plane_revision="0" * 40,
        )
