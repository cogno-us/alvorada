from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from experiments.odex_gax_imx_reference.generation_fence import (
    GenerationFenceClaim,
    GenerationFencePersistenceError,
    GenerationFenceRefusal,
    LineageGenerationFence,
)


def _head(path: Path, conversation_id: str, generation: int) -> None:
    with sqlite3.connect(path, timeout=10, isolation_level=None) as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS heads(
                conversation_id TEXT PRIMARY KEY,
                packet_id TEXT NOT NULL,
                packet_digest TEXT NOT NULL,
                state_version INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            "INSERT OR REPLACE INTO heads VALUES(?,?,?,?)",
            (conversation_id, f"packet-v{generation}", f"digest-v{generation}", generation),
        )
        conn.execute("COMMIT")


class Destination:
    def __init__(self):
        self.effects = []

    def commit(self, snapshot, *, simulate=None):
        self.effects.append((snapshot.effect_id, simulate))
        return {"status": "applied"}


def _snapshot(effect_id: str = "effect-1"):
    return SimpleNamespace(
        effect_id=effect_id,
        payload={"raw": "TOP-SECRET-REFUSED-PAYLOAD"},
    )


def test_v3_claim_is_rejected_after_v4_supersession_with_durable_minimal_refusal(tmp_path):
    path = tmp_path / "exchange.sqlite"
    _head(path, "conversation-1", 3)
    fence = LineageGenerationFence(path)
    destination = Destination()
    claim = GenerationFenceClaim("conversation-1", "effect-1", 3)

    _head(path, "conversation-1", 4)

    with pytest.raises(GenerationFenceRefusal, match="superseded_lineage_generation"):
        fence.wrap_destination(destination, claim).commit(_snapshot())

    assert destination.effects == []
    rows = fence.refusals("conversation-1")
    assert len(rows) == 1
    assert rows[0]["effect_id"] == "effect-1"
    assert rows[0]["expected_generation"] == 3
    assert rows[0]["authoritative_generation"] == 4
    assert rows[0]["reason"] == "superseded_lineage_generation"
    encoded = json.dumps(rows)
    assert "TOP-SECRET-REFUSED-PAYLOAD" not in encoded
    assert "payload" not in encoded


def test_refusal_and_bounded_counters_survive_restart_and_flood(tmp_path):
    path = tmp_path / "exchange.sqlite"
    _head(path, "conversation-1", 4)
    destination = Destination()
    fence = LineageGenerationFence(
        path,
        max_refusals_per_conversation=2,
        counter_max=5,
    )
    claim = GenerationFenceClaim("conversation-1", "effect-1", 3)

    for _ in range(7):
        with pytest.raises(GenerationFenceRefusal):
            fence.wrap_destination(destination, claim).commit(_snapshot())

    restarted = LineageGenerationFence(
        path,
        max_refusals_per_conversation=2,
        counter_max=5,
    )
    assert destination.effects == []
    assert len(restarted.refusals("conversation-1")) == 2
    counters = restarted.counters("conversation-1")
    assert counters["total_refusals"] == 5
    assert counters["persisted_refusals"] == 2
    assert counters["suppressed_refusals"] == 5
    assert counters["last_effect_id"] == "effect-1"


def test_wrong_effect_identity_fails_closed_and_is_attributed(tmp_path):
    path = tmp_path / "exchange.sqlite"
    _head(path, "conversation-1", 3)
    fence = LineageGenerationFence(path)
    destination = Destination()
    claim = GenerationFenceClaim("conversation-1", "effect-expected", 3)

    with pytest.raises(GenerationFenceRefusal, match="effect_identity_mismatch"):
        fence.wrap_destination(destination, claim).commit(_snapshot("effect-other"))

    assert destination.effects == []
    row = fence.refusals("conversation-1")[0]
    assert row["effect_id"] == "effect-expected"
    assert row["reason"] == "effect_identity_mismatch"


def test_concurrent_generation_update_that_wins_lock_is_seen_before_commit(tmp_path):
    path = tmp_path / "exchange.sqlite"
    _head(path, "conversation-1", 3)
    fence = LineageGenerationFence(path)
    destination = Destination()
    claim = GenerationFenceClaim("conversation-1", "effect-1", 3)

    ready = threading.Event()

    def supersede():
        with sqlite3.connect(path, timeout=10, isolation_level=None) as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE heads SET packet_id=?,packet_digest=?,state_version=? WHERE conversation_id=?",
                ("packet-v4", "digest-v4", 4, "conversation-1"),
            )
            ready.set()
            time.sleep(0.05)
            conn.execute("COMMIT")

    thread = threading.Thread(target=supersede)
    thread.start()
    assert ready.wait(2)

    with pytest.raises(GenerationFenceRefusal, match="superseded_lineage_generation"):
        fence.wrap_destination(destination, claim).commit(_snapshot())

    thread.join(timeout=2)
    assert not thread.is_alive()
    assert destination.effects == []
    assert fence.refusals("conversation-1")[-1]["authoritative_generation"] == 4


def test_current_generation_commits_while_serializing_same_store_head_updates(tmp_path):
    path = tmp_path / "exchange.sqlite"
    _head(path, "conversation-1", 3)
    fence = LineageGenerationFence(path)
    destination = Destination()
    claim = GenerationFenceClaim("conversation-1", "effect-1", 3)

    result = fence.wrap_destination(destination, claim).commit(_snapshot())
    assert result["status"] == "applied"
    assert destination.effects == [("effect-1", None)]
    assert fence.refusals("conversation-1") == []


def test_mutation_control_disabled_refusal_writer_cannot_silently_return(tmp_path, monkeypatch):
    path = tmp_path / "exchange.sqlite"
    _head(path, "conversation-1", 4)
    fence = LineageGenerationFence(path)
    destination = Destination()
    claim = GenerationFenceClaim("conversation-1", "effect-1", 3)

    monkeypatch.setattr(fence, "_persist_refusal_locked", lambda *args, **kwargs: None)

    with pytest.raises(GenerationFencePersistenceError, match="generation_refusal_persistence_failed"):
        fence.wrap_destination(destination, claim).commit(_snapshot())

    assert destination.effects == []
    assert fence.refusals("conversation-1") == []
