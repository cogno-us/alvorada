from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

GENERATION_FENCE_PROFILE = "urn:cognous:profiles:gax-lineage-generation-fence"
GENERATION_FENCE_VERSION = "0.1.0"
DEFAULT_MAX_REFUSALS_PER_CONVERSATION = 64
DEFAULT_COUNTER_MAX = 1_000_000


class GenerationFenceRefusal(RuntimeError):
    """The claimed lineage generation is no longer the authoritative local head."""


class GenerationFencePersistenceError(RuntimeError):
    """The fence could not prove durable refusal evidence before returning."""


@dataclass(frozen=True)
class GenerationFenceClaim:
    conversation_id: str
    effect_id: str
    generation: int

    def __post_init__(self) -> None:
        if not self.conversation_id:
            raise ValueError("conversation_id is required")
        if not self.effect_id:
            raise ValueError("effect_id is required")
        if isinstance(self.generation, bool) or self.generation < 0:
            raise ValueError("generation must be a non-negative integer")


class LineageGenerationFence:
    """Optional local commit fence over the accepted GAX/IMX lineage head.

    The authoritative generation is the integer state_version in the existing
    heads table. The fence opens BEGIN IMMEDIATE on the same SQLite database,
    compares the effect claim to that head, and, only while holding that write
    lock, calls the local destination commit. Lineage-head mutations that use
    the same database therefore serialize with the local effect commit.

    This is intentionally not institutional authority and not distributed
    destination atomicity. External writers that bypass this SQLite database
    are outside this bounded reference profile.
    """

    def __init__(
        self,
        store_path: str | Path,
        *,
        max_refusals_per_conversation: int = DEFAULT_MAX_REFUSALS_PER_CONVERSATION,
        counter_max: int = DEFAULT_COUNTER_MAX,
        clock: Callable[[], datetime] | None = None,
    ):
        if max_refusals_per_conversation < 1:
            raise ValueError("max_refusals_per_conversation must be positive")
        if counter_max < max_refusals_per_conversation:
            raise ValueError("counter_max must be >= max_refusals_per_conversation")
        self.path = Path(store_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.max_refusals_per_conversation = int(max_refusals_per_conversation)
        self.counter_max = int(counter_max)
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS generation_refusals(
                    refusal_id TEXT PRIMARY KEY,
                    profile TEXT NOT NULL,
                    profile_version TEXT NOT NULL,
                    conversation_id TEXT NOT NULL,
                    effect_id TEXT NOT NULL,
                    expected_generation INTEGER NOT NULL,
                    authoritative_generation INTEGER,
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS generation_refusals_conversation
                    ON generation_refusals(conversation_id);
                CREATE TABLE IF NOT EXISTS generation_refusal_counters(
                    conversation_id TEXT PRIMARY KEY,
                    total_refusals INTEGER NOT NULL,
                    persisted_refusals INTEGER NOT NULL,
                    suppressed_refusals INTEGER NOT NULL,
                    last_effect_id TEXT NOT NULL,
                    last_expected_generation INTEGER NOT NULL,
                    last_authoritative_generation INTEGER,
                    last_reason TEXT NOT NULL,
                    last_created_at TEXT NOT NULL
                );
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _sat(value: int, maximum: int) -> int:
        return min(maximum, max(0, int(value)))

    def authoritative_generation(self, conversation_id: str) -> int | None:
        with self._connect() as conn:
            try:
                row = conn.execute(
                    "SELECT state_version FROM heads WHERE conversation_id=?",
                    (conversation_id,),
                ).fetchone()
            except sqlite3.OperationalError:
                return None
        return int(row["state_version"]) if row is not None else None

    def _persist_refusal_locked(
        self,
        conn: sqlite3.Connection,
        *,
        claim: GenerationFenceClaim,
        authoritative_generation: int | None,
        reason: str,
    ) -> str | None:
        now = self._clock().astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        counter = conn.execute(
            "SELECT total_refusals,persisted_refusals,suppressed_refusals "
            "FROM generation_refusal_counters WHERE conversation_id=?",
            (claim.conversation_id,),
        ).fetchone()
        total = self._sat((counter["total_refusals"] if counter else 0) + 1, self.counter_max)
        persisted = int(counter["persisted_refusals"] if counter else 0)
        suppressed = int(counter["suppressed_refusals"] if counter else 0)

        refusal_id = None
        if persisted < self.max_refusals_per_conversation:
            refusal_id = "gref-" + uuid.uuid4().hex
            conn.execute(
                """
                INSERT INTO generation_refusals(
                    refusal_id,profile,profile_version,conversation_id,effect_id,
                    expected_generation,authoritative_generation,reason,created_at
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (
                    refusal_id,
                    GENERATION_FENCE_PROFILE,
                    GENERATION_FENCE_VERSION,
                    claim.conversation_id,
                    claim.effect_id,
                    claim.generation,
                    authoritative_generation,
                    reason,
                    now,
                ),
            )
            persisted = self._sat(persisted + 1, self.counter_max)
        else:
            suppressed = self._sat(suppressed + 1, self.counter_max)

        conn.execute(
            """
            INSERT INTO generation_refusal_counters(
                conversation_id,total_refusals,persisted_refusals,suppressed_refusals,
                last_effect_id,last_expected_generation,last_authoritative_generation,
                last_reason,last_created_at
            ) VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(conversation_id) DO UPDATE SET
                total_refusals=excluded.total_refusals,
                persisted_refusals=excluded.persisted_refusals,
                suppressed_refusals=excluded.suppressed_refusals,
                last_effect_id=excluded.last_effect_id,
                last_expected_generation=excluded.last_expected_generation,
                last_authoritative_generation=excluded.last_authoritative_generation,
                last_reason=excluded.last_reason,
                last_created_at=excluded.last_created_at
            """,
            (
                claim.conversation_id,
                total,
                persisted,
                suppressed,
                claim.effect_id,
                claim.generation,
                authoritative_generation,
                reason,
                now,
            ),
        )
        return refusal_id or "aggregated:" + claim.conversation_id

    def _refuse_locked(
        self,
        conn: sqlite3.Connection,
        *,
        claim: GenerationFenceClaim,
        authoritative_generation: int | None,
        reason: str,
    ) -> None:
        marker = self._persist_refusal_locked(
            conn,
            claim=claim,
            authoritative_generation=authoritative_generation,
            reason=reason,
        )
        if not marker:
            conn.execute("ROLLBACK")
            raise GenerationFencePersistenceError("generation_refusal_persistence_failed")

        counter = conn.execute(
            "SELECT total_refusals,last_effect_id,last_reason "
            "FROM generation_refusal_counters WHERE conversation_id=?",
            (claim.conversation_id,),
        ).fetchone()
        if (
            counter is None
            or counter["last_effect_id"] != claim.effect_id
            or counter["last_reason"] != reason
            or int(counter["total_refusals"]) < 1
        ):
            conn.execute("ROLLBACK")
            raise GenerationFencePersistenceError("generation_refusal_persistence_failed")
        conn.execute("COMMIT")
        raise GenerationFenceRefusal(reason)

    def commit_if_current(
        self,
        destination: Any,
        snapshot: Any,
        claim: GenerationFenceClaim,
        *,
        simulate: str | None = None,
    ) -> Any:
        snapshot_effect_id = getattr(snapshot, "effect_id", None)
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                head = conn.execute(
                    "SELECT state_version FROM heads WHERE conversation_id=?",
                    (claim.conversation_id,),
                ).fetchone()
            except sqlite3.OperationalError:
                head = None
            authoritative = int(head["state_version"]) if head is not None else None

            if snapshot_effect_id != claim.effect_id:
                self._refuse_locked(
                    conn,
                    claim=claim,
                    authoritative_generation=authoritative,
                    reason="effect_identity_mismatch",
                )
            if authoritative is None:
                self._refuse_locked(
                    conn,
                    claim=claim,
                    authoritative_generation=None,
                    reason="authoritative_generation_unavailable",
                )
            if authoritative != claim.generation:
                self._refuse_locked(
                    conn,
                    claim=claim,
                    authoritative_generation=authoritative,
                    reason="superseded_lineage_generation",
                )

            try:
                result = destination.commit(snapshot, simulate=simulate)
            except Exception:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
            return result

    def wrap_destination(self, destination: Any, claim: GenerationFenceClaim) -> "_FencedDestination":
        return _FencedDestination(self, destination, claim)

    def refusals(self, conversation_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT refusal_id,profile,profile_version,conversation_id,effect_id,
                       expected_generation,authoritative_generation,reason,created_at
                FROM generation_refusals
                WHERE conversation_id=?
                ORDER BY rowid
                """,
                (conversation_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def counters(self, conversation_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM generation_refusal_counters WHERE conversation_id=?",
                (conversation_id,),
            ).fetchone()
        return dict(row) if row is not None else None


class _FencedDestination:
    def __init__(self, fence: LineageGenerationFence, destination: Any, claim: GenerationFenceClaim):
        self._fence = fence
        self._destination = destination
        self._claim = claim

    def commit(self, snapshot: Any, *, simulate: str | None = None) -> Any:
        return self._fence.commit_if_current(
            self._destination,
            snapshot,
            self._claim,
            simulate=simulate,
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._destination, name)
