from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

TRANSPORT_PROFILE = "urn:cognous:profiles:governed-message-transport:0.1.0"
TRANSPORT_VERSION = "0.1.0"
RECIPIENT_RESULT_PROFILE = "urn:cognous:profiles:governed-message-recipient-result"
RECIPIENT_RESULT_VERSION = "1.0.0"
CANONICALIZATION_VERSION = "json-sort-keys-compact-v1"
ACK_KIND_DURABLE_RECEIPT = "DURABLE_RECEIPT"
ACK_KIND_DURABLE_RECEIPT_UNRESOLVED = "DURABLE_RECEIPT_UNRESOLVED"
ACK_KIND_TERMINAL_REJECTION = "TERMINAL_REJECTION"
EXECUTION_ELIGIBLE_TYPES = {"PROPOSE", "REQUEST"}
INFORMATIONAL_TYPES = {"REPORT", "REFUSE", "NOT_UNDERSTOOD"}


class SyntheticTransportInterruption(RuntimeError):
    pass


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def commitment(value: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_bytes(value)).hexdigest()


def parse_time(value: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError("timestamp is required")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def iso(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class DeliveryClockPolicy:
    """Trusted local clock policy for transport and recipient assessment.

    The caller supplies the trusted delivery instant. Timestamps embedded in a
    message or envelope never advance that clock. Zero future skew is the
    conservative local-reference default.
    """

    max_future_skew_seconds: int = 0

    def validate_now(self, now: str) -> datetime:
        return parse_time(now)

    def validate_temporal_binding(
        self,
        *,
        envelope: dict[str, Any],
        message: dict[str, Any],
        now: str,
    ) -> datetime:
        trusted_now = self.validate_now(now)
        envelope_created = parse_time(envelope.get("created_at"))
        envelope_expires = parse_time(envelope.get("expires_at"))
        message_created = parse_time(message.get("created_at"))
        message_expires = parse_time(message.get("expires_at"))

        skew = timedelta(seconds=self.max_future_skew_seconds)
        if envelope_created > trusted_now + skew:
            raise ValueError("envelope_created_at_in_future")
        if message_created > trusted_now + skew:
            raise ValueError("message_created_at_in_future")
        if envelope_created < message_created:
            raise ValueError("envelope_created_before_message")
        if message_expires <= message_created:
            raise ValueError("message_expiry_not_after_creation")
        if envelope_expires != message_expires:
            raise ValueError("envelope_message_expiry_mismatch")
        if message_expires <= trusted_now:
            raise ValueError("governed_message_expired")
        return trusted_now


def governed_message_commitment(message: dict[str, Any]) -> str:
    supplied = message.get("message_digest")
    if not isinstance(supplied, str):
        raise ValueError("governed message lacks message_digest")
    computed = commitment({k: v for k, v in message.items() if k != "message_digest"})
    if supplied != computed:
        raise ValueError("governed message digest mismatch")
    content = message.get("content")
    if "content_digest" in message and message.get("content_digest") != commitment(content):
        raise ValueError("governed message content digest mismatch")
    return supplied


@dataclass(frozen=True)
class Route:
    route_id: str
    sender_endpoint_ref: str
    sender_identity_ref: str
    expected_sender_claim: str
    recipient_endpoint_ref: str
    recipient_identity_ref: str
    expected_recipient_claim: str
    local_endpoint: bool = True
    configured_identity_authenticated: bool = False

    def transport_binding(self) -> tuple[str, str, str, str]:
        return (
            self.sender_endpoint_ref,
            self.sender_identity_ref,
            self.recipient_endpoint_ref,
            self.recipient_identity_ref,
        )


class TrustedRouteTable:
    """Explicit local routing configuration.

    A route is trusted configuration for this fixture. It is not cryptographic
    network authentication and it is never an institutional authority grant.
    """

    def __init__(self, routes: list[Route]):
        self._routes = {route.route_id: route for route in routes}
        if len(self._routes) != len(routes):
            raise ValueError("route_id values must be unique")
        by_binding: dict[tuple[str, str, str, str], list[str]] = {}
        for route in routes:
            by_binding.setdefault(route.transport_binding(), []).append(route.route_id)
        ambiguous = [ids for ids in by_binding.values() if len(ids) > 1]
        if ambiguous:
            raise ValueError("ambiguous transport route binding")

    def resolve(self, route_id: str) -> Route:
        try:
            return self._routes[route_id]
        except KeyError as exc:
            raise LookupError("unknown trusted route") from exc

    def resolve_envelope(self, envelope: dict[str, Any]) -> Route:
        binding = (
            envelope.get("sender_endpoint_ref"),
            envelope.get("sender_identity_ref"),
            envelope.get("recipient_endpoint_ref"),
            envelope.get("recipient_identity_ref"),
        )
        matches = [r for r in self._routes.values() if r.transport_binding() == binding]
        if not matches:
            raise LookupError("untrusted_or_unknown_route_binding")
        if len(matches) != 1:
            raise LookupError("ambiguous_transport_route_binding")
        route = matches[0]
        if not route.local_endpoint:
            raise LookupError("recipient_endpoint_not_local")
        return route

    def verify_sender_endpoint(self, route: Route, sender_endpoint_ref: str) -> bool:
        return sender_endpoint_ref == route.sender_endpoint_ref

    def verify_recipient_endpoint(self, route: Route, recipient_endpoint_ref: str) -> bool:
        return recipient_endpoint_ref == route.recipient_endpoint_ref and route.local_endpoint


@dataclass
class RecipientOutcome:
    assessment: dict[str, Any]
    execution: dict[str, Any]
    producer_refs: dict[str, Any]
    result_state: str = "complete"
    artifact_export: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "result_profile": RECIPIENT_RESULT_PROFILE,
            "result_version": RECIPIENT_RESULT_VERSION,
            "result_state": self.result_state,
            "assessment": copy.deepcopy(self.assessment),
            "execution": copy.deepcopy(self.execution),
            "producer_refs": copy.deepcopy(self.producer_refs),
            "artifact_export": copy.deepcopy(self.artifact_export),
        }


class AcceptedGaxRecipientAdapter:
    """Adapter to the accepted GAX/IMX reference assessment/execution path.

    Each new recipient assessment receives the trusted delivery time from the
    transport. That same instant is supplied to run_exchange so the accepted
    Control Plane and executor perform current-time authorization/revalidation.
    Historical transport duplicates do not call this adapter again.
    """

    def __init__(
        self,
        *,
        bundle: dict[str, Any],
        registry: Any,
        manifest: dict[str, Any],
        exchange_store_path: str | Path,
        resolver: Any,
        destination: Any,
        execution_policy_factory: Callable[[Any], Any],
        clock_policy: DeliveryClockPolicy | None = None,
        evaluation_time: str | None = None,
    ):
        from experiments.odex_gax_imx_reference.gax_ref_runtime import (
            assess_message,
            run_exchange,
        )

        self._assess_message = assess_message
        self._run_exchange = run_exchange
        self.bundle = bundle
        self.registry = registry
        self.manifest = manifest
        self.exchange_store_path = Path(exchange_store_path)
        self.resolver = resolver
        self.destination = destination
        if execution_policy_factory is None:
            raise ValueError("execution_policy_factory is required")
        self.execution_policy_factory = execution_policy_factory
        self.clock_policy = clock_policy or DeliveryClockPolicy()
        # Retained only for source compatibility with the initial transport
        # profile. It is intentionally not used for recipient evaluation.
        self.constructor_evaluation_time = evaluation_time

    def handle(self, message: dict[str, Any], *, delivery_time: str) -> RecipientOutcome:
        current_time = iso(self.clock_policy.validate_now(delivery_time))
        assessment = self._assess_message(
            copy.deepcopy(message),
            self.bundle,
            self.registry,
            evaluation_time=current_time,
        )
        message_type = message.get("message_type")
        if message_type in INFORMATIONAL_TYPES:
            return RecipientOutcome(
                assessment=assessment,
                execution={
                    "attempted": False,
                    "reason": "informational_message_type",
                },
                producer_refs={},
            )
        if message_type not in EXECUTION_ELIGIBLE_TYPES:
            return RecipientOutcome(
                assessment=assessment,
                execution={
                    "attempted": False,
                    "reason": "unsupported_execution_message_type",
                },
                producer_refs={},
            )
        if assessment.get("permitted_handling") != "ACCEPT_FOR_ASSESSMENT":
            return RecipientOutcome(
                assessment=assessment,
                execution={
                    "attempted": False,
                    "reason": "recipient_assessment_not_eligible",
                },
                producer_refs={},
            )

        result = self._run_exchange(
            copy.deepcopy(message),
            self.bundle,
            self.registry,
            self.destination,
            evaluation_time=current_time,
            manifest=self.manifest,
            store_path=self.exchange_store_path,
            resolver=self.resolver,
            execution_policy_factory=self.execution_policy_factory,
        )
        execution = copy.deepcopy(result.get("execution") or {})
        artifact_export = copy.deepcopy(result.get("artifact_export"))
        artifact_refs = (
            (artifact_export or {}).get("producer_refs") or {}
            if isinstance(artifact_export, dict) else {}
        )
        producer_refs = {
            "decision_id": execution.get("decision_id"),
            "effect_id": execution.get("effect_id"),
            "executor_attempt_id": execution.get("attempt_id"),
            **copy.deepcopy(artifact_refs),
        }
        state = (
            (artifact_export or {}).get("state")
            if isinstance(artifact_export, dict) else "recovery_required"
        )
        return RecipientOutcome(
            assessment=copy.deepcopy(result.get("assessment") or assessment),
            execution=execution,
            producer_refs={k: v for k, v in producer_refs.items() if v is not None},
            result_state=state or "recovery_required",
            artifact_export=artifact_export,
        )

    def recover(self, message: dict[str, Any], *, delivery_time: str) -> RecipientOutcome:
        """Recover retained GAX artifacts without authorizing a replacement effect."""
        current_time = iso(self.clock_policy.validate_now(delivery_time))
        result = self._run_exchange(
            copy.deepcopy(message),
            self.bundle,
            self.registry,
            self.destination,
            evaluation_time=current_time,
            manifest=self.manifest,
            store_path=self.exchange_store_path,
            resolver=self.resolver,
            execution_policy_factory=self.execution_policy_factory,
        )
        execution = copy.deepcopy(result.get("execution") or {})
        artifact_export = copy.deepcopy(result.get("artifact_export"))
        refs = copy.deepcopy((artifact_export or {}).get("producer_refs") or {})
        refs.update({
            k: v for k, v in {
                "decision_id": execution.get("decision_id"),
                "effect_id": execution.get("effect_id"),
                "executor_attempt_id": execution.get("attempt_id"),
            }.items() if v is not None
        })
        return RecipientOutcome(
            assessment=copy.deepcopy(result.get("assessment") or {}),
            execution=execution,
            producer_refs=refs,
            result_state=(artifact_export or {}).get("state", "recovery_required"),
            artifact_export=artifact_export,
        )


class TransportStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as con:
            con.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS outbox (
                    message_id TEXT PRIMARY KEY,
                    route_id TEXT NOT NULL,
                    content_commitment TEXT NOT NULL,
                    canonicalization_version TEXT NOT NULL,
                    governed_message_json TEXT NOT NULL,
                    correlation_id TEXT,
                    conversation_id TEXT,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    state TEXT NOT NULL,
                    next_attempt_at TEXT,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    terminal_reason TEXT
                );
                CREATE TABLE IF NOT EXISTS inbox (
                    message_id TEXT PRIMARY KEY,
                    content_commitment TEXT NOT NULL,
                    governed_message_json TEXT NOT NULL,
                    sender_endpoint_ref TEXT NOT NULL,
                    recipient_endpoint_ref TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    assessment_json TEXT,
                    execution_json TEXT,
                    producer_refs_json TEXT
                );
                CREATE TABLE IF NOT EXISTS recipient_results (
                    message_id TEXT PRIMARY KEY,
                    result_profile TEXT NOT NULL,
                    result_version TEXT NOT NULL,
                    state TEXT NOT NULL,
                    artifact_export_json TEXT,
                    artifact_export_digest TEXT
                );
                CREATE TABLE IF NOT EXISTS attempts (
                    attempt_id TEXT PRIMARY KEY,
                    message_id TEXT NOT NULL,
                    route_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    state TEXT NOT NULL,
                    retry_of_attempt_id TEXT,
                    acknowledgement_id TEXT,
                    reason TEXT,
                    envelope_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS acknowledgements (
                    acknowledgement_id TEXT PRIMARY KEY,
                    message_id TEXT NOT NULL,
                    attempt_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    content_commitment TEXT NOT NULL,
                    recipient_endpoint_ref TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    detail_json TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_attempt_ordinal
                    ON attempts(message_id, ordinal);
                """
            )

    def enqueue(
        self,
        *,
        message: dict[str, Any],
        route_id: str,
        content_commitment: str,
        created_at: str,
        expires_at: str,
        correlation_id: str | None,
    ) -> bool:
        encoded = json.dumps(message, sort_keys=True, separators=(",", ":"))
        with sqlite3.connect(self.path, isolation_level="IMMEDIATE") as con:
            row = con.execute(
                "SELECT content_commitment, governed_message_json FROM outbox WHERE message_id=?",
                (message["message_id"],),
            ).fetchone()
            if row:
                if row[0] != content_commitment or row[1] != encoded:
                    raise ValueError("message_id reuse with changed governed content")
                return False
            con.execute(
                """
                INSERT INTO outbox(
                    message_id,route_id,content_commitment,canonicalization_version,
                    governed_message_json,correlation_id,conversation_id,created_at,
                    expires_at,state,next_attempt_at,attempt_count,terminal_reason
                ) VALUES(?,?,?,?,?,?,?,?,?,'QUEUED',NULL,0,NULL)
                """,
                (
                    message["message_id"],
                    route_id,
                    content_commitment,
                    CANONICALIZATION_VERSION,
                    encoded,
                    correlation_id,
                    message.get("conversation_id"),
                    created_at,
                    expires_at,
                ),
            )
            return True

    def outbox_record(self, message_id: str) -> dict[str, Any] | None:
        with sqlite3.connect(self.path) as con:
            con.row_factory = sqlite3.Row
            row = con.execute("SELECT * FROM outbox WHERE message_id=?", (message_id,)).fetchone()
            return dict(row) if row else None

    def inbox_record(self, message_id: str) -> dict[str, Any] | None:
        with sqlite3.connect(self.path) as con:
            con.row_factory = sqlite3.Row
            row = con.execute("SELECT * FROM inbox WHERE message_id=?", (message_id,)).fetchone()
            return dict(row) if row else None

    def record_attempt(
        self,
        *,
        envelope: dict[str, Any],
        route_id: str,
        ordinal: int,
        retry_of_attempt_id: str | None,
        now: str,
    ) -> None:
        with sqlite3.connect(self.path, isolation_level="IMMEDIATE") as con:
            con.execute(
                """
                INSERT INTO attempts(
                    attempt_id,message_id,route_id,ordinal,created_at,updated_at,state,
                    retry_of_attempt_id,acknowledgement_id,reason,envelope_json
                ) VALUES(?,?,?,?,?,?,'DISPATCHED',?,NULL,NULL,?)
                """,
                (
                    envelope["delivery_attempt_id"],
                    envelope["message_id"],
                    route_id,
                    ordinal,
                    now,
                    now,
                    retry_of_attempt_id,
                    json.dumps(envelope, sort_keys=True, separators=(",", ":")),
                ),
            )
            con.execute(
                "UPDATE outbox SET state='IN_FLIGHT', attempt_count=? WHERE message_id=?",
                (ordinal, envelope["message_id"]),
            )

    def mark_attempt(
        self,
        attempt_id: str,
        *,
        state: str,
        now: str,
        acknowledgement_id: str | None = None,
        reason: str | None = None,
    ) -> None:
        with sqlite3.connect(self.path, isolation_level="IMMEDIATE") as con:
            con.execute(
                """
                UPDATE attempts
                SET state=?,updated_at=?,acknowledgement_id=?,reason=?
                WHERE attempt_id=?
                """,
                (state, now, acknowledgement_id, reason, attempt_id),
            )

    def mark_outbox(
        self,
        message_id: str,
        *,
        state: str,
        next_attempt_at: str | None = None,
        terminal_reason: str | None = None,
    ) -> None:
        with sqlite3.connect(self.path, isolation_level="IMMEDIATE") as con:
            con.execute(
                """
                UPDATE outbox
                SET state=?,next_attempt_at=?,terminal_reason=?
                WHERE message_id=?
                """,
                (state, next_attempt_at, terminal_reason, message_id),
            )

    def record_inbox_before_ack(
        self,
        *,
        envelope: dict[str, Any],
        now: str,
    ) -> tuple[bool, dict[str, Any] | None]:
        message = envelope["governed_message"]
        encoded = json.dumps(message, sort_keys=True, separators=(",", ":"))
        with sqlite3.connect(self.path, isolation_level="IMMEDIATE") as con:
            con.row_factory = sqlite3.Row
            prior = con.execute(
                "SELECT * FROM inbox WHERE message_id=?",
                (envelope["message_id"],),
            ).fetchone()
            if prior:
                prior_dict = dict(prior)
                if (
                    prior_dict["content_commitment"] != envelope["content_commitment"]
                    or prior_dict["governed_message_json"] != encoded
                ):
                    raise ValueError("message_id reuse with changed content")
                return False, prior_dict
            con.execute(
                """
                INSERT INTO inbox(
                    message_id,content_commitment,governed_message_json,
                    sender_endpoint_ref,recipient_endpoint_ref,received_at,
                    assessment_json,execution_json,producer_refs_json
                ) VALUES(?,?,?,?,?,?,NULL,NULL,NULL)
                """,
                (
                    envelope["message_id"],
                    envelope["content_commitment"],
                    encoded,
                    envelope["sender_endpoint_ref"],
                    envelope["recipient_endpoint_ref"],
                    now,
                ),
            )
            return True, None

    def record_recipient_outcome(self, message_id: str, outcome: RecipientOutcome) -> None:
        payload = outcome.as_dict()
        artifact = payload.get("artifact_export")
        encoded_artifact = (
            json.dumps(artifact, sort_keys=True, separators=(",", ":"))
            if artifact is not None else None
        )
        artifact_digest = commitment(artifact) if artifact is not None else None
        with sqlite3.connect(self.path, isolation_level="IMMEDIATE") as con:
            con.execute(
                """
                UPDATE inbox
                SET assessment_json=?,execution_json=?,producer_refs_json=?
                WHERE message_id=?
                """,
                (
                    json.dumps(payload["assessment"], sort_keys=True, separators=(",", ":")),
                    json.dumps(payload["execution"], sort_keys=True, separators=(",", ":")),
                    json.dumps(payload["producer_refs"], sort_keys=True, separators=(",", ":")),
                    message_id,
                ),
            )
            con.execute(
                """
                INSERT OR REPLACE INTO recipient_results(
                    message_id,result_profile,result_version,state,
                    artifact_export_json,artifact_export_digest
                ) VALUES(?,?,?,?,?,?)
                """,
                (
                    message_id, payload["result_profile"], payload["result_version"],
                    payload["result_state"], encoded_artifact, artifact_digest,
                ),
            )

    def prior_outcome(self, message_id: str) -> RecipientOutcome | None:
        row = self.inbox_record(message_id)
        if not row or row.get("assessment_json") is None:
            return None
        with sqlite3.connect(self.path) as con:
            con.row_factory = sqlite3.Row
            result = con.execute(
                "SELECT * FROM recipient_results WHERE message_id=?",
                (message_id,),
            ).fetchone()
        if result is None:
            return RecipientOutcome(
                assessment=json.loads(row["assessment_json"]),
                execution=json.loads(row["execution_json"] or "{}"),
                producer_refs=json.loads(row["producer_refs_json"] or "{}"),
                result_state="historical_artifacts_unavailable",
                artifact_export=None,
            )
        if result["result_profile"] != RECIPIENT_RESULT_PROFILE or result["result_version"] != RECIPIENT_RESULT_VERSION:
            raise ValueError("unsupported retained recipient result profile")
        artifact = json.loads(result["artifact_export_json"]) if result["artifact_export_json"] else None
        if artifact is not None and result["artifact_export_digest"] != commitment(artifact):
            raise ValueError("retained recipient artifact digest mismatch")
        return RecipientOutcome(
            assessment=json.loads(row["assessment_json"]),
            execution=json.loads(row["execution_json"] or "{}"),
            producer_refs=json.loads(row["producer_refs_json"] or "{}"),
            result_state=result["state"],
            artifact_export=artifact,
        )

    def retained_result(self, message_id: str) -> RecipientOutcome | None:
        return self.prior_outcome(message_id)

    def record_ack(self, ack: dict[str, Any]) -> None:
        with sqlite3.connect(self.path, isolation_level="IMMEDIATE") as con:
            con.execute(
                """
                INSERT OR IGNORE INTO acknowledgements(
                    acknowledgement_id,message_id,attempt_id,kind,content_commitment,
                    recipient_endpoint_ref,created_at,detail_json
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    ack["acknowledgement_id"],
                    ack["message_id"],
                    ack["delivery_attempt_id"],
                    ack["acknowledgement_kind"],
                    ack["content_commitment"],
                    ack["recipient_endpoint_ref"],
                    ack["created_at"],
                    json.dumps(ack.get("detail") or {}, sort_keys=True, separators=(",", ":")),
                ),
            )

    def attempt_history(self, message_id: str) -> list[dict[str, Any]]:
        with sqlite3.connect(self.path) as con:
            con.row_factory = sqlite3.Row
            return [
                dict(row)
                for row in con.execute(
                    "SELECT * FROM attempts WHERE message_id=? ORDER BY ordinal,created_at",
                    (message_id,),
                )
            ]

    def acknowledgement_history(self, message_id: str) -> list[dict[str, Any]]:
        with sqlite3.connect(self.path) as con:
            con.row_factory = sqlite3.Row
            return [
                dict(row)
                for row in con.execute(
                    "SELECT * FROM acknowledgements WHERE message_id=? ORDER BY created_at",
                    (message_id,),
                )
            ]


def make_transport_envelope(
    *,
    governed_message: dict[str, Any],
    route: Route,
    delivery_attempt_id: str,
    correlation_id: str | None,
    created_at: str,
    retry_of_attempt_id: str | None = None,
) -> dict[str, Any]:
    content_commitment = governed_message_commitment(governed_message)
    return {
        "transport_profile": TRANSPORT_PROFILE,
        "transport_version": TRANSPORT_VERSION,
        "canonicalization_version": CANONICALIZATION_VERSION,
        "message_id": governed_message["message_id"],
        "delivery_attempt_id": delivery_attempt_id,
        "sender_endpoint_ref": route.sender_endpoint_ref,
        "sender_identity_ref": route.sender_identity_ref,
        "recipient_endpoint_ref": route.recipient_endpoint_ref,
        "recipient_identity_ref": route.recipient_identity_ref,
        "conversation_id": governed_message.get("conversation_id"),
        "correlation_id": correlation_id,
        "content_commitment": content_commitment,
        "created_at": created_at,
        "expires_at": governed_message["expires_at"],
        "retry_of_attempt_id": retry_of_attempt_id,
        "acknowledgement_for_attempt_id": None,
        "governed_message": copy.deepcopy(governed_message),
    }


class LocalDurableTransport:
    """SQLite-backed local reference transport.

    Consistency boundary: one SQLite transaction durably records an inbox
    message before a receipt acknowledgement is emitted. Sender and recipient
    stores are separate transactions. Therefore this reference is at-least-once
    with duplicate suppression, not distributed exactly-once delivery.
    """

    def __init__(
        self,
        *,
        sender_store_path: str | Path,
        recipient_store_path: str | Path,
        routes: TrustedRouteTable,
        recipient_handler: Callable[..., RecipientOutcome] | Any,
        max_attempts: int = 3,
        base_backoff_seconds: int = 1,
        attempt_id_factory: Callable[[], str] | None = None,
        ack_id_factory: Callable[[], str] | None = None,
        clock_policy: DeliveryClockPolicy | None = None,
    ):
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if base_backoff_seconds < 0:
            raise ValueError("base_backoff_seconds must be >= 0")
        self.sender_store = TransportStore(sender_store_path)
        self.recipient_store = TransportStore(recipient_store_path)
        self.routes = routes
        self.recipient_handler = recipient_handler
        self.max_attempts = max_attempts
        self.base_backoff_seconds = base_backoff_seconds
        self.attempt_id_factory = attempt_id_factory or (lambda: "delivery-" + uuid.uuid4().hex)
        self.ack_id_factory = ack_id_factory or (lambda: "ack-" + uuid.uuid4().hex)
        self.clock_policy = clock_policy or DeliveryClockPolicy()

    def queue(
        self,
        message: dict[str, Any],
        *,
        route_id: str,
        sender_endpoint_ref: str,
        now: str,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        trusted_now = iso(self.clock_policy.validate_now(now))
        route = self.routes.resolve(route_id)
        if not self.routes.verify_sender_endpoint(route, sender_endpoint_ref):
            raise PermissionError("sender endpoint is not trusted for route")
        if message.get("sender") != route.expected_sender_claim:
            raise PermissionError("governed sender claim does not match configured route")
        if message.get("recipient") != route.expected_recipient_claim:
            raise PermissionError("governed recipient claim does not match configured route")
        governed_message_commitment(message)
        # Queue-time timestamp checks use the same trusted clock policy as delivery.
        envelope_preview = make_transport_envelope(
            governed_message=message,
            route=route,
            delivery_attempt_id="queue-validation",
            correlation_id=correlation_id,
            created_at=trusted_now,
        )
        self.clock_policy.validate_temporal_binding(
            envelope=envelope_preview,
            message=message,
            now=trusted_now,
        )
        content_commitment = message["message_digest"]
        self.sender_store.enqueue(
            message=copy.deepcopy(message),
            route_id=route_id,
            content_commitment=content_commitment,
            created_at=trusted_now,
            expires_at=message["expires_at"],
            correlation_id=correlation_id,
        )
        return self.sender_store.outbox_record(message["message_id"]) or {}

    def _ack(
        self,
        envelope: dict[str, Any],
        *,
        now: str,
        kind: str,
        detail: dict[str, Any],
    ) -> dict[str, Any]:
        ack = {
            "acknowledgement_id": self.ack_id_factory(),
            "acknowledgement_kind": kind,
            "message_id": envelope.get("message_id"),
            "delivery_attempt_id": envelope.get("delivery_attempt_id"),
            "content_commitment": envelope.get("content_commitment"),
            "recipient_endpoint_ref": envelope.get("recipient_endpoint_ref"),
            "created_at": now,
            "detail": detail,
        }
        self.recipient_store.record_ack(ack)
        return ack

    def _recipient_reject(
        self,
        envelope: dict[str, Any],
        *,
        now: str,
        reason: str,
    ) -> dict[str, Any]:
        return self._ack(
            envelope,
            now=now,
            kind=ACK_KIND_TERMINAL_REJECTION,
            detail={
                "transport_status": "rejected",
                "reason": reason,
                "authorization": "not_evaluated_by_transport",
                "external_effect": "not_established",
            },
        )

    def _unresolved_receipt_ack(
        self,
        envelope: dict[str, Any],
        *,
        now: str,
        reason: str,
    ) -> dict[str, Any]:
        return self._ack(
            envelope,
            now=now,
            kind=ACK_KIND_DURABLE_RECEIPT_UNRESOLVED,
            detail={
                "acknowledged_object": "durable_inbox_receipt",
                "recipient_processing_status": "unresolved",
                "reason": reason,
                "authorization": "not_established",
                "execution": "not_established",
                "destination_observation": "not_established",
                "independent_verification": "not_established",
            },
        )

    def receive_envelope(
        self,
        envelope: dict[str, Any],
        *,
        now: str,
        fault_after_receipt: bool = False,
        fault_after_handler: bool = False,
    ) -> dict[str, Any]:
        try:
            trusted_now = iso(self.clock_policy.validate_now(now))
        except (TypeError, ValueError):
            # Without a valid trusted current time, no transport conclusion is usable.
            trusted_now = now if isinstance(now, str) else ""
            return self._recipient_reject(envelope, now=trusted_now, reason="invalid_trusted_delivery_time")

        if envelope.get("transport_profile") != TRANSPORT_PROFILE:
            return self._recipient_reject(envelope, now=trusted_now, reason="unsupported_transport_profile")
        if envelope.get("transport_version") != TRANSPORT_VERSION:
            return self._recipient_reject(envelope, now=trusted_now, reason="unsupported_transport_version")
        if envelope.get("canonicalization_version") != CANONICALIZATION_VERSION:
            return self._recipient_reject(envelope, now=trusted_now, reason="unsupported_canonicalization")

        message = envelope.get("governed_message")
        if not isinstance(message, dict):
            return self._recipient_reject(envelope, now=trusted_now, reason="missing_governed_message")
        if message.get("message_id") != envelope.get("message_id"):
            return self._recipient_reject(envelope, now=trusted_now, reason="message_identity_mismatch")
        if message.get("conversation_id") != envelope.get("conversation_id"):
            return self._recipient_reject(envelope, now=trusted_now, reason="conversation_identity_mismatch")

        try:
            self.clock_policy.validate_temporal_binding(
                envelope=envelope,
                message=message,
                now=trusted_now,
            )
        except (TypeError, ValueError) as exc:
            return self._recipient_reject(envelope, now=trusted_now, reason=str(exc))

        try:
            route = self.routes.resolve_envelope(envelope)
        except LookupError as exc:
            return self._recipient_reject(envelope, now=trusted_now, reason=str(exc))
        if message.get("sender") != route.expected_sender_claim:
            return self._recipient_reject(envelope, now=trusted_now, reason="sender_claim_mismatch")
        if message.get("recipient") != route.expected_recipient_claim:
            return self._recipient_reject(envelope, now=trusted_now, reason="recipient_claim_mismatch")

        try:
            expected = governed_message_commitment(message)
        except ValueError as exc:
            return self._recipient_reject(envelope, now=trusted_now, reason=str(exc))
        if expected != envelope.get("content_commitment"):
            return self._recipient_reject(envelope, now=trusted_now, reason="content_commitment_mismatch")

        try:
            is_new, _ = self.recipient_store.record_inbox_before_ack(
                envelope=envelope,
                now=trusted_now,
            )
        except ValueError:
            return self._recipient_reject(
                envelope,
                now=trusted_now,
                reason="message_id_content_conflict",
            )

        if fault_after_receipt and is_new:
            raise SyntheticTransportInterruption("after_receipt_before_recipient_processing")

        if is_new:
            outcome = self.recipient_handler.handle(
                copy.deepcopy(message),
                delivery_time=trusted_now,
            )
            if fault_after_handler:
                raise SyntheticTransportInterruption("after_recipient_processing_before_outcome_persistence")
            self.recipient_store.record_recipient_outcome(message["message_id"], outcome)
        else:
            try:
                outcome = self.recipient_store.prior_outcome(message["message_id"])
            except ValueError as exc:
                return self._unresolved_receipt_ack(
                    envelope, now=trusted_now, reason=str(exc)
                )
            if outcome is None or outcome.result_state in {
                "recovery_required", "historical_artifacts_unavailable"
            }:
                recover = getattr(self.recipient_handler, "recover", None)
                if recover is None:
                    return self._unresolved_receipt_ack(
                        envelope,
                        now=trusted_now,
                        reason="durable_receipt_without_recoverable_recipient_outcome",
                    )
                outcome = recover(copy.deepcopy(message), delivery_time=trusted_now)
                if outcome.artifact_export is None:
                    return self._unresolved_receipt_ack(
                        envelope,
                        now=trusted_now,
                        reason="recipient_recovery_artifacts_unavailable",
                    )
                self.recipient_store.record_recipient_outcome(
                    message["message_id"], outcome
                )

        return self._ack(
            envelope,
            now=trusted_now,
            kind=ACK_KIND_DURABLE_RECEIPT,
            detail={
                "acknowledged_object": "durable_inbox_receipt",
                "duplicate_suppressed": not is_new,
                "recipient_processing_status": "retained",
                "recipient_assessment_handling": outcome.assessment.get("permitted_handling"),
                "authorization": "not_implied_by_acknowledgement",
                "execution": "not_implied_by_acknowledgement",
                "destination_observation": "not_implied_by_acknowledgement",
                "independent_verification": "not_implied_by_acknowledgement",
                "producer_refs": copy.deepcopy(outcome.producer_refs),
            },
        )

    def deliver(
        self,
        message_id: str,
        *,
        now: str,
        lose_ack: bool = False,
        force: bool = False,
        fault_after_receipt: bool = False,
        fault_after_handler: bool = False,
    ) -> dict[str, Any]:
        trusted_now = iso(self.clock_policy.validate_now(now))
        row = self.sender_store.outbox_record(message_id)
        if row is None:
            raise LookupError("message not queued")
        if row["state"] in {"DELIVERED", "TERMINAL_REJECTED", "UNRESOLVED"} and not force:
            return {
                "message_id": message_id,
                "transport_state": row["state"],
                "attempted": False,
            }
        if parse_time(row["expires_at"]) <= parse_time(trusted_now):
            self.sender_store.mark_outbox(
                message_id,
                state="TERMINAL_REJECTED",
                terminal_reason="expired_before_dispatch",
            )
            return {
                "message_id": message_id,
                "transport_state": "TERMINAL_REJECTED",
                "attempted": False,
                "reason": "expired_before_dispatch",
            }
        if row["next_attempt_at"] and parse_time(trusted_now) < parse_time(row["next_attempt_at"]):
            return {
                "message_id": message_id,
                "transport_state": "BACKOFF",
                "attempted": False,
                "next_attempt_at": row["next_attempt_at"],
            }

        ordinal = int(row["attempt_count"]) + 1
        if ordinal > self.max_attempts:
            self.sender_store.mark_outbox(
                message_id,
                state="UNRESOLVED",
                terminal_reason="retry_exhausted_without_sender_ack",
            )
            return {
                "message_id": message_id,
                "transport_state": "UNRESOLVED",
                "attempted": False,
                "reason": "retry_exhausted_without_sender_ack",
            }

        route = self.routes.resolve(row["route_id"])
        message = json.loads(row["governed_message_json"])
        prior = self.sender_store.attempt_history(message_id)
        retry_of = prior[-1]["attempt_id"] if prior else None
        attempt_id = self.attempt_id_factory()
        envelope = make_transport_envelope(
            governed_message=message,
            route=route,
            delivery_attempt_id=attempt_id,
            correlation_id=row["correlation_id"],
            created_at=trusted_now,
            retry_of_attempt_id=retry_of,
        )
        self.sender_store.record_attempt(
            envelope=envelope,
            route_id=row["route_id"],
            ordinal=ordinal,
            retry_of_attempt_id=retry_of,
            now=trusted_now,
        )

        try:
            ack = self.receive_envelope(
                copy.deepcopy(envelope),
                now=trusted_now,
                fault_after_receipt=fault_after_receipt,
                fault_after_handler=fault_after_handler,
            )
        except SyntheticTransportInterruption as exc:
            next_time = parse_time(trusted_now) + timedelta(
                seconds=self.base_backoff_seconds * (2 ** max(0, ordinal - 1))
            )
            self.sender_store.mark_attempt(
                attempt_id,
                state="INTERRUPTED",
                now=trusted_now,
                reason=str(exc),
            )
            self.sender_store.mark_outbox(
                message_id,
                state="PENDING_RETRY",
                next_attempt_at=iso(next_time),
            )
            return {
                "message_id": message_id,
                "delivery_attempt_id": attempt_id,
                "transport_state": "PENDING_RETRY",
                "attempted": True,
                "acknowledgement_received": False,
                "interruption": str(exc),
            }

        if lose_ack:
            next_time = parse_time(trusted_now) + timedelta(
                seconds=self.base_backoff_seconds * (2 ** max(0, ordinal - 1))
            )
            self.sender_store.mark_attempt(
                attempt_id,
                state="ACK_LOST",
                now=trusted_now,
                reason="synthetic_ack_loss",
            )
            if ordinal >= self.max_attempts:
                self.sender_store.mark_outbox(
                    message_id,
                    state="UNRESOLVED",
                    terminal_reason="retry_exhausted_without_sender_ack",
                )
                state = "UNRESOLVED"
            else:
                self.sender_store.mark_outbox(
                    message_id,
                    state="PENDING_RETRY",
                    next_attempt_at=iso(next_time),
                )
                state = "PENDING_RETRY"
            return {
                "message_id": message_id,
                "delivery_attempt_id": attempt_id,
                "transport_state": state,
                "attempted": True,
                "acknowledgement_received": False,
            }

        self.sender_store.record_ack(ack)
        kind = ack["acknowledgement_kind"]
        if kind == ACK_KIND_TERMINAL_REJECTION:
            reason = (ack.get("detail") or {}).get("reason")
            self.sender_store.mark_attempt(
                attempt_id,
                state="TERMINAL_REJECTED",
                now=trusted_now,
                acknowledgement_id=ack["acknowledgement_id"],
                reason=reason,
            )
            self.sender_store.mark_outbox(
                message_id,
                state="TERMINAL_REJECTED",
                terminal_reason=reason,
            )
            state = "TERMINAL_REJECTED"
        elif kind == ACK_KIND_DURABLE_RECEIPT_UNRESOLVED:
            reason = (ack.get("detail") or {}).get("reason")
            self.sender_store.mark_attempt(
                attempt_id,
                state="RECEIPT_UNRESOLVED",
                now=trusted_now,
                acknowledgement_id=ack["acknowledgement_id"],
                reason=reason,
            )
            self.sender_store.mark_outbox(
                message_id,
                state="UNRESOLVED",
                terminal_reason=reason,
            )
            state = "UNRESOLVED"
        else:
            self.sender_store.mark_attempt(
                attempt_id,
                state="ACKNOWLEDGED",
                now=trusted_now,
                acknowledgement_id=ack["acknowledgement_id"],
            )
            self.sender_store.mark_outbox(message_id, state="DELIVERED")
            state = "DELIVERED"
        return {
            "message_id": message_id,
            "delivery_attempt_id": attempt_id,
            "transport_state": state,
            "attempted": True,
            "acknowledgement_received": True,
            "acknowledgement": ack,
        }

    def recover_due(self, *, now: str, lose_ack: bool = False) -> list[dict[str, Any]]:
        trusted_now = iso(self.clock_policy.validate_now(now))
        with sqlite3.connect(self.sender_store.path) as con:
            con.row_factory = sqlite3.Row
            rows = [
                dict(r)
                for r in con.execute(
                    """
                    SELECT * FROM outbox
                    WHERE state IN ('QUEUED','PENDING_RETRY','IN_FLIGHT')
                    ORDER BY created_at,message_id
                    """
                )
            ]
        results = []
        for row in rows:
            if row.get("next_attempt_at") and parse_time(trusted_now) < parse_time(row["next_attempt_at"]):
                continue
            results.append(self.deliver(row["message_id"], now=trusted_now, lose_ack=lose_ack))
        return results

    def evidence(self, message_id: str) -> dict[str, Any]:
        outbox = self.sender_store.outbox_record(message_id)
        inbox = self.recipient_store.inbox_record(message_id)
        attempts = self.sender_store.attempt_history(message_id)
        sender_acks = self.sender_store.acknowledgement_history(message_id)
        recipient_acks = self.recipient_store.acknowledgement_history(message_id)
        producer_refs: dict[str, Any] = {}
        recipient_processing_status = "not_received"
        if inbox:
            recipient_processing_status = (
                "retained"
                if inbox.get("assessment_json") is not None
                else "unresolved_after_durable_receipt"
            )
        if inbox and inbox.get("producer_refs_json"):
            producer_refs = json.loads(inbox["producer_refs_json"])
        retained = None
        try:
            retained = self.recipient_store.retained_result(message_id)
        except ValueError:
            retained = None
        artifact_summary = None
        if retained and retained.artifact_export:
            artifact = retained.artifact_export
            artifact_summary = {
                "result_profile": RECIPIENT_RESULT_PROFILE,
                "result_version": RECIPIENT_RESULT_VERSION,
                "state": retained.result_state,
                "export_profile": artifact.get("export_profile"),
                "export_version": artifact.get("export_version"),
                "producer_refs": copy.deepcopy(artifact.get("producer_refs") or {}),
                "artifact_commitment": commitment(artifact),
            }
        return {
            "transport_profile": TRANSPORT_PROFILE,
            "transport_version": TRANSPORT_VERSION,
            "message_id": message_id,
            "content_commitment": outbox.get("content_commitment") if outbox else None,
            "outbox_state": outbox.get("state") if outbox else None,
            "inbox_received_at": inbox.get("received_at") if inbox else None,
            "recipient_processing_status": recipient_processing_status,
            "delivery_attempts": [
                {
                    "transport_attempt_id": row["attempt_id"],
                    "ordinal": row["ordinal"],
                    "state": row["state"],
                    "retry_of_transport_attempt_id": row["retry_of_attempt_id"],
                    "acknowledgement_id": row["acknowledgement_id"],
                    "reason": row["reason"],
                }
                for row in attempts
            ],
            "sender_acknowledgements": [
                {
                    "acknowledgement_id": row["acknowledgement_id"],
                    "transport_attempt_id": row["attempt_id"],
                    "kind": row["kind"],
                }
                for row in sender_acks
            ],
            "recipient_acknowledgements": [
                {
                    "acknowledgement_id": row["acknowledgement_id"],
                    "transport_attempt_id": row["attempt_id"],
                    "kind": row["kind"],
                }
                for row in recipient_acks
            ],
            "producer_refs": producer_refs,
            "retained_artifact_summary": artifact_summary,
            "namespace_rule": (
                "transport_attempt_id is transport-owned; decision_id, effect_id and "
                "executor_attempt_id are linked only when returned by the recipient producer"
            ),
            "unsupported": [
                "cryptographically_authenticated_network_identity",
                "distributed_exactly_once_delivery",
                "independent_effect_verification",
            ],
        }
