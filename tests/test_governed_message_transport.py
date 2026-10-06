from __future__ import annotations

import copy
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import pytest

from experiments.governed_message_transport import (
    LocalDurableTransport,
    RecipientOutcome,
    Route,
    TrustedRouteTable,
    commitment,
    make_transport_envelope,
)
from experiments.governed_message_transport.transport import (
    ACK_KIND_DURABLE_RECEIPT,
    ACK_KIND_TERMINAL_REJECTION,
    TRANSPORT_PROFILE,
    TRANSPORT_VERSION,
)


NOW = "2026-10-06T12:00:00Z"
LATER = "2026-10-06T12:00:02Z"
MUCH_LATER = "2026-10-06T12:00:10Z"


def message(
    *,
    message_id: str = "msg-1",
    message_type: str = "PROPOSE",
    sender: str = "agent-a",
    recipient: str = "agent-b",
    expires_at: str = "2026-10-07T00:00:00Z",
) -> dict:
    content = {"task": "synthetic", "value": 1}
    item = {
        "message_id": message_id,
        "conversation_id": "conv-1",
        "profile": "urn:cognous:profiles:odex-gax-imx-refund-exchange:0.1.0",
        "protocol_version": "0.1.0",
        "message_type": message_type,
        "created_at": "2026-10-06T11:59:00Z",
        "expires_at": expires_at,
        "sender": sender,
        "recipient": recipient,
        "purpose": "refund_execution_assessment",
        "requested_action": {"action_id": "synthetic"},
        "operation_commitment": "sha256:" + "1" * 64,
        "proposal_commitment": "sha256:" + "2" * 64,
        "authority_refs": [],
        "evidence_refs": [],
        "content": content,
        "content_digest": commitment(content),
        "acknowledgement_requested": True,
    }
    item["message_digest"] = commitment(
        {k: v for k, v in item.items() if k != "message_digest"}
    )
    return item


def route_table() -> TrustedRouteTable:
    return TrustedRouteTable(
        [
            Route(
                route_id="local-a-b",
                sender_endpoint_ref="local://sender-a",
                sender_identity_ref="urn:cognous:transport-identity:agent-a",
                expected_sender_claim="agent-a",
                recipient_endpoint_ref="local://recipient-b",
                recipient_identity_ref="urn:cognous:transport-identity:agent-b",
                expected_recipient_claim="agent-b",
                configured_identity_authenticated=False,
            )
        ]
    )


@dataclass
class DurableFixtureHandler:
    path: Path
    handling: str = "ACCEPT_FOR_ASSESSMENT"
    execute: bool = True

    def __post_init__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as con:
            con.execute(
                "CREATE TABLE IF NOT EXISTS calls(message_id TEXT PRIMARY KEY, count INTEGER NOT NULL)"
            )
            con.execute(
                "CREATE TABLE IF NOT EXISTS effects(effect_id TEXT PRIMARY KEY, message_id TEXT UNIQUE)"
            )

    def handle(self, item: dict) -> RecipientOutcome:
        with sqlite3.connect(self.path, isolation_level="IMMEDIATE") as con:
            row = con.execute(
                "SELECT count FROM calls WHERE message_id=?", (item["message_id"],)
            ).fetchone()
            count = (row[0] if row else 0) + 1
            con.execute(
                "INSERT OR REPLACE INTO calls(message_id,count) VALUES(?,?)",
                (item["message_id"], count),
            )
            attempted = (
                self.execute
                and self.handling == "ACCEPT_FOR_ASSESSMENT"
                and item["message_type"] in {"PROPOSE", "REQUEST"}
            )
            effect_id = None
            if attempted:
                effect_id = "effect-" + item["message_id"]
                con.execute(
                    "INSERT OR IGNORE INTO effects(effect_id,message_id) VALUES(?,?)",
                    (effect_id, item["message_id"]),
                )
        return RecipientOutcome(
            assessment={"permitted_handling": self.handling},
            execution={
                "attempted": attempted,
                "effect_id": effect_id,
                "reason": None if attempted else "no_effect",
            },
            producer_refs=({"effect_id": effect_id} if effect_id else {}),
        )

    def call_count(self, message_id: str) -> int:
        with sqlite3.connect(self.path) as con:
            row = con.execute(
                "SELECT count FROM calls WHERE message_id=?", (message_id,)
            ).fetchone()
            return row[0] if row else 0

    def effects(self) -> list[tuple]:
        with sqlite3.connect(self.path) as con:
            return list(con.execute("SELECT effect_id,message_id FROM effects ORDER BY effect_id"))


def transport(tmp_path: Path, handler: DurableFixtureHandler, *, max_attempts: int = 3):
    counter = {"attempt": 0, "ack": 0}

    def attempt_id():
        counter["attempt"] += 1
        return f"delivery-{counter['attempt']}"

    def ack_id():
        counter["ack"] += 1
        return f"ack-{counter['ack']}"

    return LocalDurableTransport(
        sender_store_path=tmp_path / "sender.sqlite",
        recipient_store_path=tmp_path / "recipient.sqlite",
        routes=route_table(),
        recipient_handler=handler,
        max_attempts=max_attempts,
        base_backoff_seconds=1,
        attempt_id_factory=attempt_id,
        ack_id_factory=ack_id,
    )


def queue(t: LocalDurableTransport, item: dict):
    return t.queue(
        item,
        route_id="local-a-b",
        sender_endpoint_ref="local://sender-a",
        now=NOW,
        correlation_id="corr-1",
    )


def test_valid_delivery_preserves_identity_and_ack_meaning(tmp_path):
    handler = DurableFixtureHandler(tmp_path / "effects.sqlite")
    t = transport(tmp_path, handler)
    item = message()
    queued = queue(t, item)
    result = t.deliver(item["message_id"], now=NOW)

    assert result["transport_state"] == "DELIVERED"
    ack = result["acknowledgement"]
    assert ack["acknowledgement_kind"] == ACK_KIND_DURABLE_RECEIPT
    assert ack["message_id"] == item["message_id"]
    assert ack["content_commitment"] == item["message_digest"]
    assert ack["detail"]["acknowledged_object"] == "durable_inbox_receipt"
    assert ack["detail"]["authorization"] == "not_implied_by_acknowledgement"
    assert ack["detail"]["execution"] == "not_implied_by_acknowledgement"
    assert queued["content_commitment"] == item["message_digest"]

    inbox = t.recipient_store.inbox_record(item["message_id"])
    assert inbox is not None
    assert json.loads(inbox["governed_message_json"]) == item
    assert handler.effects() == [("effect-msg-1", "msg-1")]
    evidence = t.evidence(item["message_id"])
    assert evidence["delivery_attempts"][0]["transport_attempt_id"] == "delivery-1"
    assert evidence["producer_refs"]["effect_id"] == "effect-msg-1"


def test_wrong_recipient_and_untrusted_sender_fail_before_assessment(tmp_path):
    handler = DurableFixtureHandler(tmp_path / "effects.sqlite")
    t = transport(tmp_path, handler)

    wrong = message(recipient="agent-x")
    with pytest.raises(PermissionError):
        queue(t, wrong)
    assert handler.effects() == []

    good = message()
    route = route_table().resolve("local-a-b")
    env = make_transport_envelope(
        governed_message=good,
        route=route,
        delivery_attempt_id="external-1",
        correlation_id="c",
        created_at=NOW,
    )
    env["sender_endpoint_ref"] = "local://attacker"
    ack = t.receive_envelope(env, now=NOW)
    assert ack["acknowledgement_kind"] == ACK_KIND_TERMINAL_REJECTION
    assert ack["detail"]["reason"] == "untrusted_sender_endpoint"
    assert handler.call_count(good["message_id"]) == 0
    assert handler.effects() == []


def test_altered_content_and_message_id_reuse_conflict_are_rejected(tmp_path):
    handler = DurableFixtureHandler(tmp_path / "effects.sqlite")
    t = transport(tmp_path, handler)
    first = message()
    queue(t, first)

    changed = copy.deepcopy(first)
    changed["content"]["value"] = 99
    changed["content_digest"] = commitment(changed["content"])
    changed["message_digest"] = commitment(
        {k: v for k, v in changed.items() if k != "message_digest"}
    )
    with pytest.raises(ValueError, match="message_id reuse"):
        queue(t, changed)

    route = route_table().resolve("local-a-b")
    env = make_transport_envelope(
        governed_message=first,
        route=route,
        delivery_attempt_id="external-2",
        correlation_id="c",
        created_at=NOW,
    )
    env["governed_message"]["content"]["value"] = 22
    ack = t.receive_envelope(env, now=NOW)
    assert ack["acknowledgement_kind"] == ACK_KIND_TERMINAL_REJECTION
    assert "digest mismatch" in ack["detail"]["reason"]
    assert handler.effects() == []


def test_duplicate_and_out_of_order_delivery_suppresses_second_assessment(tmp_path):
    handler = DurableFixtureHandler(tmp_path / "effects.sqlite")
    t = transport(tmp_path, handler)
    item = message()
    route = route_table().resolve("local-a-b")

    second = make_transport_envelope(
        governed_message=item,
        route=route,
        delivery_attempt_id="delivery-2",
        correlation_id="c",
        created_at=LATER,
        retry_of_attempt_id="delivery-1",
    )
    ack2 = t.receive_envelope(second, now=LATER)
    first = make_transport_envelope(
        governed_message=item,
        route=route,
        delivery_attempt_id="delivery-1",
        correlation_id="c",
        created_at=NOW,
    )
    ack1 = t.receive_envelope(first, now=LATER)

    assert ack2["acknowledgement_kind"] == ACK_KIND_DURABLE_RECEIPT
    assert ack1["acknowledgement_kind"] == ACK_KIND_DURABLE_RECEIPT
    assert ack2["detail"]["duplicate_suppressed"] is False
    assert ack1["detail"]["duplicate_suppressed"] is True
    assert handler.call_count(item["message_id"]) == 1
    assert handler.effects() == [("effect-msg-1", "msg-1")]


def test_lost_ack_restart_recovery_preserves_message_and_attempt_lineage(tmp_path):
    handler = DurableFixtureHandler(tmp_path / "effects.sqlite")
    t1 = transport(tmp_path, handler)
    item = message()
    queue(t1, item)

    first = t1.deliver(item["message_id"], now=NOW, lose_ack=True)
    assert first["transport_state"] == "PENDING_RETRY"
    assert handler.call_count(item["message_id"]) == 1
    assert len(handler.effects()) == 1

    t2 = LocalDurableTransport(
        sender_store_path=tmp_path / "sender.sqlite",
        recipient_store_path=tmp_path / "recipient.sqlite",
        routes=route_table(),
        recipient_handler=handler,
        max_attempts=3,
        base_backoff_seconds=1,
        attempt_id_factory=lambda: "delivery-restart-2",
        ack_id_factory=lambda: "ack-restart-2",
    )
    recovered = t2.recover_due(now=LATER)
    assert recovered[0]["transport_state"] == "DELIVERED"
    assert handler.call_count(item["message_id"]) == 1
    assert len(handler.effects()) == 1

    evidence = t2.evidence(item["message_id"])
    assert [a["transport_attempt_id"] for a in evidence["delivery_attempts"]] == [
        "delivery-1",
        "delivery-restart-2",
    ]
    assert evidence["delivery_attempts"][1]["retry_of_transport_attempt_id"] == "delivery-1"
    assert evidence["content_commitment"] == item["message_digest"]


def test_expiry_and_retry_exhaustion_are_terminal_or_unresolved(tmp_path):
    handler = DurableFixtureHandler(tmp_path / "effects.sqlite")
    t = transport(tmp_path, handler, max_attempts=2)

    expired = message(message_id="expired", expires_at="2026-10-06T11:00:00Z")
    queue(t, expired)
    result = t.deliver("expired", now=NOW)
    assert result["transport_state"] == "TERMINAL_REJECTED"
    assert result["reason"] == "expired_before_dispatch"
    assert handler.call_count("expired") == 0

    retry = message(message_id="retry")
    queue(t, retry)
    first = t.deliver("retry", now=NOW, lose_ack=True)
    assert first["transport_state"] == "PENDING_RETRY"
    second = t.deliver("retry", now=LATER, lose_ack=True)
    assert second["transport_state"] == "UNRESOLVED"
    assert t.sender_store.outbox_record("retry")["terminal_reason"] == "retry_exhausted_without_sender_ack"
    assert handler.call_count("retry") == 1
    assert handler.effects() == [("effect-retry", "retry")]


@pytest.mark.parametrize("message_type", ["REPORT", "REFUSE", "NOT_UNDERSTOOD"])
def test_informational_acts_produce_no_effect(tmp_path, message_type):
    handler = DurableFixtureHandler(tmp_path / "effects.sqlite")
    t = transport(tmp_path, handler)
    item = message(message_id=message_type.lower(), message_type=message_type)
    queue(t, item)
    result = t.deliver(item["message_id"], now=NOW)
    assert result["transport_state"] == "DELIVERED"
    assert handler.effects() == []


@pytest.mark.parametrize("handling", ["HOLD", "DENY"])
def test_recipient_hold_or_deny_produces_no_effect(tmp_path, handling):
    handler = DurableFixtureHandler(
        tmp_path / "effects.sqlite",
        handling=handling,
        execute=True,
    )
    t = transport(tmp_path, handler)
    item = message()
    queue(t, item)
    result = t.deliver(item["message_id"], now=NOW)
    assert result["transport_state"] == "DELIVERED"
    assert result["acknowledgement"]["detail"]["recipient_assessment_handling"] == handling
    assert handler.effects() == []


def test_delivery_retry_after_committed_effect_never_creates_second_effect(tmp_path):
    handler = DurableFixtureHandler(tmp_path / "effects.sqlite")
    t = transport(tmp_path, handler, max_attempts=3)
    item = message()
    queue(t, item)

    first = t.deliver(item["message_id"], now=NOW, lose_ack=True)
    assert first["transport_state"] == "PENDING_RETRY"
    assert handler.effects() == [("effect-msg-1", "msg-1")]

    second = t.deliver(item["message_id"], now=LATER)
    assert second["transport_state"] == "DELIVERED"
    assert second["acknowledgement"]["detail"]["duplicate_suppressed"] is True
    assert handler.call_count(item["message_id"]) == 1
    assert handler.effects() == [("effect-msg-1", "msg-1")]


def test_unsupported_transport_version_is_terminal_rejection(tmp_path):
    handler = DurableFixtureHandler(tmp_path / "effects.sqlite")
    t = transport(tmp_path, handler)
    item = message()
    route = route_table().resolve("local-a-b")
    env = make_transport_envelope(
        governed_message=item,
        route=route,
        delivery_attempt_id="external-unsupported",
        correlation_id="c",
        created_at=NOW,
    )
    env["transport_version"] = "99.0.0"
    ack = t.receive_envelope(env, now=NOW)
    assert ack["acknowledgement_kind"] == ACK_KIND_TERMINAL_REJECTION
    assert ack["detail"]["reason"] == "unsupported_transport_version"
    assert handler.effects() == []


def test_transport_attempt_namespace_is_distinct_from_executor_attempt_namespace(tmp_path):
    handler = DurableFixtureHandler(tmp_path / "effects.sqlite")
    t = transport(tmp_path, handler)
    item = message()
    queue(t, item)
    t.deliver(item["message_id"], now=NOW)
    evidence = t.evidence(item["message_id"])
    assert evidence["delivery_attempts"][0]["transport_attempt_id"] == "delivery-1"
    assert "executor_attempt_id" not in evidence["delivery_attempts"][0]
    assert "linked only when returned by the recipient producer" in evidence["namespace_rule"]
