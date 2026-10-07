from __future__ import annotations

from experiments.odex_gax_imx_reference.synthetic_fixture import synthetic_observation_policy, synthetic_observation_clock

import json
import os
import sqlite3
from pathlib import Path

import pytest

from experiments.governed_message_transport import (
    AcceptedGaxRecipientAdapter,
    LocalDurableTransport,
    Route,
    TrustedRouteTable,
)
from experiments.odex_gax_imx_reference.gax_ref_runtime import (
    LocalRegistry,
    load_executor_runtime,
    make_message,
    parse_time,
    runtime_proposal_model,
)

from experiments.odex_gax_imx_reference.synthetic_fixture import (
    build_synthetic_resolver,
    synthetic_refund_policy,
)

EVAL = "2026-08-08T01:00:00Z"


def _load_env(name: str) -> dict:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"missing pinned integration fixture {name}")
    return json.loads(Path(value).read_text(encoding="utf-8"))


def _rows(destination):
    with sqlite3.connect(destination.path) as con:
        con.row_factory = sqlite3.Row
        return [dict(r) for r in con.execute("SELECT * FROM effects ORDER BY rowid")]


def _routes() -> TrustedRouteTable:
    return TrustedRouteTable(
        [
            Route(
                route_id="accepted-gax-local",
                sender_endpoint_ref="local://refund-sender",
                sender_identity_ref="urn:cognous:transport-identity:refund-sender",
                expected_sender_claim="refund-sender",
                recipient_endpoint_ref="local://refund-recipient",
                recipient_identity_ref="urn:cognous:transport-identity:refund-recipient",
                expected_recipient_claim="refund-recipient",
                configured_identity_authenticated=False,
            )
        ]
    )


def test_transport_to_accepted_gax_assessment_lost_ack_restart_no_second_effect(tmp_path):
    manifest = _load_env("UPSTREAM_MANIFEST_EXAMPLE")
    bundle = _load_env("UPSTREAM_REPLAY_SUCCESS_EXAMPLE")
    proposal = runtime_proposal_model(bundle)
    resolver = build_synthetic_resolver(proposal, now=parse_time(EVAL))
    runtime = load_executor_runtime()
    destination = runtime["DurableRefundDestination"](tmp_path / "moltbot-state")
    registry = LocalRegistry({"refund-sender"}, {"refund-recipient"}, "refund-recipient")
    handler = AcceptedGaxRecipientAdapter(
        bundle=bundle,
        registry=registry,
        evaluation_time=EVAL,
        manifest=manifest,
        exchange_store_path=tmp_path / "gax-exchange.sqlite",
        resolver=resolver,
        execution_policy_factory=synthetic_refund_policy,
        observation_policy=synthetic_observation_policy(),
        observation_clock=synthetic_observation_clock,
        destination=destination,
    )
    ids = iter(["transport-attempt-1", "transport-attempt-2"])
    acks = iter(["transport-ack-1", "transport-ack-2"])
    t = LocalDurableTransport(
        sender_store_path=tmp_path / "transport-sender.sqlite",
        recipient_store_path=tmp_path / "transport-recipient.sqlite",
        routes=_routes(),
        recipient_handler=handler,
        max_attempts=3,
        base_backoff_seconds=1,
        attempt_id_factory=lambda: next(ids),
        ack_id_factory=lambda: next(acks),
    )
    governed = make_message(bundle, message_id="transport-gax-1")
    original_digest = governed["message_digest"]
    t.queue(
        governed,
        route_id="accepted-gax-local",
        sender_endpoint_ref="local://refund-sender",
        now=EVAL,
        correlation_id="corr-accepted-gax",
    )

    first = t.deliver("transport-gax-1", now=EVAL, lose_ack=True)
    assert first["transport_state"] == "PENDING_RETRY"
    assert len(_rows(destination)) == 1

    restarted = LocalDurableTransport(
        sender_store_path=tmp_path / "transport-sender.sqlite",
        recipient_store_path=tmp_path / "transport-recipient.sqlite",
        routes=_routes(),
        recipient_handler=handler,
        max_attempts=3,
        base_backoff_seconds=1,
        attempt_id_factory=lambda: "transport-attempt-2",
        ack_id_factory=lambda: "transport-ack-2",
    )
    recovered = restarted.recover_due(now="2026-08-08T01:00:02Z")
    assert recovered[0]["transport_state"] == "DELIVERED"
    assert recovered[0]["acknowledgement"]["detail"]["duplicate_suppressed"] is True
    assert len(_rows(destination)) == 1

    inbox = restarted.recipient_store.inbox_record("transport-gax-1")
    assert json.loads(inbox["governed_message_json"]) == governed
    assert restarted.evidence("transport-gax-1")["content_commitment"] == original_digest
    refs = restarted.evidence("transport-gax-1")["producer_refs"]
    assert refs["effect_id"] == _rows(destination)[0]["effect_id"]
    assert refs["decision_id"]
    assert refs["executor_attempt_id"]



def _accepted_fixture(tmp_path: Path):
    manifest = _load_env("UPSTREAM_MANIFEST_EXAMPLE")
    bundle = _load_env("UPSTREAM_REPLAY_SUCCESS_EXAMPLE")
    proposal = runtime_proposal_model(bundle)
    resolver = build_synthetic_resolver(proposal, now=parse_time(EVAL))
    runtime = load_executor_runtime()
    destination = runtime["DurableRefundDestination"](tmp_path / "moltbot-state")
    registry = LocalRegistry({"refund-sender"}, {"refund-recipient"}, "refund-recipient")
    handler = AcceptedGaxRecipientAdapter(
        bundle=bundle,
        registry=registry,
        evaluation_time=EVAL,
        manifest=manifest,
        exchange_store_path=tmp_path / "gax-exchange.sqlite",
        resolver=resolver,
        execution_policy_factory=synthetic_refund_policy,
        observation_policy=synthetic_observation_policy(),
        observation_clock=synthetic_observation_clock,
        destination=destination,
    )
    return bundle, handler, destination


def _transport_for_accepted(tmp_path: Path, handler, *, attempt_id: str, ack_id: str):
    return LocalDurableTransport(
        sender_store_path=tmp_path / "transport-sender.sqlite",
        recipient_store_path=tmp_path / "transport-recipient.sqlite",
        routes=_routes(),
        recipient_handler=handler,
        max_attempts=3,
        base_backoff_seconds=1,
        attempt_id_factory=lambda: attempt_id,
        ack_id_factory=lambda: ack_id,
    )


def test_delivery_time_not_constructor_time_blocks_expired_grant(tmp_path):
    bundle, handler, destination = _accepted_fixture(tmp_path)
    t = _transport_for_accepted(
        tmp_path,
        handler,
        attempt_id="expired-grant-attempt",
        ack_id="expired-grant-ack",
    )
    governed = make_message(
        bundle,
        message_id="transport-expired-grant",
        expires_at="2026-08-10T00:00:00Z",
    )
    t.queue(
        governed,
        route_id="accepted-gax-local",
        sender_endpoint_ref="local://refund-sender",
        now=EVAL,
    )

    result = t.deliver(
        governed["message_id"],
        now="2026-08-09T01:00:00Z",
    )

    assert result["transport_state"] == "DELIVERED"
    assert _rows(destination) == []
    inbox = t.recipient_store.inbox_record(governed["message_id"])
    execution = json.loads(inbox["execution_json"])
    assessment = json.loads(inbox["assessment_json"])
    assert execution["attempted"] is False
    assert assessment["stages"]["authority"] in {"hold", "denied"}


def test_historical_duplicate_after_grant_expiry_does_not_renew_or_repeat_effect(tmp_path):
    bundle, handler, destination = _accepted_fixture(tmp_path)
    ids = iter(["pre-expiry-attempt", "post-expiry-duplicate"])
    acks = iter(["pre-expiry-ack", "post-expiry-ack"])
    t = LocalDurableTransport(
        sender_store_path=tmp_path / "transport-sender.sqlite",
        recipient_store_path=tmp_path / "transport-recipient.sqlite",
        routes=_routes(),
        recipient_handler=handler,
        max_attempts=3,
        base_backoff_seconds=1,
        attempt_id_factory=lambda: next(ids),
        ack_id_factory=lambda: next(acks),
    )
    governed = make_message(
        bundle,
        message_id="transport-historical-duplicate",
        expires_at="2026-08-10T00:00:00Z",
    )
    t.queue(
        governed,
        route_id="accepted-gax-local",
        sender_endpoint_ref="local://refund-sender",
        now=EVAL,
    )

    first = t.deliver(governed["message_id"], now=EVAL, lose_ack=True)
    assert first["transport_state"] == "PENDING_RETRY"
    assert len(_rows(destination)) == 1
    before = t.recipient_store.inbox_record(governed["message_id"])
    before_refs = json.loads(before["producer_refs_json"])

    second = t.deliver(
        governed["message_id"],
        now="2026-08-09T01:00:00Z",
    )
    assert second["transport_state"] == "DELIVERED"
    assert second["acknowledgement"]["detail"]["duplicate_suppressed"] is True
    assert len(_rows(destination)) == 1
    after = t.recipient_store.inbox_record(governed["message_id"])
    assert json.loads(after["producer_refs_json"]) == before_refs
    assert second["acknowledgement"]["detail"]["producer_refs"] == before_refs
