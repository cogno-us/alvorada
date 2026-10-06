from __future__ import annotations

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
    _build_resolver,
    load_actual_pinned_moltbot_helpers,
    make_message,
    parse_time,
    runtime_proposal_model,
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
    resolver = _build_resolver(proposal, now=parse_time(EVAL))
    h = load_actual_pinned_moltbot_helpers()
    destination = h.DurableRefundDestination(tmp_path / "moltbot-state")
    registry = LocalRegistry({"refund-sender"}, {"refund-recipient"}, "refund-recipient")
    handler = AcceptedGaxRecipientAdapter(
        bundle=bundle,
        registry=registry,
        evaluation_time=EVAL,
        manifest=manifest,
        exchange_store_path=tmp_path / "gax-exchange.sqlite",
        resolver=resolver,
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
