"""Actual transported accepted-generation qualification; deterministic faults."""
import json
import os
from pathlib import Path
import sqlite3
from unittest.mock import patch

import pytest
from test_gax_public_runtime_artifacts import _accepted_transport, EVAL, effect_count
from experiments.odex_gax_imx_reference.gax_ref_runtime import digest


def snapshot(destination, root):
    with sqlite3.connect(destination.path) as db:
        rows = list(db.iterdump())
    return rows, {p.name: p.read_bytes() for p in root.glob('control-plane-*.json')}


@pytest.mark.parametrize('mode', ['success', 'wrong_effect', 'unavailable', 'lost_ack', 'partial', 'prior_absence', 'revoked', 'evidence_failure'])
def test_transport_repaired_chain(tmp_path, mode):
    options = {'lose_ack': True} if mode == 'lost_ack' else {'partial_delivery': True} if mode == 'partial' else {'fault_evidence_once': True} if mode == 'evidence_failure' else {}
    if mode == 'revoked':
        def revoke(resolver):
            for status in resolver.statuses.values(): status.status = 'revoked'
        options['mutate_resolver_after_decision'] = revoke
    transport, dest, message, handler = _accepted_transport(tmp_path, 'repair-' + mode, exchange_options=options)
    from engine.control_plane_adapter import ControlPlaneRefundDestinationAdapter
    original = ControlPlaneRefundDestinationAdapter.observe
    def observe(adapter, effect_id):
        result = original(adapter, effect_id)
        if adapter.outcome.result is not None:
            if mode == 'unavailable': raise OSError('synthetic unavailable observation')
            if mode == 'wrong_effect': result.effect_id = 'rejected-unrelated-effect'
        return result
    with patch.object(ControlPlaneRefundDestinationAdapter, 'observe', observe):
        if mode == 'prior_absence':
            with patch.object(dest, 'commit', side_effect=TimeoutError('synthetic before commit')):
                transport.deliver(message['message_id'], now=EVAL)
        else: transport.deliver(message['message_id'], now=EVAL)
    count = 0 if mode in {'prior_absence', 'revoked'} else 1
    assert effect_count(dest) == count
    before = snapshot(dest, tmp_path)
    if mode == 'evidence_failure':
        handler.recover(message, delivery_time=EVAL)
        assert snapshot(dest, tmp_path) == before
        # Recipient transport retry obtains the derivative from GAX without dispatch.
        transport.recover_due(now='2026-08-08T01:00:02Z')
    retained = transport.retained_artifacts(message['message_id'])['artifact_export']
    assert retained is not None
    bundle = retained['reconstruction_bundle']
    records = lambda kind: [r['data'] for r in bundle['records'] if r['record_type'] == kind]
    effect = retained['producer_refs']['effect_id']
    result = records('execution_result')[0]
    if count:
        actual = dest.observe(effect)['destination_state']
        op = records('execution_envelope')[0]['operation']
        assert actual['effect_id'] == effect
        assert all(actual[k] == op[k] for k in ('target', 'amount', 'unit', 'payload'))
    if mode in {'wrong_effect', 'unavailable'}:
        assert result['acknowledged'] and result['observation'] is None
        assert retained['successor_packet']['unresolved_delivery'] is True
    if mode == 'lost_ack':
        assert result['acknowledged'] is False and result['observed_state'] == 'applied'
    if mode == 'prior_absence':
        assert retained['successor_packet']['pending_effects'] == [effect]
        assert retained['successor_packet']['unresolved_delivery'] is True
        assert result['acknowledged'] is False
    if mode == 'revoked':
        assert retained['successor_packet']['pending_effects'] == []
        assert retained['successor_packet']['unresolved_delivery'] is False
        assert not records('control_plane_attempt_transition')
    if mode == 'partial':
        assert bundle['status'] == 'reconstruction_complete'
        assert retained['successor_packet']['unresolved_delivery'] is True
    before = snapshot(dest, tmp_path)
    from agent_governance_evidence_pack import import_manifest_reconstruction, validate_evidence_pack, render_traceable_markdown
    pack = import_manifest_reconstruction(handler.manifest, bundle)
    assert validate_evidence_pack(pack).valid
    assert 'unavailable' in render_traceable_markdown(pack)
    from odes import evaluate_recipient_package
    package = retained['odes']['odes_package']
    check = evaluate_recipient_package(package, {'now':'2026-10-06T00:00:00Z', 'purpose':'audit', 'relying_party':'recipient.example.org', 'status_inputs':{}, 'supported_profiles':[package['profile']['implementation_profile']], 'trusted_digests':[], 'trusted_key_refs':[], 'evaluation_scope':'audit', 'status_max_age_seconds':300, 'allow_unauthenticated_informational_inspection':False})
    assert check['package_content_integrity']['status'] == 'pass'
    assert check['integrity_authentication_checks']['status'] == 'unavailable'
    assert check['authority_status_and_freshness']['status'] == 'unavailable'
    assert snapshot(dest, tmp_path) == before
    transport.deliver(message['message_id'], now=EVAL, force=True)
    assert transport.retained_artifacts(message['message_id'])['artifact_export'] == retained
    assert snapshot(dest, tmp_path) == before
    if mode in {'wrong_effect', 'unavailable', 'prior_absence', 'lost_ack'}:
        from engine.producer_contract import DurableRefundDestination
        handler.destination = DurableRefundDestination(dest.root)
        recovered = handler.resume_original(message, delivery_time=EVAL)
        assert recovered['execution']['effect_id'] == effect
        assert recovered['execution']['newly_executed'] is False
        assert effect_count(dest) == count
        assert snapshot(dest, tmp_path)[0] == before[0]
        if mode == 'prior_absence':
            assert recovered['execution']['attempt_status'] == 'denied'
            recs = recovered['execution_facts']['reconciliations']
            assert recs[-1]['result'] == 'observed_absent' and recs[-1]['retry_eligible'] is False
            assert recovered['execution_facts']['pending_effects'] == [effect]
            assert recovered['execution_facts']['unresolved_delivery'] is True
            assert recovered['successor_packet']['pending_effects'] == [effect]
            assert recovered['successor_packet']['unresolved_delivery'] is True
            assert recovered['execution_facts']['destination_effects'] == []
            assert recovered['execution_facts']['acknowledgement_summary'] == 'unknown'
        else:
            assert recovered['execution_facts']['destination_observed'] == 'applied'
            assert recovered['successor_packet']['unresolved_delivery'] is False
            assert recovered['successor_packet']['pending_effects'] == []
            if mode in {'wrong_effect','unavailable'}:
                assert any(not r['observation_accepted'] for r in recovered['execution_facts']['reconciliations'])
        if mode == 'lost_ack':
            assert recovered['execution_facts']['acknowledgement_summary'] == 'mixed'
            assert any(a['status'] == 'unknown' and not a['acknowledgement'] for a in recovered['execution_facts']['control_plane_attempt_transitions'])
        assert transport.retained_artifacts(message['message_id'])['artifact_export'] == retained
    assert retained['producer_refs']['reconstruction_digest'] == digest(bundle)
    output = os.environ.get('GAX_QUALIFICATION_RESULTS')
    if output:
        path = Path(output)
        results = json.loads(path.read_text()) if path.exists() else {}
        results[mode] = {'classification':'required_safety_invariant_pass',
            'effect_id':effect, 'decision_id':result['decision_id'],
            'attempt_identity':retained['producer_refs'].get('attempt_identity'),
            'destination':dest.observe(effect), 'effect_count':count,
            'initial_pending_effects':retained['successor_packet']['pending_effects'],
            'initial_unresolved_delivery':retained['successor_packet']['unresolved_delivery'],
            'initial_acknowledged':result['acknowledged'],
            'initial_observed_state':result['observed_state'],
            'reconstruction_status':bundle['status'],
            'evidence_only_stores_unchanged':True,
            'replay_id':bundle['bundle_id'], 'replay_digest':digest(bundle),
            'original_export_digest':digest(retained),
            'recovery':recovered['execution_facts'] if mode in {'wrong_effect','unavailable','prior_absence','lost_ack'} else None}
        path.write_text(json.dumps(results, indent=2) + '\n')


def test_missing_observation_policy_fails_closed(tmp_path):
    transport, dest, message, handler = _accepted_transport(tmp_path, 'missing-policy')
    handler.observation_policy = None
    result = handler.handle(message, delivery_time=EVAL)
    assert result.execution['reason'] == 'observation_policy_required'
    assert effect_count(dest) == 0


def test_transport_held_before_attempt_has_no_pending_effect(tmp_path):
    transport, dest, message, handler = _accepted_transport(tmp_path, 'held-before-attempt')
    for status in handler.resolver.statuses.values():
        status.status = 'revoked'
    transport.deliver(message['message_id'], now=EVAL)
    retained = transport.retained_artifacts(message['message_id'])['artifact_export']
    assert retained['successor_packet']['pending_effects'] == []
    assert retained['successor_packet']['unresolved_delivery'] is False
    assert not any(r['record_type'] == 'control_plane_attempt_transition' for r in retained['reconstruction_bundle']['records'])
    assert effect_count(dest) == 0
    before = snapshot(dest, tmp_path)
    transport.deliver(message['message_id'], now=EVAL, force=True)
    assert transport.retained_artifacts(message['message_id'])['artifact_export'] == retained
    assert snapshot(dest, tmp_path) == before
