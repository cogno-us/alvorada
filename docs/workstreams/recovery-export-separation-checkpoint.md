# Recovery export separation checkpoint

Baseline: `1dbfe2bdd1c647e742f17aa2eab2a0dfb768681f`

Branch: `worker14d/recovery-export-separation`

Scope: bounded repair to Alvorada GAX recovery/export semantics only. No dependency pins, Replay code, hub tests, deployment configuration, or adjacent repositories changed.

## Reproduction and root cause

The hub reproduction remains `cogno-us/cognous-open-control-stack` PR #6 at head `07c250ed4f6fae448dff3d77c7db3a5c5d056531`, smallest case:

`tests/test_research_qualification.py::test_recovery_authority[applied-revocation]`

An original effect commits with unknown acknowledgement. Current authority then changes. The accepted executor correctly denies the recovery request before destination observation. At the accepted Alvorada baseline, the recovery pipeline exported that new denied executor result together with the historical applied destination effect. Replay correctly rejected the collapsed episode with:

`denied execution result contradicts retained effect evidence`

Replay's validation remains unchanged.

## Corrected repair

`resume_original_exchange` distinguishes recovery outcomes using evidence produced by the accepted executor for that call.

### Denial before destination observation

The historical-only `recovery_denied_derivative` path is selected only when:

- `result.status == "denied"`; and
- the current result carries no `control_plane_evidence.reconciliation`.

For this pre-observation denial:

- historical reconstruction, ODES package, successor evidence, original decision/effect IDs, acknowledgement history and effect evidence remain unchanged;
- the current denial is returned on the execution surface;
- derivative lineage binds the current recovery result to its evaluation time and original artifact/checkpoint;
- lineage retains the actual executor `error` as `recovery_reason` and the complete serialized current `recovery_result`;
- `destination_observation_performed=false`;
- `replacement_dispatch_performed=false`;
- `renewed_authorization=false`;
- `effect_reexecution=false`.

No denied current execution result is passed to Replay together with historical applied effect rows.

### Denial after observation/reconciliation

A denied result carrying current `control_plane_evidence.reconciliation` does **not** take the historical-only path. It follows the normal producer/replay pipeline so current observation and reconciliation evidence is retained.

The required prior-attempt-absence case with unchanged valid authority is covered explicitly: recovery performs one bound destination observation, produces `observed_absent`, retains `retry_eligible=false`, leaves the original effect pending/unresolved, and performs no replacement dispatch.

This distinction prevents the repair from falsely labelling an observed recovery as unobserved or discarding current rejection evidence.

## Regression coverage

`tests/test_recovery_export_separation.py` contains:

1. Ten changed-authority cases:
   - revocation
   - expiry
   - policy-version change
   - stale required evidence
   - invalid approval

   Each is exercised against historical applied effect and prior-attempt absence. These assert current denial, zero destination observation queries, no dispatch, unchanged destination/Control Plane state, original artifact immutability, preserved identities and historical acknowledgement/effect evidence.

2. Valid-authority prior-attempt absence:
   - exactly one destination observation occurs;
   - no destination commit/replacement dispatch occurs;
   - current reconciliation is `observed_absent`;
   - `retry_eligible=false`;
   - original effect remains pending and delivery unresolved;
   - the derivative is `reconciled_derivative`;
   - original retained transport artifacts remain unchanged.

3. Replay negative control:
   - obtains the actual accepted Moltbot producer record from `TransactionalExchangeStore.workflow_for_message` after the supported `run_once` fixture/export path;
   - changes only the producer execution result status to `denied` while retaining real effect rows;
   - asserts Replay still raises exactly `denied execution result contradicts retained effect evidence`.

The changed-authority test uses the accepted response contract: an empty observation object is absence of observation evidence. It additionally asserts zero destination queries and no Control Plane reconciliation evidence for the pre-observation branch.

## Producer-reference preservation

The historical-only derivative is built from a validated retained export. `_artifacts_from_retained_export` now carries the complete validated original `producer_refs` forward as `producer_identity_refs`. `_retained_artifact_export` then recomputes reconstruction, ODES, validation and successor references/commitments from the derivative contents in the normal way. Because the historical contents are unchanged, those recomputed artifact references remain equal to the original; no missing identity is synthesized.

Regression assertions require complete `producer_refs` equality between original and denial derivative and explicitly verify decision ID, effect ID, attempt identity/IDs, executor producer profile and executor repository whenever those fields are present.

## CI history

Initial PR head `a95d6d1d6e98add6b7734b24e90f83ad3e9c4668`, Actions run `37562096841`:

- Python 3.11: **11 failed, 88 passed**.
- Python 3.12 job was cancelled after the matrix failure.
- Ten failures were a test-contract error: accepted denied responses expose empty observation `{}`, not `None`.
- The negative-control failure was a fixture-access error: `run_once` does not return `moltbot_record` at the top level.

Corrected head `6f88aa298d6f90a96f0409244cea417d3006a162`, Actions run `37563767907`:

- complete test invocation reached **90 passed, 10 failed**;
- all ten failures were `KeyError: effect_id` in denial-derivative `producer_refs`;
- root cause: `_artifacts_from_retained_export` restored historical artifacts but omitted `producer_identity_refs`, so derivative construction dropped original execution identities;
- the demonstration step did not run because the test step failed.

That defect is corrected by preserving the complete validated original producer references before derivative reconstruction. Final-head Actions status is recorded after the final code/document commits. The repository workflow runs the complete `pytest -q tests` suite and, only after tests pass, the demonstration command with the exact accepted dependency pins.

## Compatibility

- GAX retained artifact profile/version remains `urn:cognous:profiles:gax-retained-artifacts` / `1.1.0`.
- Replay `274543f1cd7171784a923a8e37015017a0d8bc9d`: unchanged.
- Moltbot Safe `177354e959cc78c59c1a776f018cfbfbf28c927b`: unchanged producer profile 2.0.0.
- Control Plane `2ea9528eeb87e14ff10f05de06473122b9df540f`: unchanged.
- ODES `226adb0e3cde5377ac9db6f7e5857bfa7e65e30a`: unchanged.
- Evidence Pack `812194b9a89a5fa21e675200fcb4e0089666f1b6`: unchanged.

Consumers that exhaustively enumerate retained-artifact `state` values must tolerate `recovery_denied_derivative`. Existing fields do not change meaning.

## Deliberately unchanged

- Hub PR #6 and its tests.
- Replay validation logic.
- Authority rules and denial reasons.
- Destination observation APIs.
- Retry authorization semantics.
- Original retained artifacts and transport receipts.
- Dependency pins.
- Deployment/release state.
