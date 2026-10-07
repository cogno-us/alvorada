# Recovery export separation checkpoint

Baseline: `1dbfe2bdd1c647e742f17aa2eab2a0dfb768681f`

Branch: `worker14d/recovery-export-separation`

Scope: bounded repair to Alvorada GAX recovery/export semantics only. No dependency pins, Replay code, hub tests, deployment configuration, or adjacent repositories changed.

## Reproduction

The hub reproduction at `cogno-us/cognous-open-control-stack` PR #6, head `07c250ed4f6fae448dff3d77c7db3a5c5d056531`, identifies the smallest failing case as:

`tests/test_research_qualification.py::test_recovery_authority[applied-revocation]`

The original effect commits with unknown acknowledgement. Current authority is then revoked. `AcceptedGaxRecipientAdapter.resume_original` correctly reaches the public executor, which denies before destination observation. At the accepted baseline, Alvorada then exports that new denied executor result together with the original retained applied destination effect. Replay rejects the combined producer record with:

`denied execution result contradicts retained effect evidence`

Replay's rejection is correct for a single execution episode. The producer export was collapsing two different episodes.

## Repair

`resume_original_exchange` now treats a current pre-observation denial as a separate recovery derivative:

- Historical reconstruction, ODES package, successor evidence, original decision/effect IDs, acknowledgement history, and destination-effect evidence remain unchanged.
- The returned execution surface reports the current recovery denial.
- The derivative lineage records:
  - `relationship=recovery_denial_derivative`
  - trusted recovery evaluation time
  - scope `current_authority_revalidation_before_destination_observation`
  - `recovery_status=denied`
  - `destination_observation_performed=false`
  - `replacement_dispatch_performed=false`
  - `renewed_authorization=false`
  - `effect_reexecution=false`
  - commitments to the original checkpoint and retained artifact.
- No current denied result is passed to Replay with historical effect rows.
- Replay's denied/effect contradiction check is unchanged.

The export profile/version remains `urn:cognous:profiles:gax-retained-artifacts` / `1.1.0`. This is an additive state/lineage use of the existing retained-artifact envelope, not a schema or dependency contract change.

## Regression coverage

New `tests/test_recovery_export_separation.py` covers all ten required authority/effect combinations:

- revocation × historical applied / prior-attempt absence
- expiry × historical applied / prior-attempt absence
- policy-version change × historical applied / prior-attempt absence
- stale required evidence × historical applied / prior-attempt absence
- invalid approval × historical applied / prior-attempt absence

Each case asserts:

- current recovery is denied;
- no replacement dispatch;
- no fresh destination observation;
- unchanged destination effect count;
- unchanged owning Control Plane bytes;
- original transport-retained artifact is byte-equivalent by canonical digest;
- original decision/effect identity is preserved;
- historical reconstruction and acknowledgement history survive;
- applied history remains applied without becoming current permission;
- prior-attempt absence remains pending/unresolved and no reconciliation grants retry.

A separate malformed-producer regression changes a valid executor result to `status=denied` while retaining effect rows and asserts Replay still raises the exact contradiction error. This proves the repair does not weaken the downstream invariant or allow fabricated effect evidence on a denied execution.

## Execution status

Local container execution was not available because this environment cannot resolve GitHub for cloning the pinned dependency repositories. Repository writes and review were performed through the connected GitHub interface. GitHub Actions on the branch/PR is therefore the executable source of truth. Final-head CI must be checked once and recorded in the PR handoff.

## Compatibility

Expected compatible consumers:

- Replay `274543f1cd7171784a923a8e37015017a0d8bc9d`: unchanged contract and unchanged contradiction guard.
- Moltbot Safe `177354e959cc78c59c1a776f018cfbfbf28c927b`: unchanged producer profile 2.0.0.
- Control Plane `2ea9528eeb87e14ff10f05de06473122b9df540f`: unchanged public execution/revalidation behavior.
- ODES `226adb0e3cde5377ac9db6f7e5857bfa7e65e30a` and Evidence Pack `812194b9a89a5fa21e675200fcb4e0089666f1b6`: historical evidence remains the same retained reconstruction.

Consumers that enumerate artifact `state` values exhaustively must accept `recovery_denied_derivative`. The export version does not change because state is already an open producer-level discriminator in the existing envelope; no existing field changes meaning.

## Deliberately unchanged

- Hub PR #6 and its failing tests.
- Replay validation logic.
- Authority rules and decision reasons.
- Destination observation APIs.
- Retry authorization semantics.
- Original retained artifacts and transport receipts.
- Dependency pins.
- Deployment/release state.
