# Batch 4C-E — Alvorada/GAX consumer migration

Starting main: `6bcde026a804c7377f5e39f57ca6dd00b3c3292d`.
Branch: `worker14b/gax-producer-v2`. No existing 4C-E work was found. Deferred PR #2 was not consumed. Final head and one-time CI status are recorded in the PR handoff.

## Supported dependency generation

| Component | Accepted revision |
|---|---|
| Control Plane | `2ea9528eeb87e14ff10f05de06473122b9df540f` |
| Executor | `177354e959cc78c59c1a776f018cfbfbf28c927b` |
| Replay | `274543f1cd7171784a923a8e37015017a0d8bc9d` |
| ODES | `226adb0e3cde5377ac9db6f7e5857bfa7e65e30a` |
| Evidence Pack | `812194b9a89a5fa21e675200fcb4e0089666f1b6` |
| Manifest | `46c950bed37fe3812000895430bc0312d29e37ce` |
| Authority Context | `fb3d97938969a89e149e8ff8db2756091d1233fc` |

Executor producer 2.0.0; Execution Envelope and Reconstruction Bundle 0.2.0; ODES implementation profile 0.2 with base pder-v0.1 unchanged. GAX retained-artifact export advances to 1.1.0 for explicit observation-reconciliation derivatives. Historical export 1.0.0 remains readable without relabeling; new recovery refuses historical producer generations. Old base `gax_ref` execution routines remain historical, not the supported public executor path.

## Implementation and evidence

[Migration/interface notes](../gax-runtime-artifact-interface.md) and [actual scenario results](gax-observation-repair-results.json) are the entry points.

- Public Transport → GAX → accepted Control Plane/executor tests cover success; rejected wrong-effect and unavailable post-dispatch observation; restarted original-effect recovery; lost acknowledgement; partial delivery; prior attempt followed by fresh absence/denied resubmission; revocation after decision; exact redelivery; and post-effect evidence-only recovery.
- Caller ObservationPolicy is propagated without a permissive default. Missing policy fails closed. Synthetic policy/clock exist only in demo/test setup. Trusted timezone-aware time passes to execute/reconciliation. Production uses the public executor's clock unless supplied explicitly.
- Nullable result observation, acknowledgement and full Control Plane evidence are retained separately in response and producer artifacts. ODES validated history drives successor delivery status, not completeness. Applied observation with unknown acknowledgement is not unresolved delivery solely because the acknowledgement was lost. Historical acknowledgement remains in the history.
- Explicit `resume_original` requires retained prior attempt, matching original operation and a unique owning CP store. It calls the accepted executor and writes a separately identified derivative. It does not overwrite original Replay/ODES/successor artifacts or transport results. Prior absence cannot issue a replacement. This operation adds CP evidence; pure import/export/redelivery does not.
- Tests assert exact effect ID, target, amount, unit and payload, counts, unchanged full logical destination DB and unchanged CP bytes for evidence-only operations. Replay → Evidence Pack import/validate/render and accepted ODES recipient APIs run inside those assertions. Package integrity passes; authentication/current authority remain unavailable.
- Original Replay IDs/canonical commitments remain bound across the chain. Separate attempt namespaces, rejected content and successor null decision attribution are preserved. Existing tamper, lineage, informational-message, transport receipt, continuity and runtime-with-tests-namespace-blocked checks remain enforced.

## Validation

Local Python 3.12.14: **87 passed, zero failures, zero skips**, including 9 new observation-repair tests. Demo command passed. Focused integration checks ran first. Initial full gate exposed two historical acknowledgement/delivery conflation assertions and a new test's early transport retry; these were corrected without weakening partial-delivery or unknown-acknowledgement requirements.

```sh
# Configure the exact checkouts from .github/workflows/tests.yml.
pytest -q tests/test_gax_public_runtime_artifacts.py tests/test_governed_message_transport_integration.py
pytest -q tests/test_observation_repair.py
GAX_QUALIFICATION_RESULTS=/tmp/gax-observation-repair-results.json pytest -q tests
python -m experiments.odex_gax_imx_reference.demo --manifest "$UPSTREAM_MANIFEST_EXAMPLE" --replay "$UPSTREAM_REPLAY_SUCCESS_EXAMPLE" --out /tmp/gax-imx-demo.json
```

CI runs Python 3.11/3.12, verifies exact pins, and uploads actual scenario results plus demo output. One final-head CI check only; queued means unexecuted at the checkpoint, never a green claim.

## Remaining limits and next step

Governor review; no self-merge. The next bounded batch is the hub pin update and remaining Batch 4C qualification, including cases 3–5 and research extensions. This batch does not modify the hub lock or adjacent implementations. Same-host SQLite/store evidence does not establish distributed delivery, cross-host budget enforcement, production authentication, live confinement, independent verification or effective human oversight. Original-effect recovery requires a unique accessible owning local CP store; concurrent multi-process recovery is not newly qualified. No population reconciliation, deployment applicability or human-oversight study was added.
