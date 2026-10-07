# GAX Control Plane store adoption checkpoint

Status: **Phase B complete on accepted persistence-compatible consumers; Governor review pending.**

## Scope and baseline

- Repository: `cogno-us/alvorada`
- Starting SHA: `c52f9f0b998a77c0dbac7e8c56e1be1b5117e1df`
- Branch: `worker13/gax-control-plane-store-adoption`
- Accepted GAX baseline: `c52f9f0b998a77c0dbac7e8c56e1be1b5117e1df`
- Deferred Alvorada PR #2 is not consumed.
- No hub or adjacent-repository changes are part of this workstream.

The existing GAX retained-artifact profile remains
`urn:cognous:profiles:gax-retained-artifacts` version `1.1.0`. The
dependency revision update does not change its wire semantics.

## Dependency roles

### Selected runtime generation for this branch

| Role | Repository | Revision | Status / purpose |
|---|---|---|---|
| Runtime Control Plane | `cogno-us/cognous-agent-control-plane` | `248d899634d9db3518e831bc7ab568a48733f825` | accepted same-host record persistence repair |
| Executor producer | `cogno-us/moltbot-safe` | `177354e959cc78c59c1a776f018cfbfbf28c927b` | accepted executor producer profile 2.0.0 |
| Replay decoder | `cogno-us/cognous-agent-replay-bundle` | `043830b56595cecddfa65c064afd1c0b95e64792` | accepted compatibility for Control Plane persistence revision |
| ODES consumer | `cogno-us/open-decision-evidence-standard` | `0486b645e99c46d9cd16ca34b1ba7c653a6b3024` | accepted persistence-compatible ODES mapping |
| Evidence Pack consumer | `cogno-us/cognous-agent-governance-evidence-pack` | `de6b9e071df49fc3e0c1254d39b5c94cced554f0` | accepted persistence-compatible retained-Replay mapping |
| Manifest | `cogno-us/cognous-agent-action-manifest` | `46c950bed37fe3812000895430bc0312d29e37ce` | unchanged |
| Authority Context | `cogno-us/constitutional-governance-for-institutions` | `fb3d97938969a89e149e8ff8db2756091d1233fc` | unchanged |

### Preserved historical compatibility generation

The immediately preceding accepted GAX generation remains historical evidence
and is not relabeled:

- Control Plane: `2ea9528eeb87e14ff10f05de06473122b9df540f`
- Replay: `274543f1cd7171784a923a8e37015017a0d8bc9d`
- Executor: `177354e959cc78c59c1a776f018cfbfbf28c927b`
- ODES: `226adb0e3cde5377ac9db6f7e5857bfa7e65e30a`
- Evidence Pack: `812194b9a89a5fa21e675200fcb4e0089666f1b6`

Historical fixture mappings inside those consumer repositories are not modified
by this branch.

## Accepted interface findings

The repaired Control Plane `BoundedRecordStore` keeps the same
`BoundedRunRecord` wire contract. Its accepted persistence repair adds
same-host Linux/local-filesystem transaction protection: a stable sidecar flock,
reload-inside-transaction, fsync, atomic replacement, and fail-closed handling
for unsupported filesystem/locking environments. This transaction protects the
record file only. It is not an atomic transaction spanning GAX transport,
Control Plane and the destination.

Replay `043830b5...` explicitly accepts both the preceding v2 Control Plane
revision and the persistence-repair revision. For `248d8996...`, Replay emits
a revision-specific Control Plane producer profile and records the selected
revision in both the producer profile and
`metadata.control_plane_revision`. Unsupported revision selections fail.

The GAX runtime continues to require caller-supplied resolver,
execution-policy factory, ObservationPolicy and trusted evaluation time. Public
executor imports remain `engine.control_plane_adapter` /
`engine.producer_contract`; no supported runtime import from `tests.*` is
introduced.

## Phase A qualification

`tests/test_control_plane_store_adoption.py` is explicitly **stage-level
evidence**, not full GAX pipeline qualification. It checks:

1. exact selected revisions;
2. repaired `BoundedRecordStore` construction on the CI local filesystem;
3. decision/effect/attempt identity persistence across a fresh store instance;
4. Replay acceptance and source attribution for `248d8996...`;
5. rejection of an unsupported Control Plane revision.

Existing full GAX/transport tests remain unchanged as the consumer-boundary
reproduction. They continue to assert original identities, no replacement
dispatch, prior-attempt uncertainty, exact redelivery, recovery semantics and
destination-state invariants.

## Consumer compatibility boundary

At the start of this branch, accepted ODES `226adb0e...` declares producer-v2
pins for Control Plane `2ea9528e...` and Replay `274543f1...`.
Accepted Evidence Pack `812194b9...` likewise classifies v2 using Control Plane
`2ea9528e...` and advertises Replay `274543f1...`.

Worker 8 and Worker 10 are independently updating those consumers. Their
unaccepted branches are not consumed here. Phase B requires accepted merge SHAs
whose reviewed changes explicitly include the new persistence/Replay mapping.

If current accepted consumers reject the branch's truthful selected revision,
that rejection is retained as qualification evidence. It must not be mocked,
made optional, or bypassed.

## Semantics preserved

- original decision/effect and executor-attempt identities remain producer-owned;
- Control Plane attempts and executor attempts remain separate namespaces;
- prior-attempt absence does not create replacement-dispatch permission;
- `retry_eligible=false` remains required for the repaired absence path;
- `recovery_denied_derivative` remains distinct from historical authorized execution;
- Replay's denied-execution/effect contradiction rejection remains intact;
- evidence-only reads/exports do not authorize or execute an effect;
- no distributed delivery/finality, independent effect verification, business-intent deduplication or cross-system atomicity is inferred.

## Validation record

CI run `37581905307` executed exact pinned checkouts for the Phase A branch.

### Focused stage-level checks

Command:

```sh
pytest -q tests/test_control_plane_store_adoption.py
```

Results:

- Python 3.11: **passed**.
- Python 3.12: **passed**.

These results establish only the repaired local Control Plane store/restart
behavior and Replay revision selection/source attribution described above.

### Full suite / consumer-boundary reproduction

Command:

```sh
pytest -q tests
```

Python 3.12 reached **44 passed, 60 failed**. The failures converge on the
accepted ODES consumer at
`upstream/odes/src/odes/exporter.py::_run_replay_validation`, which reads
`bundle.metadata.control_plane_revision` and rejects
`248d899634d9db3518e831bc7ab568a48733f825` because accepted ODES
`226adb0e3cde5377ac9db6f7e5857bfa7e65e30a` permits only its historical and
previous producer-v2 Control Plane revisions. The exact error is:

```text
odes.exporter.ExportError: unsupported Control Plane compatibility revision
```

This is the required non-bypassed smallest integration blocker. The failures
occur after Replay has truthfully emitted the selected Control Plane revision
and before ODES export/recipient validation can complete.

Python 3.11 completed the focused checks successfully; its full-suite step was
cancelled after the matrix failure. The demonstration command was skipped
because the full test gate failed. Evidence Pack persistence-revision
qualification was therefore not reached through the complete public path; it is
still independently blocked by its accepted `V2_REVISIONS` mapping, which
declares Control Plane `2ea9528e...` and Replay `274543f1...`.

No validation was weakened, mocked or marked optional. No ODES or Evidence Pack
code was changed.

### Qualification state

Completed now:

- repaired store construction on supported CI local filesystem;
- record persistence across a fresh store instance;
- decision/effect/Control Plane attempt identity persistence;
- Replay acceptance of `248d8996...`;
- Replay metadata and producer attribution of the actual selected revision;
- unsupported Control Plane revision rejection;
- preservation of public executor imports and explicit resolver/policy/time
  inputs.

Blocked pending accepted consumer merges:

- complete Transport → GAX → Control Plane → executor → Replay → ODES →
  successor qualification;
- retained Replay → Evidence Pack import/validate/render under the new revision
  mapping;
- therefore the requested full scenario matrix under the new dependency
  generation.

Existing accepted baseline tests continue to define the required semantic
assertions for no replacement dispatch, pending original effect after prior
absence, `retry_eligible=false`, recovery denial separation, reconciliation,
historical artifact immutability and contradictory-lineage rejection. They are
not claimed requalified under the new generation until the consumer gate is
accepted.

## Phase B qualification

Accepted consumer merges were verified before selection:

- ODES: `0486b645e99c46d9cd16ca34b1ba7c653a6b3024`
- Evidence Pack: `de6b9e071df49fc3e0c1254d39b5c94cced554f0`

Only those current dependency selections and their CI/documentation references
were advanced. Earlier ODES `226adb0e...` and Evidence Pack `812194b9...`
remain historical qualification mappings and are not relabelled.

The focused Phase B gate executes:

```sh
pytest -q tests/test_control_plane_store_adoption.py
pytest -q tests/test_gax_public_runtime_artifacts.py tests/test_observation_repair.py tests/test_recovery_export_separation.py
```

At code head `201b8e1f7b43e6482a0bc57b2b466c5c6648de5e`, Actions run
`37613785684` produced:

- Python 3.11: **4 passed** stage-level adoption checks; **36 passed** focused
  public-path/recovery checks; **104 passed** complete suite; demo **passed**.
- Python 3.12: **4 passed** stage-level adoption checks; **36 passed** focused
  public-path/recovery checks; **104 passed** complete suite; demo **passed**.
- exact pin verification: **passed** on both jobs.

The complete suite therefore exercises the accepted public path:

`Transport -> GAX assessment -> Control Plane -> actual executor -> Replay -> ODES -> successor`

and retained Replay through Evidence Pack import/validate/traceable render.

The exercised regressions preserve:

- one bound destination effect for authorized success;
- duplicate/redelivery returning retained original artifacts without a second effect;
- lost acknowledgement and restart under original decision/effect identities;
- derivative-only post-effect evidence recovery with no replacement dispatch;
- prior-attempt absence as pending/unresolved with `retry_eligible=false`;
- unresolved partial delivery;
- changed-authority pre-observation recovery denial as
  `recovery_denied_derivative`, separate from historical authorized execution;
- denied recovery carrying reconciliation through the normal reconciliation
  derivative so current observation evidence is retained;
- distinct Control Plane and executor attempt namespaces;
- fail-closed contradictory attribution, operation binding and retained-artifact
  tampering;
- evidence-only import/export/redelivery without destination or Control Plane
  mutation;
- ODES content integrity without upgrading it to producer authentication or
  current institutional authority.

A Phase B test-only correction changed the Evidence Pack assertion from the
previous selected Replay revision to the accepted persistence generation and
asserts both old and new revisions in `supported_revision_sets`. Historical
mappings were not rewritten.

## Remaining boundary

No component blocker remains in this bounded workstream. Governor review/merge
is still required.

The repaired Control Plane protects individual same-host record transactions.
This qualification does **not** establish atomicity across the transport store,
GAX exchange store, Control Plane record store and destination. It also does not
establish distributed delivery guarantees, remote finality, independent
real-world effect verification, production authentication, or business-intent
deduplication.
