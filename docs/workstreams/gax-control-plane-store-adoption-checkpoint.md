# GAX Control Plane store adoption checkpoint

Status: **Phase A preparation in progress; full consumer qualification not yet accepted.**

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
| ODES consumer | `cogno-us/open-decision-evidence-standard` | `226adb0e3cde5377ac9db6f7e5857bfa7e65e30a` | accepted producer-v2 consumer; does **not** yet declare CP `248d8996...` / Replay `043830b5...` |
| Evidence Pack consumer | `cogno-us/cognous-agent-governance-evidence-pack` | `812194b9a89a5fa21e675200fcb4e0089666f1b6` | accepted producer-v2 consumer; does **not** yet declare CP `248d8996...` / Replay `043830b5...` |
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

Focused checks are run before the full suite in CI. Final commands/results and
any exact consumer rejection are recorded here after the branch head executes.

## Phase B gate

Phase B is blocked until accepted ODES and Evidence Pack merge revisions are
available and verified to contain their reviewed persistence-compatibility
changes. This branch does not poll for or consume those pending branches.
