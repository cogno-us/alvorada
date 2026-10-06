# GAX transport result 1.0.0

The supported GAX/executor path consumes public runtime interfaces only. It does
not import `tests/test_safe_executor.py` or any upstream test module.

## Executor dependency

GAX consumes Moltbot executor producer profile:

- profile: `cognous.moltbot-safe.executor`
- profile version: `1.0.0`
- proposed implementation revision:
  `894e1c115cb91229c474a906c51ea9af7999e675`

The repository revision identifies the implementation. The producer profile
version identifies emitted record semantics. Source-asserted provenance is not
independent verification.

Runtime execution requires a caller-supplied trusted authority resolver. GAX
does not manufacture authority from incoming proposal fields. Synthetic
resolver construction exists only in explicitly named demonstration/test
fixtures.

## Transport result

GAX emits and durably retains:

- result profile: `cognous.gax.transport-result`
- result profile version: `1.0.0`
- original Reconstruction Bundle;
- ODES export and recipient-validation result where produced;
- IMX successor packet;
- decision/effect/executor-attempt identities;
- executor producer revision/profile version;
- artifact commitments.

The transport recipient inbox retains that result together with assessment,
execution and producer references. Exact duplicate delivery returns the
retained artifacts rather than recreating them.

## Interrupted retention

If the effect was committed and executor/Control Plane source records were
durably retained but the original artifact set was not, the original artifacts
remain unavailable. Recovery may create a distinct derivative with:

- `original_artifacts=false`;
- `retention_state=complete_regenerated_derivative`;
- lineage `regenerated_from_retained_source_records`;
- `replacement_effect_executed=false`.

Recovery must not execute a replacement effect to regenerate evidence.

Unsupported result/profile versions, changed artifact commitments, changed
message content or contradictory producer bindings fail closed.

## Legacy

Historical messages that contain only producer references are not relabeled as
having retained originals. Existing stores are migrated by adding the nullable
transport artifact column/table; absence remains absence.
