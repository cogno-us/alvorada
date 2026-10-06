# GAX runtime and retained-artifact interface

## Supported runtime boundary

The supported GAX runtime imports the public Moltbot Safe runtime modules:

- executor adapter: `engine.control_plane_adapter.PinnedControlPlaneExecutor`
- producer export: `engine.producer_contract`
- executor producer profile:
  `urn:cognous:profiles:moltbot-safe-executor-producer`
- producer profile version: `1.0.0`
- accepted Moltbot repository revision:
  `1d308faf664c504b6e310db3c7a310153ef7b067`

The runtime requires a caller-supplied trusted authority resolver and a
caller-supplied execution-policy factory. It never constructs an authority grant
from incoming proposal content.

Synthetic authority and policy construction remain in
`experiments/odex_gax_imx_reference/synthetic_fixture.py` for examples/tests.

## Retained-artifact interface

GAX retained artifact export:

- profile: `urn:cognous:profiles:gax-retained-artifacts`
- version: `1.0.0`

The export retains:

- the original Reconstruction Bundle when successfully produced;
- the ODES package and recipient-validation result when successfully produced;
- the complete IMX successor packet;
- producer references for decision, effect and attempt identities;
- explicit content commitments for retained artifacts.

Transport recipient result:

- profile: `urn:cognous:profiles:governed-message-recipient-result`
- version: `1.0.0`

`LocalDurableTransport.retained_result(message_id)` returns the versioned
recipient result. `LocalDurableTransport.retained_artifacts(message_id)`
returns the retained GAX export or an explicit unavailable state. Consumers do
not need private SQLite access.

## Recovery states

- `original_complete`: the original generated artifact set was durably retained.
- `recovery_required`: an effect or recipient operation completed far enough to
  require evidence recovery, but a complete artifact set is not durably retained.
- `regenerated_derivative`: original artifacts were unavailable after an
  interruption; evidence was regenerated from retained producer/control records
  without executing a replacement effect. The derivative has its own identities
  and explicit lineage.
- `historical_artifacts_unavailable`: a legacy retained recipient record exists
  without the versioned artifact export.

A recoverable incomplete recipient result produces an unresolved/retryable
transport receipt rather than a false complete acknowledgement.

## Attempt namespaces

Executor/destination attempt identifiers and Control Plane reconciliation attempt
identifiers are not interchangeable. The Moltbot producer profile carries an
explicit `attempt_identity` namespace and separately retained
`control_plane_attempts` where applicable. GAX retains these producer
references without relabeling them.

## Consumer dependency status

Batch 2 does not modify consumers.

For integration testing this branch pins:

- Replay proposed head:
  `f63ce914504dd06813c4ccd199b0570dbd8dd427`
- ODES proposed head:
  `cba83a1c06f718a8afd76178f36e5cc15896347d`

The preserved Replay proposal currently recognizes producer profile 1.0.0 but
pins Moltbot repository revision
`054e92d12ccb0bc756ca6652f39fc13b51e05d9b`, the pre-merge feature head.
It does not yet accept the merged dependency revision `1d308faf...`.

Alvorada does not relabel the merged producer as the pre-merge revision.
Consumer acceptance of the merged Moltbot revision is therefore an explicit
downstream dependency for the complete GAX artifact pipeline.
