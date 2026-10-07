# GAX runtime and retained-artifact interface

## Supported runtime boundary

The supported GAX runtime imports the public Moltbot Safe runtime modules:

- executor adapter: `engine.control_plane_adapter.PinnedControlPlaneExecutor`
- producer export: `engine.producer_contract`
- executor producer profile:
  `urn:cognous:profiles:moltbot-safe-executor-producer`
- producer profile version: `2.0.0`
- accepted Moltbot repository revision:
  `177354e959cc78c59c1a776f018cfbfbf28c927b`

The runtime requires a caller-supplied trusted authority resolver and a
caller-supplied execution-policy factory and explicit ObservationPolicy.
The trusted timezone-aware evaluation time reaches the public executor; its
observation clock may be explicitly supplied for deterministic tests. Production
code does not inject a synthetic clock or observation policy. It never constructs an authority grant
from incoming proposal content.

Synthetic authority and policy construction remain in
`experiments/odex_gax_imx_reference/synthetic_fixture.py` for examples/tests.

## Retained-artifact interface

GAX retained artifact export:

- profile: `urn:cognous:profiles:gax-retained-artifacts`
- version: `1.1.0`

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

## Accepted observation-repair generation

See the [Batch 4C-E checkpoint](workstreams/gax-producer-v2-checkpoint.md) for exact pins and executed results. Replay 0.2.0, executor producer 2.0.0 and ODES implementation profile 0.2 are consumed through their public APIs. The base pder-v0.1 schema is unchanged.

Artifact export 1.1.0 adds an explicit `reconciled_derivative` state for new observations of the original effect. `AcceptedGaxRecipientAdapter.resume_original(message, delivery_time=...)` uses the exact retained owning CP attempt/store and accepted executor. It saves a separate recovery artifact and leaves original exports and transport receipts unchanged. It requires an unambiguous owning store and the accepted producer generation. Recovery is not automatic redelivery. It adds CP reconciliation evidence; unlike evidence-only reads, it is expected to change CP records. It does not authorize replacement dispatch. Current authorization checks may deny this executor path; effect-free historical observation under revoked authority is not newly qualified here.

When current authority denies `resume_original` before destination observation, the denial is represented as a `recovery_denied_derivative`, not as a replacement execution record. The branch is selected only when the accepted executor returns `status=denied` without per-call Control Plane reconciliation evidence. Its reconstruction/ODES/successor evidence remains the retained historical original, while lineage records the current recovery evaluation time, denial scope, the complete current recovery result and reason, no destination observation, no replacement dispatch, no renewed authorization, and no effect reexecution. This preserves historical applied effects or unresolved prior attempts without asserting that current denial erased the past. A denied result that does carry a new reconciliation is not collapsed into historical-only evidence: the normal reconciliation derivative preserves that new observation/rejection evidence. Replay's general denied-execution/effect contradiction remains unchanged.

The original `recover` path remains evidence-only: when originals were never produced, it regenerates explicit derivatives from retained records without changing CP or destination state. Exact redelivery returns originals. Historical 1.0.0 exports remain readable as retained originals; historical execution/checkpoint regeneration requires the original revision-pinned runtime and is explicitly refused by the new recovery path rather than relabeled.

Completeness and delivery are separate. An acknowledged real effect can have null validated observation and unresolved delivery. Accepted later applied evidence resolves delivery while retaining earlier rejection. Lost acknowledgement remains unknown in its original record even with applied observation; new reconciliation acknowledgements are separate and produce a mixed history. `observed_absent` does not confer retry permission. Control Plane and executor attempts retain their own namespaces.

Successor decision attribution remains null where the producer supplies none. No attribution is invented. Pending/unresolved state follows validated destination evidence, not reconstruction completeness or receipt labels. ODES recipient inspection remains unauthenticated with current authority unavailable under the reference policy.


## Control Plane persistence-adoption candidate

This branch selects the accepted same-host persistence repair
`cogno-us/cognous-agent-control-plane@248d899634d9db3518e831bc7ab568a48733f825`
and its accepted Replay compatibility
`cogno-us/cognous-agent-replay-bundle@043830b56595cecddfa65c064afd1c0b95e64792`.
The executor remains
`cogno-us/moltbot-safe@177354e959cc78c59c1a776f018cfbfbf28c927b`
with producer profile 2.0.0.

This is a dependency-generation change, not a retained-artifact wire/profile
change. GAX retained-artifact export remains 1.1.0. The repaired Control Plane
changes persistence mechanics for individual record transactions while keeping
the consumed `BoundedRunRecord` contract. Replay records the actual selected
Control Plane revision in its producer profile and metadata.

The immediately preceding Control Plane/Replay pair
`2ea9528eeb87e14ff10f05de06473122b9df540f` /
`274543f1cd7171784a923a8e37015017a0d8bc9d` remains historical compatibility
evidence and is not relabeled.

Phase B selects accepted persistence-compatible consumers:
`cogno-us/open-decision-evidence-standard@0486b645e99c46d9cd16ca34b1ba7c653a6b3024`
and
`cogno-us/cognous-agent-governance-evidence-pack@de6b9e071df49fc3e0c1254d39b5c94cced554f0`.
Earlier ODES/Evidence Pack revisions remain historical qualification mappings
and are not relabeled. Full-path qualification results are recorded in the
workstream checkpoint; dependency selection alone is not an end-to-end
readiness claim.
