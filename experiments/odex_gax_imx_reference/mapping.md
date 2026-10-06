# Field Mapping and Loss Register

This document maps actual pinned producer artifacts into the experimental GAX/IMX reference profile.

## Manifest v1.1

| Source path | Target field | Transformation | Meaning | Loss |
|---|---|---|---|---|
| `manifest_id` | `evidence_refs[].manifest_id` | copied | Declared operation inventory identity | Manifest does not itself authorize execution |
| `manifest_version` | `evidence_refs[].manifest_version` | copied | Manifest contract version | Distinct from GAX profile version |
| `actions[].action_id` | `requested_action.action_id` | selected from proposal | Declared action | Other manifest actions are not executed |
| `actions[].authority_required` | assessment input | boolean/readable | Whether authority is required | Requirement is not grant |
| `actions[].review_requirement` | assessment input | preserved as evidence | Review requirement declaration | Does not prove human review |
| `actions[].reliance_requirement` | assessment input | preserved as evidence | Reliance requirement declaration | Does not prove recipient reliance |

## Control Plane / Replay retained records

| Source path | Target field | Transformation | Meaning | Loss |
|---|---|---|---|---|
| `records[record_type=runtime_proposal].data` | `requested_action` + `operation_commitment` | canonical SHA-256 over proposal payload/action fields | Exact proposed operation | Does not expose private policy evaluation beyond retained fields |
| `runtime_decision.data.result` | `authority_assessment.status` | normalized `authorized/held/denied` | Current retained decision outcome | Does not create present validity without revalidation |
| `runtime_decision.data.binding.proposal_commitment` | `authority_assessment.proposal_commitment` | copied and checked | Binds grant to proposal | Missing binding blocks execution |
| `runtime_decision.data.binding.grant_revision` | `authority_assessment.grant_revision` | copied | Grant revision | Does not prove future freshness |
| `execution_envelope.data.operation` | `execution_boundary.operation` | canonical digest check | Operation passed to executor | Only present when execution occurred |
| `execution_result.data` | `execution_boundary.result` | copied | Executor result / attempt relationship | Attempt status is not destination observation |
| `destination_attempt.data.attempt_id` | `attempts.executor[]` | namespace `destination:` | Executor-attempt identity | Separate from Control Plane attempts |
| `control_plane_attempt_transition.data.attempt_id` | `attempts.control_plane[]` | namespace `control:` | Control Plane transition identity | Separate from executor attempts |
| `effect_observation.data.state` | `destination_observed` | normalized | Observed destination state | Not independent verification |
| `destination_effect.data` | `destination_effects[]` | copied with digest checks | Retained effect facts | A retained fact is not external certification |
| `reconciliation.data` | `reconciliation[]` | copied | Restart/recovery facts | Does not silently erase lost acknowledgement |

## Governance Evidence Pack

The evidence pack is review material only. It may be referenced as `review_material_ref`, but it is not a source of execution facts and cannot override retained producer records.

## ODES

ODES carries portable decision evidence. This reference records an ODES package reference and digest when supplied. ODES informational inspection is separate from authenticated reliance. A content digest does not authenticate issuer identity.

## Unsupported semantics

- effect IDs are retained as profile-specific execution facts, not general GAX core fields
- separate Control Plane and executor attempt namespaces are preserved but not standardized as general GAX
- reconciliation and restart behavior is profile-specific
- partial delivery is represented as unresolved continuity state
- independent real-world effect verification is unavailable
- BitRep and The Index are optional evidence interfaces, not required dependencies
