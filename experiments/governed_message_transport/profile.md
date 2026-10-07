# Governed Message Transport Profile 0.1.0

Status: experimental local reference.

Profile identifier: `urn:cognous:profiles:governed-message-transport:0.1.0`

## Purpose

This package adds a bounded durable delivery interface around an already-governed message. It deliberately does not redefine ODEX-GAX/IMX message semantics, institutional authority, Control Plane authorization, constrained execution, Replay reconstruction, ODES decision evidence, or independent verification.

The accepted GAX/IMX source baseline is `cogno-us/cognous-governed-exchange@9ad378145d326799e3209136e47e82d66c6f69af`. The open follow-up PR #2 is not a dependency.

## Transport envelope

The transport envelope is versioned separately from the governed message. It carries:

- stable governed `message_id`;
- unique `delivery_attempt_id`;
- configured sender and recipient endpoint/identity references;
- conversation and correlation references;
- the governed message content commitment;
- canonicalization version;
- creation and expiry;
- retry linkage;
- the governed message verbatim.

The envelope cannot modify governed-message content without invalidating the commitment. Retry attempts retain the same governed message identity and content commitment and receive a new transport attempt identity.

## Identity and routing

Routes are resolved only from explicit trusted local configuration. A caller-supplied sender string and a message-provided endpoint do not create routing authority.

The local fixture treats configured endpoint identity as trusted configuration only. `configured_identity_authenticated=false` explicitly records that this is not cryptographic network authentication. Transport credentials and successful delivery never create an Alvorada grant or Control Plane authorization.

## Recipient handling

The `AcceptedGaxRecipientAdapter` calls the accepted GAX assessment interface. Only `PROPOSE` and `REQUEST` may continue from assessment into the accepted GAX `run_exchange` path. `REPORT`, `REFUSE`, and `NOT_UNDERSTOOD` remain informational and cannot trigger an effect through transport.

External effects, if any, remain behind the pinned Control Plane and constrained executor. The transport layer has no direct effect-dispatch path.

## Durable delivery semantics

The local adapter uses separate SQLite sender and recipient stores.

Sender lifecycle:

`QUEUED -> IN_FLIGHT -> DELIVERED`

or:

`QUEUED/IN_FLIGHT -> PENDING_RETRY -> ... -> UNRESOLVED`

or:

`QUEUED/IN_FLIGHT -> TERMINAL_REJECTED`

Recipient receipt is inserted in a durable SQLite transaction before a durable-receipt acknowledgement is emitted. Duplicate recipient delivery is suppressed by `message_id + content_commitment`. Reuse of the same message ID with changed content is rejected.

All delivery attempts are append-preserved in the attempts table. Retry uses bounded exponential backoff and distinct delivery-attempt IDs. Expired, unsupported, wrong-recipient, untrusted-sender and content-conflict cases terminate without recipient execution.

## Acknowledgement semantics

`DURABLE_RECEIPT` acknowledges only durable inbox receipt of the exact governed message commitment. It does **not** mean:

- institutional authorization;
- execution;
- successful external effect;
- destination observation;
- independent verification.

`TERMINAL_REJECTION` acknowledges a transport-level refusal and its reason. It does not establish a substantive governance denial unless the recipient producer separately emits that decision.

## Consistency boundary

The exact consistency boundary is the local SQLite transaction that inserts the recipient inbox record before acknowledgement. Sender outbox/attempt state and recipient inbox state are separate durable transactions.

Therefore the reference provides at-least-once delivery with duplicate suppression and recovery. It does **not** claim distributed exactly-once delivery or atomicity across sender, recipient, Control Plane, executor, or external destination.

A lost acknowledgement can cause redelivery. Redelivery reuses the governed message identity and commitment, receives a new transport attempt ID, and is suppressed at the recipient if already durably received.

## Recovery boundary

Transport recovery never invents a replacement decision/effect record. Recipient outcomes are retained with explicit producer-returned `decision_id`, `effect_id`, and executor attempt references when available. Transport attempts use a separate namespace.

If recipient receipt exists without a retained recipient outcome, redelivery holds with `durable_receipt_without_recipient_outcome`; transport does not rerun execution from incomplete evidence.

## Downstream reconstruction mapping

| Transport field | Downstream meaning | Loss / limitation |
|---|---|---|
| `message_id` | stable governed message identity | none |
| `content_commitment` | commitment to exact governed message | local SHA-256 canonical JSON; no issuer signature |
| `transport_attempt_id` | one delivery attempt | not an executor attempt ID |
| `acknowledgement_id` | receipt/rejection evidence | not authorization or effect proof |
| `producer_refs.decision_id` | recipient-provided Control Plane reference | absent when recipient produced none |
| `producer_refs.effect_id` | recipient-provided effect reference | absent when recipient produced none |
| `producer_refs.executor_attempt_id` | recipient-provided executor attempt | distinct namespace; absent when unavailable |
| inbox receipt timestamp | local durable receipt time | not independent observation |
| transport log | delivery reconstruction | cannot independently verify external effect |

Unsupported fields remain explicit: cryptographic network authentication, distributed exactly-once delivery, and independent effect verification.

## Trust assumptions

- SQLite durability is local-process/storage durability only.
- Fixture route configuration is trusted.
- Configured transport identity is not cryptographically authenticated.
- The accepted GAX/IMX adapter and its pinned dependencies are relied upon for recipient assessment and execution gating.
- Institutional authority remains external to transport.
- No production credentials, public service, public-chain write, or paid service is used.


## Correction pass: temporal and recovery rules

The trusted delivery instant is supplied explicitly for each new recipient
assessment. Constructor-time values are not authorization time. The accepted
GAX assessment and `run_exchange` receive the trusted delivery instant, so
Control Plane decision and effect-time revalidation occur against current
delivery time. Historical transport duplicates with a retained recipient
outcome are observed from durable recipient state and do not rerun assessment or
renew authorization.

The local clock policy requires timezone-aware trusted, message and envelope
timestamps. With the default zero-skew policy:

- envelope and governed-message creation times cannot be in the future;
- envelope delivery-attempt creation cannot precede governed-message creation;
- governed expiry must be after governed creation;
- envelope expiry must equal governed-message expiry;
- governed expiry is checked directly against trusted delivery time;
- duplicated `message_id` and `conversation_id` fields must exactly match the
  governed message before recipient processing.

A durable inbox receipt without retained recipient outcome is
`DURABLE_RECEIPT_UNRESOLVED`, not terminal rejection and not successful
processing. Recovery does not rerun recipient execution when the retained
outcome boundary is missing.

Inbound route selection matches the complete configured transport binding:
sender endpoint, sender identity reference, recipient endpoint and recipient
identity reference. Multiple configured senders may share one recipient.
Duplicate complete bindings are rejected as ambiguous configuration.
