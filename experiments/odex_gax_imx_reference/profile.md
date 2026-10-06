# Experimental Profile: ODEX-GAX-IMX Refund Exchange 0.1.0

Profile identifier: `urn:cognous:profiles:odex-gax-imx-refund-exchange:0.1.0`

Status: experimental implementation profile.

This profile extracts bounded, reusable exchange and continuity semantics from ODEX-GAX 0.3 and ODEX-IMX 0.1 for a single local synthetic refund exchange. It is not a full GAX profile, not an ODES conformance claim, and not a W3C or regulator-recognized specification.

## Version resolution

- ODEX-GAX design input: 0.3 Working Draft.
- ODEX-IMX design input: 0.1 bootstrap / discussion profile.
- This implementation profile: 0.1.0.

Mixed GAX 0.2/0.3 identifiers in draft examples are treated as draft artifacts. This implementation accepts only its own profile identifier and rejects downgrade attempts.

## Required message fields

- `message_id`
- `conversation_id`
- `profile`
- `protocol_version`
- `message_type`
- `created_at`
- `expires_at`
- `sender`
- `recipient`
- `purpose`
- `requested_action`
- `operation_commitment`
- `authority_refs`
- `evidence_refs`
- `content_digest`

Narrative content is untrusted data. It cannot expand the structured operation, change recipient identity, or create authority.

## State machine

`RECEIVED -> STRUCTURE_VALIDATED -> PROFILE_SUPPORTED -> IDENTITY_BOUND -> REFERENCES_RESOLVED -> AUTHORITY_ASSESSED -> ACCEPTED_FOR_ASSESSMENT -> AUTHORIZED -> EXECUTION_ATTEMPTED -> DESTINATION_OBSERVED -> REPORTED`

Failure outcomes include `REFUSED`, `NOT_UNDERSTOOD`, `EXPIRED`, `DUPLICATE`, `CONFLICT`, and `UNRESOLVED_PARTIAL`.

## Boundary rules

- A sender declaration is never self-validation.
- Capability is not authority.
- Receipt is not task acceptance.
- Task acceptance is not operation authorization.
- Authorization is not execution.
- Execution attempt is not destination observation.
- Destination observation is not independent verification.
- Continuity loading is not role assignment, freshness proof, or authorization renewal.
