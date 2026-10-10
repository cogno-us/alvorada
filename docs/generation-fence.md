# Optional lineage-generation effect fence

Status: experimental, opt-in GAX reference profile.

Profile: `urn:cognous:profiles:gax-lineage-generation-fence` version `0.1.0`.

## Boundary

The fence binds one already-authorized local effect claim to the integer
`state_version` of the accepted GAX/IMX lineage head for the same conversation.
Immediately at the local destination commit boundary it opens
`BEGIN IMMEDIATE` on the exchange SQLite store, reads the authoritative head,
and calls the destination commit only while that write lock is held.

A v3 effect claim therefore fails closed if v4 is already authoritative. A head
update that acquires the same SQLite write lock first is observed before effect
commit. A head update that waits behind an in-progress accepted effect can become
authoritative only after that local commit completes.

This is a bounded same-store serialization property. It is **not** distributed
destination atomicity, institutional authority, a production effect guarantee,
or protection against a writer that bypasses the accepted exchange store.

## Refusal evidence and flood behavior

Before a superseded, missing-generation, or wrong-effect refusal returns, the
fence durably updates a fixed-size per-conversation counter row. Up to the
configured refusal cap it also appends minimal refusal rows containing only:

- refusal/profile identifiers;
- conversation and effect identifiers;
- claimed and authoritative generations;
- reason and timestamp.

Raw governed-message content, operation payloads and refused payload bytes are
not stored in these refusal records.

After the detail-row cap, refusal evidence is aggregated into the bounded
counter row. Counters saturate at a configured maximum rather than growing
without bound. Both detail rows and counters survive process restart.

If refusal persistence is disabled or cannot be verified, the fence raises a
persistence error and still does not call the destination.

## Runtime opt-in

`run_exchange(..., generation_fence=fence, lineage_generation=N)` wraps the
existing destination only for that call. Supplying one option without the other
is an error. Omitting both preserves the accepted default runtime path.

The fence does not create, refresh or reinterpret an institutional grant. The
existing Control Plane decision and effect identity remain authoritative for
permission; the generation fence is an additional local supersession guard.
