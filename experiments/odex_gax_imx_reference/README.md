# ODEX-GAX / IMX Refund Exchange Reference 0.2.0

This directory contains a bounded experimental reference implementation for a single local synthetic governed exchange:

`PROPOSE bounded refund -> recipient assessment -> Control Plane decision evidence -> Moltbot-style bounded execution evidence -> Replay-style outcome reconstruction -> ODES evidence reference -> IMX successor packet`

It is **not** full ODEX-GAX conformance, W3C recognition, a production network protocol, an authentication system, deployment approval, certification, compliance advice, or proof of independently verified real-world effects.

## Source status

Historical Alvorada material remains in `titanicprime/alvorada-archive` and is not modified here. The supplied ODEX-GAX 0.3 Working Draft and ODEX-IMX 0.1 bootstrap materials are used as design inputs only. They do not grant runtime authority.

## Implemented profile

Profile identifier: `urn:cognous:profiles:odex-gax-imx-refund-exchange:0.1.0`

Implemented message types:

- `PROPOSE`
- `REQUEST`
- `REPORT`
- `REFUSE`
- `NOT_UNDERSTOOD`

Implemented recipient-assessment stages:

- structure
- supported profile
- identity binding through a local trusted registry
- reference resolution
- authority assessment from retained Control Plane decision facts
- freshness and expiry
- permitted handling

Implemented delivery semantics:

- message receipt is separate from understood/accepted/authorized/executed/observed/verified
- identical redelivery is idempotent
- message ID reuse with changed content rejects
- new message ID replay cannot duplicate an existing effect ID
- lost acknowledgement with applied destination state is reconciled, not re-executed
- partial delivery remains unresolved

Implemented IMX continuity semantics:

- successor packets preserve predecessor identity and commitment
- successor loading alone creates no effect
- stale or rolled-back predecessor histories fail closed
- omitted information must be reported as loss
- current authority must be resolved again before any new external effect

## Unsupported and deferred

- production identity, credentials, key custody, public signatures, public chains, authenticated institutional resolvers
- real network transport
- scheduler/fleet orchestration
- GAX cognitive orchestration, social-model inference, crosslingual alignment and coordination-market profiles
- direct writes to ODES, BitRep or The Index
- independent real-world effect verification

## Demo

```bash
python -m experiments.odex_gax_imx_reference.demo \
  --manifest upstream/manifest/examples/refund_integration_v1_1.manifest.json \
  --replay upstream/replay/examples/bounded_success_reconstruction_v0_2.json \
  --out /tmp/gax-imx-demo.json
```

The demo writes a local JSON artifact only. It creates no external effect and renews no authorization.


## Runtime and evidence interfaces

The supported execution path imports Moltbot Safe only through public runtime
modules. It does not import `tests/test_safe_executor.py` or Control Plane test
fixtures. The caller must provide a trusted authority resolver; synthetic
authority construction is confined to `synthetic_fixture.py` for tests and
examples.

Successful exchanges retain a GAX Result Export `1.0.0` atomically with the
workflow association. The export contains the original Reconstruction Bundle,
ODES package and recipient-validation result, IMX successor packet, producer
identities and artifact commitments.

Exact redelivery returns the retained original export. If an effect was committed
but evidence export was interrupted, recovery may create a new
`complete_regenerated_derivative` export from retained Control Plane/executor
records. The derivative has its own Replay identity and explicit lineage and
never executes a replacement effect merely to regenerate evidence.

Governed Message Transport `0.2.0` durably retains the GAX Result Export with
the recipient outcome. A transport crash after recipient processing but before
outcome persistence recovers through GAX idempotent redelivery and retains the
existing original export when GAX had already committed it.
