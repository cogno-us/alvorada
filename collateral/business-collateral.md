# Alvorada Experimental Workbench — Business Collateral

## 1. Executive Summary

An experimental GAX/IMX reference that connects governed messages, recipient assessment, bounded execution and retained evidence. It is separate from the constitutional authority repository: this workbench implements exchange behavior and local transport, not an institution or a constitution.

## 2. The Business Problem

Message receipt, understanding, acceptance, authorization and execution are different events. A recipient must preserve those distinctions when messages are duplicated, acknowledgements are lost or a workflow resumes under changed authority.

## 3. The Component in One View

| Capability | Practical role |
|---|---|
| GAX assessment | Assess message structure, supported profile, identity binding, references, freshness and permitted handling. |
| Local durable transport | Queue, deliver and redeliver messages while retaining their association with recipient outcomes. |
| Public runtime integration | Invoke supported Control Plane and executor interfaces with caller-supplied resolver, execution policy, observation policy and trusted time. |
| Retained artifacts | Expose original Replay, ODES and IMX artifacts with identities and content commitments. |
| Recovery and continuity | Reconcile original effects, retain derivative lineage and preserve predecessor commitments without making successor loading an execution grant. |

## 4. Who Should Evaluate It

Engineers can inspect the reference contracts and examples; enterprise architecture, security and governance reviewers can examine the boundary and evidence. Evaluate this component for its named responsibility rather than as a complete governance platform.

## 5. A Bounded Workflow

LocalDurableTransport delivers a synthetic refund message to AcceptedGaxRecipientAdapter. GAX assesses the message and routes the Manifest-bound proposal through the trusted runtime. Redelivery returns the retained original artifacts when available. If evidence was never retained after an effect, evidence-only recovery creates an explicitly derived record without issuing a replacement effect.

This is a reference use case. Adopting the format or running the example does not establish a production deployment, institutional acceptance or measured business benefit.

## 6. Relationship to the Stack

This component contributes **governed exchange and continuity for a bounded synthetic workflow**. The [Cognous Open Control Stack](https://github.com/cogno-us/cognous-open-control-stack) connects declared proposals, independent authority, constrained execution and retained review evidence. Components remain separately owned and versioned; the [selected lock](https://github.com/cogno-us/cognous-open-control-stack/blob/5737267d94d2b445735c95e8480a31de73a2abe8/component-lock.json) determines which revisions participate in the supported integration.

A valid signature, chain inclusion, message receipt, reasoning instruction or evidence-package digest does not authorize execution. Institutional authority must be supplied and evaluated through the appropriate trusted boundary.

## 7. What the Evidence Supports

The hub selects accepted merge `9984d9011568ccdf3d562fa9760ad41368947b34`: bounded refund exchange 0.1.0, retained artifacts 1.1.0 and recipient result 1.0.0. The public runtime uses supported executor interfaces, not upstream test helpers. Authority Context comes from [the separate constitutional repository](https://github.com/cogno-us/constitutional-governance-for-institutions).

The [accepted hub evidence](https://github.com/cogno-us/cognous-open-control-stack/blob/5737267d94d2b445735c95e8480a31de73a2abe8/examples/control-plane-store-adoption/qualification-summary.json) supports bounded synthetic integration at its exact pins. Aggregate test totals do not establish deployment benefit, compliance or independent real-world verification. The [support ledger](https://github.com/cogno-us/cognous-open-control-stack/blob/main/docs/release-status.md) distinguishes the standard reference, separate protected-worker campaign and unqualified production work.

## 8. What It Does Not Establish

This is a bounded refund profile, not full GAX conformance, a production network or institutional authentication. Message IDs and receipt do not authorize actions. Current recovery can be denied while historical applied evidence remains true. Deferred Alvorada PR #2 is excluded from the hub; no pending branch supplies supported behavior.

## 9. Evaluation Questions

- Which exact input, output and source revision will the receiving system consume?
- Who supplies trusted authority or evidence, and which assumptions remain outside this component?
- Can a reviewer trace the result to retained sources, including rejected or missing information?
- Which documented checks were actually executed in the intended environment?
- What deployment-specific work is required before relying on the result?

## 10. Why Open Reference Material Matters

Public formats, source, examples and evidence allow reviewers to inspect the claimed boundary and reproduce its checks. They also expose what has not been tested. Openness supports review; it does not substitute for independent assurance or operating responsibility.

## 11. Practical Next Step

Follow the [README](../README.md) and select one bounded use case. Inspect its inputs and expected outputs, reproduce the documented checks where prerequisites are available, and record failures and unresolved assumptions alongside passes. Use the [one-page overview](one-page-overview.md) for initial stakeholder orientation.

## 12. Status and Attribution

This collateral summarizes merged public material at repository `9984d9011568ccdf3d562fa9760ad41368947b34` and the accepted hub baseline `5737267d94d2b445735c95e8480a31de73a2abe8`. It does not anticipate pending branches. The protected-worker result applies only to its recorded Linux/bubblewrap fixture; live OpenShell and logical-intent prevention are not hub-supported at this snapshot.

[Cognous](https://cogno.us) · [Source repository](https://github.com/cogno-us/alvorada) · [Stack responsibilities](https://github.com/cogno-us/cognous-open-control-stack/blob/main/docs/architecture.md). Existing licenses and third-party notices remain controlling.
