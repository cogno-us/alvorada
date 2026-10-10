<!-- cognous-banner:start -->
```text
──────────────────────────────────────────────────
   __________  _______   ______  __  _______
  / ____/ __ \/ ____/ | / / __ \/ / / / ___/
 / /   / / / / / __/  |/ / / / / / / /\__ \
/ /___/ /_/ / /_/ / /|  / /_/ / /_/ /___/ /
\____/\____/\____/_/ |_/\____/\____//____/
            COGNOUS GOVERNED EXCHANGE
       g o v e r n e d   b y   d e s i g n
  github.com/cogno-us/cognous-open-control-stack
──────────────────────────────────────────────────
```
<!-- cognous-banner:end -->

# Cognous Governed Exchange

**Governed exchange and continuity for a bounded synthetic workflow.**

## Overview

An experimental GAX/IMX reference that connects governed messages, recipient assessment, bounded execution and retained evidence. It is separate from the constitutional authority repository: this workbench implements exchange behavior and local transport, not an institution or a constitution.

**Implementation and selection (2026-10-10):** The current [hub component lock](https://github.com/cogno-us/cognous-open-control-stack/blob/main/component-lock.json) selects GAX `a1cbc7b28f702283b0e4f3192bb43e4a9e618ebf`. This repository has separately accepted optional generation-fence source `814927dac65a2078d5efee6bbf07c46091844822`; that source is **not automatically selected or enabled** by the hub. The earlier `9984d9011568ccdf3d562fa9760ad41368947b34` and hub `5737267d...` are **historical qualification references**, retained below for provenance. [Release status](https://github.com/cogno-us/cognous-open-control-stack/blob/main/docs/release-status.md) governs release claims; deployment trust remains [HOLD](https://github.com/cogno-us/cognous-stack-orchestrator/issues/30).

## Purpose and intended users

Message receipt, understanding, acceptance, authorization and execution are different events. A recipient must preserve those distinctions when messages are duplicated, acknowledgements are lost or a workflow resumes under changed authority.

Engineers can inspect the reference contracts and examples; enterprise architecture, security and governance reviewers can examine the boundary and evidence. Evaluate this component for its named responsibility rather than as a complete governance platform.

## Key features

| Capability | Implemented or specified responsibility |
|---|---|
| **GAX assessment** | Assess message structure, supported profile, identity binding, references, freshness and permitted handling. |
| **Local durable transport** | Queue, deliver and redeliver messages while retaining their association with recipient outcomes. |
| **Public runtime integration** | Invoke supported Control Plane and executor interfaces with caller-supplied resolver, execution policy, observation policy and trusted time. |
| **Retained artifacts** | Expose original Replay, ODES and IMX artifacts with identities and content commitments. |
| **Recovery and continuity** | Reconcile original effects, retain derivative lineage and preserve predecessor commitments without making successor loading an execution grant. |

## How it works

LocalDurableTransport delivers a synthetic refund message to AcceptedGaxRecipientAdapter. GAX assesses the message and routes the Manifest-bound proposal through the trusted runtime. Redelivery returns the retained original artifacts when available. If evidence was never retained after an effect, evidence-only recovery creates an explicitly derived record without issuing a replacement effect.

A valid signature, chain inclusion, message receipt, reasoning instruction or evidence-package digest does not authorize execution. Institutional authority must be supplied and evaluated through the appropriate trusted boundary.

## Example: two generations and a late result

Suppose a synthetic refund conversation has an authoritative local lineage head **generation 5**, while a delayed worker still claims **generation 4**.

1. The optional `LineageGenerationFence` reads the authoritative local generation from the same SQLite lineage store under its local commit lock.
2. The version-4 effect is **refused before local commit** as `superseded_lineage_generation`. A minimal record binds the claimed effect, expected and authoritative generations, and refusal reason; ordinary refusal logs do not need the raw payload.
3. The persisted refusal counter survives restart. Once the configured record limit is reached, later refusals increment bounded suppression counters instead of expanding individual logs indefinitely.
4. A generation-5 claim may proceed to its **synthetic local destination** only if independently supplied institutional authorization and other execution prerequisites are satisfied. Matching a lineage generation does **not** grant those permissions.

The accepted source implementation is [`generation_fence.py`](experiments/odex_gax_imx_reference/generation_fence.py), with [source-owned negative tests](tests/test_generation_fence.py). The [O6-Q4 orchestration qualification](https://github.com/cogno-us/cognous-stack-orchestrator/blob/main/development/acceptance/o6-q4/README.md) exercises it alongside actual pinned synthetic Control Plane and Runtime interfaces, but does not establish distributed atomicity or production settlement. A stale message receipt cannot authorize a replacement refund, and a timeout or observed absence never makes a blind retry safe.

## Getting started

For a complete environment with exact dependencies, follow the [hub developer quickstart](https://github.com/cogno-us/cognous-open-control-stack/blob/main/docs/quickstart.md). The [exchange demo](experiments/odex_gax_imx_reference/README.md) has separate prerequisites and writes an illustrative JSON artifact; it is not the transported, effect-producing qualification. Consult the [transport profile](experiments/governed_message_transport/profile.md) before treating receipt as any stronger state.

## Evidence and supported scope

The hub selects accepted merge `9984d9011568ccdf3d562fa9760ad41368947b34`: bounded refund exchange 0.1.0, retained artifacts 1.1.0 and recipient result 1.0.0. The public runtime uses supported executor interfaces, not upstream test helpers. Authority Context comes from [the separate constitutional repository](https://github.com/cogno-us/cognous-institutional-governance).

The accepted [hub persistence-generation evidence](https://github.com/cogno-us/cognous-open-control-stack/blob/5737267d94d2b445735c95e8480a31de73a2abe8/examples/control-plane-store-adoption/qualification-summary.json) records 915 Python tests in each of two repetitions, 35 matrix entries satisfying their gates and 120 separate mocked OpenShell tests. Those are aggregate hub results, not a per-component test count or a claim of production readiness. Optional behavioral layers receive static checks only. The [support ledger](https://github.com/cogno-us/cognous-open-control-stack/blob/main/docs/release-status.md) separates implementation, execution and adoption.

## Limitations and deployment decisions

This is a bounded refund profile, not full GAX conformance, a production network or institutional authentication. Message IDs and receipt do not authorize actions. Current recovery can be denied while historical applied evidence remains true. Deferred Alvorada PR #2 is excluded from the hub; no pending branch supplies supported behavior.

Review original artifacts and their exact source revisions before extending a claim to a new environment. New dependencies, authority sources, destinations or enforcement mechanisms need their own compatibility and qualification. A passing reference case is not a certification of an enterprise deployment.

## Repository guide

Use these sources for details; their historical checkpoints retain the status and scope of the work they recorded:

- [docs/gax-runtime-artifact-interface.md](docs/gax-runtime-artifact-interface.md)
- [experiments/odex_gax_imx_reference/profile.md](experiments/odex_gax_imx_reference/profile.md)
- [experiments/governed_message_transport/profile.md](experiments/governed_message_transport/profile.md)
- [experiments/odex_gax_imx_reference/mapping.md](experiments/odex_gax_imx_reference/mapping.md)

For a nontechnical introduction, read the [business overview](collateral/business-collateral.md) and [one-page overview](collateral/one-page-overview.md). Both describe this component's role and evidence limits, not additional runtime features.

## Contributing and attribution

Propose focused changes through repository issues and pull requests. Keep evidence-linked claims, preserve historical records and separate proposed features from accepted implementation.

See [LICENSE](LICENSE) and [attribution](NOTICE) for the existing terms and third-party scope. Developed by [Cognous](https://cogno.us); no licensing change is part of this documentation update.

---

## Bibliography

Selected external sources from the October 2026 research review. These inform evaluation questions; they do not establish Cognous implementation, adoption, conformance or production qualification.

- [Alexander Barrett. *Boundary Blindness Under Artificial Intelligence: Early Cross-Industry Findings on the Missing Decision-Evidence Layer* (2026)](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=7210798). Working paper on carrying the basis for reliance across organizational boundaries; proposed architecture, not a validated interoperability guarantee.
- Jonathan Chadbourne / JCEE Labs. *When a Timeout Is Not a Failure: Authority, Evidence, and Recovery in Consequential AI Execution*. Technical Note 001, public release v0.1.1 (6 October 2026). Technical note on uncertain outcomes and recovery. An original public URL has not been verified; no substitute or private copy is linked.
- [OWASP GenAI Security Project. *State of Agentic AI Security and Governance*, version 2.01 (June 2026)](https://genai.owasp.org/resource/state-of-agentic-ai-security-and-governance/). Security synthesis covering agent identity, delegated permissions, tool access and containment.

See the [research bibliography](https://github.com/cogno-us/cognous-open-control-stack/blob/main/docs/research-bibliography.md) for review scope and source-verification limits.

## Cognous stack components

[Stack hub](https://github.com/cogno-us/cognous-open-control-stack) · [Selected pins](https://github.com/cogno-us/cognous-open-control-stack/blob/main/component-lock.json) · [Evidence and limits](https://github.com/cogno-us/cognous-open-control-stack/blob/main/docs/release-status.md)

Component links are navigation, not a requirement to install every component. The hub lock determines its supported integration.

| Component | Responsibility |
|---|---|
| [Cognous Action Manifest](https://github.com/cogno-us/cognous-action-manifest) | Declare the action before evaluating permission |
| [Cognous Control Plane](https://github.com/cogno-us/cognous-control-plane) | Evaluate proposals against authority and preserve the decision record |
| [Cognous Replay Bundle](https://github.com/cogno-us/cognous-replay-bundle) | Reconstruct what the retained records support |
| [Cognous Governance Evidence Pack](https://github.com/cogno-us/cognous-governance-evidence-pack) | Turn traceable runtime records into reviewable governance evidence |
| [Open Decision Evidence Standard](https://github.com/cogno-us/open-decision-evidence-standard) | Portable decision evidence across system and organizational boundaries |
| [Cognous Execution Runtime](https://github.com/cogno-us/cognous-execution-runtime) | Constrained execution beneath independent current authorization |
| [Cognous Evidence Attestation](https://github.com/cogno-us/cognous-evidence-attestation) | Verify issuer signatures under explicit trust assumptions |
| [Cognous Evidence Registry](https://github.com/cogno-us/cognous-evidence-registry) | A local blockchain reference for claims, evidence commitments and lifecycle history |
| [Portable Reasoning Protocol v1.0](https://github.com/cogno-us/portable-reasoning-protocol) | Portable instructions for evidence-bounded reasoning |
| [Research Intelligence Protocol v1.0](https://github.com/cogno-us/research-intelligence-protocol) | Disciplined discovery and cross-domain abstraction, kept separate |
| [TFA Protocol (S43)](https://github.com/cogno-us/truth-freedom-agency-protocol) | Truth · Freedom · Agency |
| [Cognous Institutional Governance](https://github.com/cogno-us/cognous-institutional-governance) | Alvorada: authority, challenge and correction for institutions |

## Repository locations

See the [repository rename map and compatibility notes](https://github.com/cogno-us/cognous-open-control-stack/blob/main/docs/repository-renames.md) for current component URLs. Existing package names, schema identifiers and retained producer identities are unchanged.

## Merged producer and consumer compatibility

An explicit `merged-producers-v1` profile selects the accepted newer Control Plane, execution runtime, Replay, Evidence Pack and ODES revisions. The historical profile remains the default. See [exact revisions, qualification and limits](docs/merged-consumer-chain.md). This profile does not enable atomic-claim or refund-intent execution.
