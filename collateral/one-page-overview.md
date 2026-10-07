# Alvorada Experimental Workbench — One-Page Overview

## Purpose

An experimental GAX/IMX reference that connects governed messages, recipient assessment, bounded execution and retained evidence. It is separate from the constitutional authority repository: this workbench implements exchange behavior and local transport, not an institution or a constitution.

## Problem

Message receipt, understanding, acceptance, authorization and execution are different events. A recipient must preserve those distinctions when messages are duplicated, acknowledgements are lost or a workflow resumes under changed authority.

## What It Provides

- **GAX assessment:** Assess message structure, supported profile, identity binding, references, freshness and permitted handling.
- **Local durable transport:** Queue, deliver and redeliver messages while retaining their association with recipient outcomes.
- **Public runtime integration:** Invoke supported Control Plane and executor interfaces with caller-supplied resolver, execution policy, observation policy and trusted time.
- **Retained artifacts:** Expose original Replay, ODES and IMX artifacts with identities and content commitments.

## Where It Fits

LocalDurableTransport delivers a synthetic refund message to AcceptedGaxRecipientAdapter. GAX assesses the message and routes the Manifest-bound proposal through the trusted runtime. Redelivery returns the retained original artifacts when available. If evidence was never retained after an effect, evidence-only recovery creates an explicitly derived record without issuing a replacement effect.

A valid signature, chain inclusion, message receipt, reasoning instruction or evidence-package digest does not authorize execution. Institutional authority must be supplied and evaluated through the appropriate trusted boundary.

## Evidence and Limits

The [accepted hub lock](https://github.com/cogno-us/cognous-open-control-stack/blob/5737267d94d2b445735c95e8480a31de73a2abe8/component-lock.json) selects this component at `9984d9011568ccdf3d562fa9760ad41368947b34`. Read the component's [README](../README.md) for version-specific acceptance and the [hub support ledger](https://github.com/cogno-us/cognous-open-control-stack/blob/main/docs/release-status.md) for the executed scope. Component acceptance is not automatic adoption of newer revisions or production qualification.

This is a bounded refund profile, not full GAX conformance, a production network or institutional authentication. Message IDs and receipt do not authorize actions. Current recovery can be denied while historical applied evidence remains true. Deferred Alvorada PR #2 is excluded from the hub; no pending branch supplies supported behavior.

## Practical Next Step

Choose one bounded example and follow the [README](../README.md). Compare expected and observed results and retain uncertainty. The [business collateral](business-collateral.md) supplies evaluation questions and the component's wider context.

[Cognous](https://cogno.us) · [Source](https://github.com/cogno-us/alvorada) · [All stack components](https://github.com/cogno-us/cognous-open-control-stack). Existing licenses and notices apply.
