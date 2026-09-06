---
title: When a reply disappears.
description: Why Xenon records the operation result beside the mutation.
sidebar: false
---

# When a reply disappears.

Imagine a storage operation commits, then the connection disappears before Temporal receives the reply. The caller cannot infer from the connection error whether the operation happened.

Retrying needs more than another attempt. It needs a way to recognize the first one.

## Record the answer with the change

Xenon's partition owner looks up an operation ID and input digest. For a new operation, the handler applies its conditions and records the mutation and typed outcome in one transaction. A retry with the same identity and input can return that recorded outcome.

Select **Durable outcome lookup**, then **Serializable transaction**, in the view below to follow the mechanism.

<XenonArchitecture initialView="node" />

## Identity is part of correctness

An operation ID is not a general permission to reuse an old response. The input digest binds it to the original request. Reusing an identity with different input must fail instead of returning an unrelated result.

The outcome also carries its result family. That matters when one dispatcher serves several persistence interfaces: a saved namespace response cannot be mistaken for an execution response.

## Durability before publication

A result held in process memory is not yet the durable answer. Xenon waits for durability, and managed owners use a nonempty fencing barrier before publishing results. If an old handle loses authority, the request cannot treat its captured result as a successful response.

The process-cut proof exercises concrete boundaries around transaction commit and durability. Its receipts bind the code and fault schedule; a conceptual diagram alone cannot establish recovery behavior.

## A journal has a cost

Recorded outcomes occupy storage. The current implementation exposes retained-outcome usage and an explicit capacity limit. It does not claim an indefinite retention or garbage-collection policy has been solved.

See [testing and evidence](../docs/testing.md) for the repeatable proofs, or [operations and recovery](../docs/operations.md) for the current operating boundaries.
