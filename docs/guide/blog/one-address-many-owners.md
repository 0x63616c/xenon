---
title: One address. Many owners.
description: How Xenon routes requests without confusing location with authority.
sidebar: false
---

# One address. Many owners.

A workflow should not need to know which storage process is alive. Xenon gives Temporal's persistence adapters one service address, then handles the route to the partition behind it.

The distinction that matters is simple: **an address tells a request where to go. It does not give a process permission to commit.**

## Follow the request

Choose the entry node and partition owner, then step through the request. The route gets shorter when they match; the authority check remains.

<XenonRoutingDemo />

## A stable entrance

The local ministack places one ingress endpoint in front of multiple identical Xenon agents. A node executes operations for the partitions it owns and forwards others. The forwarded envelope preserves the operation identity and typed request.

Partition assignment is separate from the service address. Moving a partition changes its owner; it does not put a node address into the application's durable keys.

## Check again at the owner

A route can become stale between lookup and delivery. Inside the partition's admission gate, Xenon checks fresh topology activation and the READY ownership record. The process must match the activated incarnation, not merely reuse the same node name.

The handler then works through its SlateDB handle. Before returning a result, a durable transaction or fencing barrier verifies writer authority. This separates routing convenience from the authority to publish a result.

## Movement is a protocol

The current ownership implementation uses explicit administrative activation and assignment stored in S3 via conditional publication. A new reservation opens the partition engine once and conditionally publishes READY; membership uses periodic S3 heartbeat checks, and stale members are evicted automatically after heartbeat expiry.

This is not an automatic election service. The implemented protocol and bounded recovery experiments are described in the [architecture guide](../docs/architecture.md); the [verification status](../docs/status.md) keeps the broader delivery gates visible.
