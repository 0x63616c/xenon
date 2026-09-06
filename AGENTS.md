# Working on Xenon

Start with [the autonomous delivery handoff](docs/handoff-autonomous.md), README.md, CONTEXT.md, docs/agents/issue-tracker.md and the live Wayfinder map.

## Current user authorization

Calum explicitly delegated autonomous project decisions and delivery to the coordinating agent. This supersedes earlier requests for live human approval of each architecture choice. Work through planning, implementation, integration and end-to-end proof without routine clarification.

For decision tickets, use two independent agents: a user-priority advocate grounded in Calum's stated requirements and an adversarial systems reviewer. The coordinator adjudicates using evidence and experiments. Record outcomes as delegated agent decisions, not claims that Calum personally approved a design. Do not impersonate him or invent preferences.

This project adapts Matt Pocock's Wayfinder: execution is in scope, agent debate replaces live-human grilling, and multiple tickets may be resolved per session. Continue beyond charting until the delivery gates pass or a concrete external blocker remains. Keep research, implementation, review and test tasks bounded; isolate concurrent edits in branches/worktrees.

## Review batching preference

Calum explicitly prefers lightweight tracking and batching reviews for throughput. Use one current board, two delivery milestones, broad tickets and ordinary Todo/In Progress/Done statuses; avoid process expansion or micro-ticket bookkeeping. Implement and test related small changes as a cohesive batch, then obtain independent review before integrating that batch. Do not serialize every micro-ticket behind a fresh review cycle. Separate spec, plan and implementation review stages remain; implementation batches may span several bounded tickets. Review safety-critical architecture decisions before dependent work when they would invalidate the batch.

## Latest engine constraint

Calum clarified during autonomous execution that the system must use S3 directly unless demonstrated impossible. Continue with SlateDB backed by S3; the exploratory SQLite snapshot alternative was dropped before acceptance. Do not reintroduce it as a shortcut. No evidence establishes impossibility.

## Node language preference

Calum explicitly prefers Go for Xenon application code. Use the official pinned SlateDB Go bindings if the reproducible binding and lifecycle tests support the required correctness contracts. SlateDB retains its Rust core and direct S3 durability. Record any concrete blocker before choosing another node language; existing Rust probes remain engine evidence, not a reason to override this preference.

## Requirements

S3-only durable application storage; disposable local disks; retain Temporal Server and existing SDKs/UI; execution and visibility; Omes; dynamic Xenon storage-node addition and ownership movement; multiple Temporal instances; crash recovery; preserved acknowledged writes. Slower is acceptable, correctness is mandatory. SlateDB and adapter-side routing remain candidates until delegated decisions validate them.

Do not silently weaken scope or confuse source review with executed tests. Pin versions, cite primary evidence, and report failures honestly. A debate is not a correctness proof.

## Reproducible proof requirement

Calum requires experiments and tests to be repeatable and committed using declarative setup. A passing ad hoc command is not a delivery gate. Commit pinned tool/image versions, environment/topology definitions, workload configuration, fault schedules and saved fuzz inputs, automatic assertions, and setup/run/teardown commands. Evidence must identify the exact source revision, configuration/input hashes, environment versions and result. A clean checkout must recreate the experiment. Keep real-S3 target configuration declarative but credentials external and secret-free. Do not replace this with prose saying a test once passed.

## Delivery

Use GitHub issues as the Wayfinder map and publish linked resolution evidence. Claim tickets before work. Commit, review, test, push and integrate completed work, respecting actual branch protections. Former grilling tickets no longer require Calum to reply. Preserve historical comments and annotate the new delegation.

Research reports currently live on research branches; integrate validated artifacts so main becomes self-contained. Build the runnable proof, CI, operational documentation, compatibility matrix, demonstration and release-ready packaging. Prepare an accurate landing-page draft after the core proof if useful; production SaaS/billing/dashboard remain later scope.

On 2026-09-06 Calum explicitly authorized public publication of this work to the already-public 0x63616c/xenon repository. This supersedes the earlier private-delivery restriction. Use existing authorized resources within their scope; do not invent credentials or assume permission for external spend. Missing external access must be recorded precisely while independent work continues.

Consult docs/handoff-autonomous.md for exact ticket URLs, research commits, architecture evidence, acceptance gates and the full delegation protocol.

## Accepted service routing

Calum explicitly selected one Go Xenon binary and one client-facing service endpoint. The Temporal persistence adapter connects to that endpoint (a Kubernetes Service when deployed in Kubernetes, or an equivalent load-balanced endpoint elsewhere). Every ready Xenon node accepts complete operations, executes locally when it owns the partition, and otherwise forwards internally to the current owner. Retain SlateDB single-writer ownership and S3-only durability. Do not introduce a separate frontend fleet, custom transaction coordinator, or multi-writer engine to pursue WarpStream-style interchangeable execution.

Keep stable logical partition identities and exclude transient node addresses from durable application records and pagination tokens (the S3 ownership directory may carry owner contact metadata). Forwarding must preserve operation identity, input digest and deadlines; bound hops/retries, refresh stale ownership routes, and preserve typed errors and durable replay. Direct adapter-to-owner routing is a future measured optimization, not an initial delivery prerequisite. One endpoint does not waive multi-node serving, dynamic ownership movement, or multiple Temporal instances.
