# Xenon testing and development strategy handoff

## Next session

See [the architecture and testing spec in progress](design/architecture-spec-in-progress.md) for the subsequent interview decisions, required folder structure, open questions, and failure cases. The latest user authorization resumes delivery through spec, plan and implementation stages, each using Astra agents and independent review.

Start by agreeing the smallest testing and development loop that can get Xenon to a solid first release. Do not resume broad implementation until this strategy is clear enough to remove redundant proofs and focus the remaining engineering.

Read [AGENTS.md](../AGENTS.md), [CONTEXT.md](../CONTEXT.md), [the autonomous delivery handoff](handoff-autonomous.md), [the current simulation boundary](../internal/simulation/README.md), and the [live GitHub map](https://github.com/0x63616c/xenon/issues/1). Treat the live repository and tracker as authoritative where older documents have drifted.

## Decisions already made

- Keep the production agent in Go. Xenon wraps and embeds Temporal Server, which is written in Go; changing the whole agent language would restore a process, RPC, or FFI boundary that the unified agent was meant to remove.
- Keep one Go Xenon binary and one client-facing Temporal-compatible endpoint. Each node embeds Temporal Server and the Xenon persistence runtime, executes owned partitions locally, and forwards other operations to the current owner.
- Keep SlateDB backed directly by S3 for durable application state. SlateDB is an implementation detail and may be replaced later if evidence warrants it.
- Do not build the managed cloud service, BYOC control plane, billing, wrapped SDK, or real-AWS proof for this first solid release.
- The docs website and broad repository copy refresh come after the product works. Developer experience still matters: there should be one clear local start, SDK workflow, UI inspection, restart/reset, and teardown path.
- Use Omes as the real-stack workload, including actual Nexus behavior.

## Testing outcome Calum wants

The practical local acceptance target is a ten-minute Omes fuzz run against the unified local stack with Nexus enabled. While it runs, add, stop, and restart Xenon agents within a bounded declared schedule. A passing run must show that accepted workflow state and visibility remain correct and that work resumes after faults stop.

Do not put the ten-minute soak in normal CI. Every failure found locally should retain the exact input, source/config hashes, fault schedule, and trace; minimize it and commit the smallest fast regression to CI.

The real-stack soak should be complemented by fast deterministic simulation testing in the TigerBeetle style. The simulator must drive production coordination decisions while controlling logical time, message delivery, storage completion and ambiguity, node lifecycle, and seeded randomness. It must distinguish volatile effects, durable effects, and response delivery. A timeout must not imply rollback.

## Recommended architecture for deterministic testing

Go remains suitable, but ordinary goroutine-heavy Go is not automatically deterministic. Use two complementary mechanisms:

1. Use Go's pinned `testing/synctest` for bounded components whose behavior is primarily goroutines and timers. It supplies fake time inside a test bubble, but it does not control real sockets, filesystem calls, object storage, or all scheduler choices.
2. Extract a small event-driven coordination kernel in Go. It should consume explicit events and emit requested effects. Inside this boundary, avoid wall-clock reads, uncontrolled goroutines, real I/O, global randomness, and map-order decisions. Production drivers execute effects through timers, internal transport, S3, and SlateDB; the simulator completes the same effects in a chosen order.

Candidate input events include timer firing, message delivery, storage completion, node start/crash, and ownership changes. Candidate effects include setting timers, sending/forwarding requests, conditional object operations, opening a partition, committing a mutation, waiting for durability, and publishing an acknowledgement.

Do not create a second simulated Xenon implementation. The existing production seams are a useful start: `membership.Step(ctx, now)` accepts explicit time, S3 access is behind an interface, transition identities are injectable, routing uses a production interceptor, and durable replay logic is shared.

## Existing deterministic proof

Run:

```sh
GOTOOLCHAIN=go1.27.1 go test -count=1 -v ./internal/persistence ./internal/simulation
python3 scripts/prove.py simulation
```

The committed scenario at `test/scenarios/simulation/lost-response-crash-move.json` already drives production ownership join, topology, membership, routing, and replay decisions through a fixed schedule. It covers a durable mutation, lost response, crash, logical-time eviction, ownership movement, retry without reapplying, changed-input rejection, stale-owner rejection, independent assertions, and negative controls.

It is a seam proof rather than a full deterministic simulator. Its declared gaps include production Manager fencing and native lifecycle, real directory timers, workload generation, schedule exploration/minimization, and native SlateDB durability-completion ordering.

## Proposed development loop to agree next session

1. Define one release behavior matrix around startup/readiness, SDK compatibility, Nexus, execution and visibility correctness, durable retry, scale out/in, and crash recovery.
2. Assign each behavior to exactly one primary proof: fast unit/component test, deterministic simulation, short real-stack CI smoke, or local ten-minute Omes/churn run. Remove overlapping acceptance machinery unless it finds a distinct class of failure.
3. Build one narrow deterministic kernel slice first: membership/readiness/forwarding with delayed directory operations, lost responses, owner crash, logical timeout, and retry. Require exact trace replay for the same schedule and an independent model/checker.
4. Finish the unified-agent readiness bug, then run a clean local developer journey and the ten-minute Omes/Nexus/churn profile.
5. Convert each discovered failure to a minimized deterministic or real-stack CI regression.
6. Only after these gates pass, refresh operational docs, README, issue states, and release packaging.

Before fixing a number of randomized schedules as a gate, benchmark the prototype. Store the complete event sequence as well as the seed because code changes can change how a seed expands.

## Current repository state

At handoff, the checkout is `main` at `ab70510a691034c8136d214b847dcaf880639a31`, matching `origin/main` before this handoff commit.

There is an uncommitted readiness patch in:

- `internal/ownership/manager.go`
- `internal/ownership/manager_test.go`
- `internal/ownership/readiness_test.go`
- `internal/app/legacy_storage.go`
- `internal/app/bootstrap_test.go`

The patch was generated with Codex Spark and is not ready to commit. It needs an independent review and correct MinIO-backed integration execution. Previous review identified topology ABA handling, route-change detection, fatal/listener rechecks during readiness retry, mutable test hooks, and missing production-Manager coverage. Inspect the current diff because the follow-up edit may still contain duplicate resolution work and timing-sensitive tests.

Leave the untracked `.agent-runs/`, `output/`, and `website/.local/` directories alone unless their ownership and purpose are established.

## Why there was a minimum of forty fuzz inputs

No explicit `40` was configured in the soak runner. Commit `5383cc7e1368f42183cb02fbf4f01b480e2b3b58` added a corpus of twenty inputs with `minimum_rounds: 2`, producing a minimum of forty top-level executions, and also required at least 3,600 seconds. The commit metadata uses Calum's inherited repository Git identity, but this was introduced by the autonomous coordinating agent, not requested by Calum. The hour-long run has never completed successfully and should not silently become the release target.

## Scope and tracker cleanup to preserve

The recent scope audit converged on readiness/lifecycle issue [#105](https://github.com/0x63616c/xenon/issues/105) and a refocused unified workload issue [#94](https://github.com/0x63616c/xenon/issues/94) as the core engineering work. Fold only essential capacity/readiness diagnostics from [#92](https://github.com/0x63616c/xenon/issues/92) into acceptance. Defer broad Temporal upgrade matrices [#93](https://github.com/0x63616c/xenon/issues/93), real AWS [#104](https://github.com/0x63616c/xenon/issues/104), and managed Cloud/BYOC [#108](https://github.com/0x63616c/xenon/issues/108). The wrapped SDK issue [#103](https://github.com/0x63616c/xenon/issues/103) was closed as not planned.

Reconcile the live tracker after the testing strategy is agreed. Do not recreate the old version 0.1/version 0.2 split; all selected core work belongs to the first solid version.

## Suggested skills

- `codebase-design` to define the deterministic kernel and the production/simulation effect boundary.
- `ponytail` to keep the release loop small and delete redundant machinery instead of expanding the matrix.
- `tdd` when implementing the first deterministic scenario and minimized regressions.
- `code-review` before committing the existing readiness patch.
- `xenon-temporal-upgrade` only when broad upgrade compatibility returns to active scope.


## Interview follow-up: measured workflow cost and website metrics

Calum explicitly requested retaining this work for the benchmark/metrics phase:

- Run reproducible benchmarks for representative named workflow profiles; do not imply one universal average workflow.
- Measure durable persistence latency and end-to-end workflow/API latency separately, with p50/p95/p99 (interpretation of "P value": latency percentiles, to confirm).
- Report object-storage PUT/GET/LIST/HEAD/DELETE counts and bytes per completed workflow, distinguishing workflow activity from idle coordination, compaction and maintenance overhead. Declare concurrency, batching, retention, partition/database count and cache conditions.
- Derive S3 cost per workflow and per representative workload volume using dated region/storage-class pricing. Include requests, storage and applicable transfer/replication costs; distinguish marginal from amortized total cost and S3-only from whole-system cost.
- Publish measured workload cost and latency percentiles on the website after metrics are verified. Link pinned source, configuration, raw results and benchmark commands; no invented performance claims.

Database-layout spike remains pending: compare one database per Temporal history shard against grouped shards, measuring resource cost, durable latency, throughput and recovery. Multi-region architecture is being discussed, not selected or implemented.
