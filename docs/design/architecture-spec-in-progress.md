# Xenon architecture and testing spec — interview checkpoint

Status: IN PROGRESS. Recorded 2026-09-06 at Calum's explicit request. This is not implementation acceptance or a declaration of release readiness. The interview was followed by explicit authorization to finalize the spec, derive a plan, and execute it using separate Astra agents with independent review at each stage. That authorization supersedes the earlier interview pause.

This document records the discussion following docs/handoff-testing-strategy.md. Explicit decisions below supersede conflicting earlier proposals for this work. Distinguish user decisions from recommendations and unverified feasibility. Existing code and historical evidence remain authoritative about what actually runs.

## Product direction and accepted constraints

- One Go Xenon executable and one instance type, runnable inside or outside Kubernetes. No Kubernetes primitives required for membership, election, placement, or ownership. The user informally calls instances pods; the design must not assume Kubernetes.
- Identical instances embed Temporal and Xenon storage. A coordinator is an additional role in an ordinary instance, not a separately deployed frontend or coordinator fleet.
- Existing Temporal SDKs and UI remain compatible. Internal persistence requests, not whole workflow lifetimes, are forwarded to storage owners. No custom SDK or public owner-redirect protocol is required.
- Design ceiling is now approximately 100 servers. This explicitly replaces the earlier 500/1,000-instance discussion. Capacity is a target to validate, not an existing capability.
- Retain shared storage as authority; local caches/process disks are disposable in shared-storage deployments. Do not introduce a persistent Raft quorum or external etcd dependency under the current direction.
- Start with SlateDB and direct S3. Keep engine-specific details behind a precise storage contract so future replacement remains possible. Portability of coordination does not prove portability of the data engine.
- Automatic rebalancing is accepted. Database paths and logical partition identities remain stable when ownership moves.
- Multi-region is deferred. Focus on one region, with multi-AZ deployments where supported. Cross-region replication/failover is not a current implementation requirement.
- Efficiency is an explicit priority: reuse suitable libraries, bounded experiments and reviews, no vanity frameworks, no broad rewrites merely for aesthetics. Public publication to the existing public repository was explicitly authorized on 2026-09-06, superseding earlier private-delivery instructions.

## Accepted codebase structure — required target

Calum explicitly required following this service-oriented structure, including folder and test locations. This is a spec requirement, not an optional suggestion. Material departures require discussion with him. Migrate existing code with verified behavior; do not mechanically create empty scaffolding or delete unrelated artifacts.

```text
cmd/xenon/
  main.go
internal/
  app/
  cluster/
  partitions/
    slatedb/
  persistence/
  routing/
  temporal/
    adapter/
  registry/
    s3/
    filesystem/
    contracttest/
  identity/
  simulation/
api/
  xenon/v1/
test/
  scenarios/
    simulation/
    integration/
benchmarks/
  placement/
  storage/
  workflows/
docs/
  design/
  operations/
```

Service/controller tests live beside implementations. Shared backend tests live in registry/contracttest. Saved schedules, minimized regressions, topology/workload/fault fixtures live in test/scenarios. Existing operational documents need deliberate migration/link preservation if moved.

### Responsibilities and representative files

- app: config.go and app.go assemble and supervise lifecycle, health and shutdown.
- cluster: service.go, membership.go, election.go, placement.go, rebalance.go, state.go; membership observations, coordinator authority, desired assignments and scheduling of moves.
- partitions: service.go, controller.go, state.go, engine.go; local partition acquisition, opening, activation, draining, retirement and resource lifecycle. SlateDB native integration stays in slatedb/.
- persistence: service.go, execution.go, history.go, matching.go, visibility.go, replay.go; actual persistence operation semantics and durable replay.
- routing: router.go, transport.go; local dispatch, remote owner requests, route refresh and typed stale-owner responses.
- temporal: service.go embeds Temporal; adapter/ translates its persistence interfaces into Xenon operations.
- registry: store.go defines backend-neutral versioned records; adapters implement the same contract.
- identity: ids.go centralizes typed Xenon IDs.
- simulation: scheduler.go, transport.go, registry.go and invariants.go execute production decisions under controlled effects.

The main background services are cluster, partitions and temporal. Routing and persistence need not acquire goroutines merely because they have packages. A service means an internal lifecycle component, not a deployment. Planning should be pure where possible: snapshot + policy -> proposed changes. Service drivers execute I/O and report completions. Coordination must not depend on Temporal or native SlateDB types. Per-partition controllers do not reason about unrelated moves; cluster-wide scheduling belongs above them.

## Shared storage and registry design

Accepted direction: backend-neutral versioned coordination records with S3 and filesystem implementations. The important capability is atomic conditional replacement, not an S3-specific API. S3 can use If-Match/If-None-Match; filesystem implementation may use appropriate locks plus durable crash-safe publication.

The contract must specify complete-record reads/publication, conflict behavior, durability after success, and ambiguous outcomes (a timeout can follow a committed write). Versions are opaque. Include exclusive creation and transition identities where needed; exact Go signatures are not finalized.

Do not artificially exclude NFS/SMB. Use one filesystem adapter unless evidence requires otherwise, and qualify actual client/server/mount configurations with the same contract and failure tests. Functionality through a mounted path is not proof of cross-machine locking or recovery safety. Independent local directories on separate machines are not shared storage. Local-directory durability depends on that disk surviving.

Proposed logical layout, not yet settled:

```text
<cluster>/config
<cluster>/members/<process-incarnation>
<cluster>/placement/...
<cluster>/ownership/<partition-id>/...
<cluster>/data/<partition-id>/...
```

Coordination must be available without opening the Temporal database whose owner it discovers. Multiple objects are not an atomic transaction. Mutable records versus immutable revision chains were explored, not selected; both need an appropriate concurrency primitive. Do not infer filenames or identity formats are final from examples.

## Coordinator and ownership direction

Converged direction: one elected coordinator role embedded in identical instances, backed by shared storage; no Raft under the present requirements. It receives heartbeats, plans assignments, limits concurrent transitions and publishes routing/assignment views. Instances can cache versioned views and route directly. Existing healthy owners need not stop solely because the coordinator is unavailable, but continued serving is conditional on actual storage/fencing authority. New planning/takeover can pause during election.

The exact election protocol/library remains OPEN. Conditional creation chooses an initial coordinator; takeover requires a defined renewal/expiry policy and conditional replacement of the observed version. Jitter is a recommended efficiency measure, not the safety mechanism. Reread after waiting. Record randomness for simulation. A leadership generation alone does not fence external effects. Checking a leadership object and then unconditionally writing a separate plan object is unsafe: plan publication must be tied to current authority by an enforceable protocol.

Assignment is intention; active ownership is acquired authority. Proposed transition: assigned -> reserve new generation -> open/recover/fence storage -> validate current assignment/reservation -> conditionally publish ready. Exact atomic boundaries and stale-opener handling must be verified. A restarted process has a new incarnation. Storage must prevent obsolete writers from acknowledging new mutations. Coordinator policies can evolve independently from per-partition safety.

Core transitions must operate below Temporal, avoiding recovery/bootstrap circular dependencies. Temporal could later orchestrate higher-level administrative operations, but essential moves cannot depend on Temporal workflows progressing.

## Placement, databases and growth

Mappings are distinct:

Temporal history shard -> Xenon storage partition/database -> current owning instance.

One SlateDB database has one active writer instance, but supports concurrent operation submission and durability batching. One Xenon server may own multiple database writers. A database can contain multiple Temporal shards in separate key ranges; grouping shares resources and ownership movement. Matching, visibility and cluster metadata need separate layouts. Preserve atomic transaction domains.

Database-layout spike is agreed: compare one database per Temporal history shard with grouped shards at fixed workload/shard count. Measure idle and loaded memory, background activity, S3 request cost, durable-write latency, throughput and takeover time. One-to-one versus grouping is OPEN. Multiple automatically assigned writers per instance is the current recommendation, not a finalized sizing policy. Configurability at cluster creation and online expansion/migration behavior are OPEN. Never remap populated data by simply changing a modulo/hash or count. Fixed database count creates a storage-writer ceiling.

Placement leading candidate: bounded-load consistent hashing (buraksezer/consistent), compared against rendezvous hashing. This is NOT a finalized dependency decision. Desired behavior: deterministic decisions, reasonably balanced partition counts, limited movement, stable assignments, and explicit handling of already-running transitions. Bounded partition counts are not workload balance. Rebalance concurrency and hot-partition policy remain OPEN. Earlier perfect-count examples were illustrations, not guarantees of a hashing algorithm.

Current code limitations observed during interview: Join creates four history partitions plus matching/global/four visibility partitions; topology validator caps membership at 64. Existing round-robin placement can move many assignments on joins. These do not meet the new scaling target merely because config accepts many Temporal shards.

## Routing and request contract

Accepted: standard SDK -> embedded Temporal -> persistence adapter -> local execution or direct gRPC to active storage owner. Do not align Temporal History ownership and Xenon storage ownership as a new prerequisite; the user accepted persistence forwarding after clarifying the layers.

Avoid recursive forwarding chains. Proposed protocol: stale destination rejects before execution and returns an owner hint; originating internal caller refreshes and retries within one bounded budget/deadline. A hint is not authority. Preserve operation ID and input digest for durable replay after response loss. Include appropriate expected owner incarnation/generation and protocol version; exact envelope remains OPEN. Pool/multiplex connections and measure topology/resource costs. Do not assume long-running workflows hold persistence RPCs open throughout their lifetime. Readiness for an instance with no locally owned partitions was recommended but not explicitly finalized.

## Required test design

Calum explicitly accepted this design as part of the spec:

1. Colocated service/controller tests exercise production decision logic.
2. Backend contract tests verify real registry implementations and actual native storage guarantees.
3. Deterministic simulation composes production controllers across instances.
4. Real-stack integration validates actual Temporal/SDK/UI/Nexus/transport/engine wiring.

Decisions consume explicit events and emit effects. Avoid hidden wall-clock reads, uncontrolled randomness, real I/O and map-order-dependent decisions within the deterministic boundary. Drivers may use goroutines. Use testing/synctest for suitable bounded goroutine/timer components, not as a claim of deterministic real network/storage behavior.

Simulator separately models requested effects, volatile changes, durable commit and response delivery. A timeout does not imply rollback. It must control lifecycle, logical time, transport, object-store completion/conflicts/ambiguity and relevant engine completions. Do not implement a second Xenon algorithm. Independent invariants and negative controls detect stale acknowledgements, duplicate application, digest mismatch, partial atomic recovery and missing progress after faults stop. Backend contract tests check that simulated assumptions match real guarantees.

Save complete event traces as well as seeds, exact source/config/input hashes, tool versions and results. Minimize discovered failures into fast regressions. Benchmark schedule execution before selecting a numeric randomized-schedule gate. First recommended coupled slice: coordinator replacement during a partition move.

Failure matrix to design and execute:

- Old coordinator resumes after takeover and tries to publish stale plans.
- Simultaneous takeover attempts; renewal races with expiry observation.
- Conditional write commits but response is lost.
- Owner crashes before/after reservation, during opening, or before readiness publication.
- Assignment is superseded while an opener is delayed; stale native opener resumes.
- Durable mutation response lost, retry reaches new owner; no double application.
- Shared storage interruption; do not promise blanket continued availability.
- Concurrent joins during moves; avoid conflicting/repeated scheduling.
- Whole cluster restarts with empty local caches/process state.

## Release and benchmark loop

Keep the local ten-minute Omes fuzz run with real Nexus and bounded declared add/stop/restart faults. Verify accepted state, histories/results, visibility, actual ownership movement and progress after faults stop. Short real-stack smoke and minimized regressions belong in normal CI; the ten-minute soak does not. Preserve meaningful existing native durability/fencing tests. Remove redundant acceptance orchestration only when replacement coverage is demonstrated.

Release matrix, readiness fix, reproducible testing, clean-checkout developer journey, operational docs and packaging remain work to complete after the interview. Do not restore the old hour-long/forty-execution minimum or broad upgrade/real-AWS/managed-cloud scope by accident. Under-ten-second behavior was discussed but request deadline versus full recovery target remains unresolved; no latency guarantee is approved.

Benchmark and website requirements explicitly requested for later:

- Named representative workflows, not one unsupported universal average.
- Durable-write and end-to-end latency separately; p50/p95/p99 ("P value" interpreted as percentiles, not a statistical p-value).
- Object-store operation counts and bytes per completed workflow; separate idle coordination, compaction and maintenance, and show amortized costs.
- Dated region/storage-class pricing for S3 requests, retained storage and applicable transfer/replication; separate S3 costs from full deployment cost.
- Publish only measured claims, with pinned source/config, commands and raw results.
- SlateDB already batches WAL durability; start there rather than adding Xenon application batching that changes transaction semantics. Wait for each operation's own durability before acknowledging.

## ID requirement

Calum explicitly requires Stripe-style IDs for Xenon-owned identities everywhere and auditing/migrating existing nonconforming IDs as part of this work. Exact prefix vocabulary and encoding are OPEN. Preserve required Temporal wire/storage IDs and user-supplied identities. Do not silently break persisted references, routing tokens or backend formats. A separate memory note was saved at his explicit request.

## Library evaluation ledger — no final adoptions

- gRPC-Go: existing transport to retain.
- buraksezer/consistent: leading placement candidate; compare dgryski/go-rendezvous for movement/balance.
- grafana/dskit/services: candidate service lifecycle supervision; do not let lifecycle framework own domain decisions.
- grafana/dskit ring/partition/KV: inspect potential reuse; built-in KV adapters and semantics are not proven to satisfy our shared-storage/fencing/simulation contract.
- hashicorp/memberlist: gossip membership/failure detection, not exclusive storage ownership. Do not add a second gossip system without evidence it helps.
- podplane/s3lect and other shared-storage election libraries: candidates requiring pause, expiry, ambiguous response, and stale-leader publication review. No production trust claim established.
- etcd-io/raft versus hashicorp/raft reviewed conceptually. etcd's deterministic core fits simulation; HashiCorp supplies more runtime machinery. Both require correct persistent consensus state/quorum management. Deferred under accepted shared-storage authority direction.
- Temporal v1.31.2 source still includes Ringpop and persisted membership bootstrap records. Do not claim tables replaced Ringpop solely from schema presence.

## Open decisions before final spec

- Exact coordinator renewal/takeover and plan-fencing protocol; library selection.
- Database grouping and initial count, online growth/migration and defaults.
- Placement implementation, move budget, simultaneous topology changes, heterogeneous/hot workload handling.
- Readiness semantics and explicit request/recovery targets.
- Record formats/layout, API boundaries and typed errors; filesystem qualification matrix.
- Resource footprint of embedding Temporal default services in up to 100 instances.
- Exact ID format and migration plan.

Resolve open engineering choices through evidence and independent review under the latest delivery authorization. Do not describe delegated choices as decisions personally made by Calum.

## Latest delivery authorization and mandatory DST requirements

Calum explicitly requested: finalize the spec with important Go snippets and exact APIs at the seams; derive a plan from that spec; execute it; use a separate Astra agent for each stage (spec, plan, implementation) and independent agent review each time. The coordinating agent adjudicates reviews, verifies results and integrates work. Efficiency remains mandatory.

- Fail fast on the first unexpected error or violated invariant. Save the first failure trace/provenance and perform bounded scoped cleanup before exiting. Expected injected failures and retryable conditions are scenario inputs, not reasons to abort a valid fault test. Do not run the rest of a ten-minute workload once its correctness gate is known to have failed.
- Time is a mandatory explicit seam. All Xenon behavior-time in component tests and DST must be controlled; do not use real sleeps, hidden context deadlines, wall-clock expiry or process-global random state as scheduling mechanisms. Distinguish wall timestamps from monotonic elapsed time. Real-stack/native integration and external watchdogs still use real time and must not be mislabeled deterministic.
- DST means deterministic simulation testing: production decisions, controlled effects, independent oracles, exact replay and minimized committed failure schedules.
- The companion [DST and service contracts](dst-service-contracts.md) supplies reviewed concrete interfaces and behavior. Its final review must verify API examples, error/timeout semantics, state ownership, lifecycle teardown and the link between coordinator authority and plan publication.

Initial source audit: prove.py stops between commands on failed return codes but mostly captures whole-command output; real-stack agent-smoke.py also blocks on individual probes/workload waits. Neither establishes immediate detection of an asynchronous invariant violation. New implementation must add scenario-level detection/cancellation without losing the first failure or skipping cleanup. Current ownership/storage code contains real timers/context deadlines, and the existing ABA readiness test is timing-dependent; those are migration gaps, not proof of DST compliance.

### Continuous failure search (explicit user requirement)

Provide an opt-in search mode which keeps generating fresh supported workflow inputs and fault schedules until the first unexpected failure, user cancellation, or a declared budget. Reuse scenario generation, execution, independent checking and replay abstractions from bounded runs; do not build a separate fuzz-only implementation. Maintain bounded concurrent/in-flight work and periodic settle/check phases so endless input generation cannot conceal lack of progress. Retain expanded workload and actual event sequence plus generator/build identity and all seeds. Separate workflow-input and scheduler/fault randomness. Real-stack workflow fuzzing executes actual Temporal/Omes/Nexus; DST additionally controls production Xenon coordination time, messages and storage outcomes, and is not a claim to simulate the entire Temporal engine. Ten-minute local acceptance, fast CI regressions, and continuous search are distinct profiles of shared machinery. Stop new generation immediately on first invariant failure, retain first cause/evidence, then cancel and clean up within bounds; expected injected faults are not test failures.

### CLI design follow-up

User explicitly requested CLI design and library selection, with a modern user experience. Current cmd/xenon uses standard flag with version/check-config/start; preserve these while evolving commands. Coordinator recommendation (not yet user-selected): Cobra for structured commands, flags/help/completion; keep typed config loading and dependency injection explicit rather than automatically adding Viper. Thin commands in cmd/xenon call service/harness interfaces; injectable input/output/context enables tests without real process startup. Human-friendly output plus stable JSON, diagnostics on stderr, machine results on stdout, documented exit codes and bounded Ctrl-C cleanup preserving evidence. Runtime operational commands must not import the simulator/native proof harness solely to inspect a cluster. Search/replay/minimize should share existing harness APIs; final command names remain to be decided; the latest user decision below requires developer tooling through the same CLI. Defer a full-screen TUI and styling dependencies until a concrete need. CLI integration belongs after core service/test seams, not a distracting parallel framework rewrite. Official candidate: https://github.com/spf13/cobra.

### Single CLI requirement — latest user decision

Calum explicitly selected one professional, standardized `xenon` CLI for all supported operations, including starting servers and the test/search/replay workflow. This supersedes the earlier unresolved companion-developer-CLI option. Keep cmd/xenon as the thin command entry point; shared library/service APIs power commands. Existing scripts may remain implementation helpers during migration, but users must not need to discover separate scripts/binaries for supported journeys.

Standardize command/flag vocabulary, help/completion, typed configuration and documented precedence, human versus stable JSON output, stdout results/stderr diagnostics, consistent exit codes and injected input/output/context. Ctrl-C stops workload generation, preserves first failure/replay artifacts and performs bounded scoped teardown. Noninteractive CI must not hang on prompts. Start runs one foreground server suitable for a process supervisor; local multi-instance development setup is a distinct explicit subcommand/profile. Server orchestration, live-cluster inspection and tests should use typed interfaces rather than duplicate business logic in CLI handlers. Lightweight version/help/config validation should avoid initiating backend connections or native database work. Cobra remains recommended pending dependency pin/review; no custom Temporal SDK is introduced. Standard Temporal SDK compatibility and clean internal Go interfaces are both required; a public Xenon SDK is not inferred from the user's wording.

### Review throughput — latest user preference

Calum explicitly prefers batched review over review after every micro-change. Keep focused tests and bounded commits during implementation, but group related changes into meaningful batches for independent review and integration. This supersedes any earlier per-ticket review/merge barrier. Spec and plan still receive independent stage review; high-risk architecture decisions remain reviewed before dependent implementation.
