# Delivery plan: production services, deterministic testing and local release proof

Status: independently reviewed and approved for execution, 2026-09-06; spec independently approved and committed at `249e43e`. Implementation follows coordinator adjudication of the spec and this plan; review is not an additional user approval request. Continue into implementation after that gate. This document records planned work and commands, not completed tests.

Authoring baseline: `fc4180d9b73c7d39dcdc0790f8d876ee10798ea1` on `codex/release-testing-loop`. Existing uncommitted `internal/ownership/manager*`, `readiness_test.go` and `internal/storage/runtime*` changes are unaccepted input; preserve and review them in step 6. Public publication was subsequently explicitly authorized and recorded at `6235200`; the earlier private-publication restriction is superseded. Check the actual head and working tree before implementation. The live [Wayfinder map](https://github.com/0x63616c/xenon/issues/1) remains open; its historical 55/66 checkpoint is not current integration proof.

## Binding scope and working rules

Implement the [architecture checkpoint](architecture-spec-in-progress.md) and [service contracts](dst-service-contracts.md). In particular retain one identical Go executable embedding Temporal, direct S3 SlateDB application durability, SDK/UI/Nexus compatibility, stable database paths and transaction domains, the exact required service folders, approximately 100-instance validation target, and the ten-minute local release profile. Native/real-stack behavior is not deterministic merely because its simulated counterpart passes. Real AWS, broad upgrades and managed services remain outside this first-release task; report qualification separately.

Claim linked bounded implementation tickets under the live map before edits. Public GitHub publication is authorized by the recorded user decision at `6235200`; do not reinstate the former publication hold. Use separate implementation agents and independent review of cohesive batches; small tickets may be implemented and tested together before that review. Do not pause for a separate review of every micro-change; decision spikes use the required advocate/adversarial review, with the coordinator recording delegated decisions. Use isolated worktrees for concurrent edits. Commit usable slices as work progresses; review, push and integrate cohesive batches in dependency order, then run their gates on the integrated revision. Do not accumulate a replacement service tree that production never calls. No unrelated deletion or mechanical empty scaffolding.

The gate names below that do not exist at the baseline are **deliverables of their step**, registered in the existing `scripts/prove.py` runner with committed manifests, exact expected tests and input hashes. A missing gate is a failure. Existing commands must remain usable until a reviewed equivalent replaces them. Development evidence with a dirty patch hash is diagnostic; clean committed reruns are the acceptance evidence.

## Implementation groupings (not separate milestones)

GitHub uses only two delivery milestones: Build and harden, then Release proof and polish. The groupings and nine sections below describe dependencies and acceptance scope, not extra milestones or mandatory stop-and-review cycles. Implement related work together, keep focused tests running during development, and review the composed diff at the batch boundary. Small commits and ticket updates need not interrupt implementation.

1. **Backend and feasibility foundation:** shared S3/filesystem registry contracts in parallel, time seams at their first consumers, plus independent native/layout/placement experiments. Review the composed backend work together; review safety-critical feasibility decisions before they constrain production integration.
2. **Working distributed service:** production cluster/partition/persistence services, coupled DST and origin-only routing (sections 3–5), delivered in usable internal commits with a joint review of their interactions.
3. **Complete migration and readiness:** remaining service families, persisted identity/authority compatibility and real lifecycle readiness (sections 6–7), reviewed together with populated-state evidence.
4. **Developer and release experience:** one CLI, shared search/replay/minimize, real Omes/Nexus/churn profiles, capacity/benchmark evidence and clean-checkout packaging (sections 8–9), with independent final integration review.

Parallel agents own disjoint files/worktrees. A shared API owner provides the common contract so backends or callers do not build incompatible versions. Do not start dependent unsafe production behavior before its required native/cutover gate; independent implementation and tests continue meanwhile. Batch boundaries can move when evidence shows a better grouping, without dropping any listed requirement.

## 1. Registry, identity and time foundation

Dependencies: adjudicated spec. Requirements: [registry](dst-service-contracts.md#registry-conditional-publication-and-ambiguity), [identity](dst-service-contracts.md#identity-and-randomness), [time](dst-service-contracts.md#events-effects-time-and-lifecycle).

Own `internal/registry/store.go`, `s3/`, `filesystem/`, `contracttest/`, `internal/identity/ids.go`, and the clock/timeout helpers in the service packages that first consume them. Reuse conditional S3 code in `internal/directory/directory.go`; retain a temporary adapter for existing callers. Use the existing pinned S3 SDK, opaque versions, typed errors, whole-record publication and explicit unknown outcomes. Inventory every Xenon-owned ID producer and persisted reference; classify Temporal/user IDs as preserved. Implement typed new IDs, old-format read fixtures and an explicit migration/version plan before writing changed durable formats. Clock helpers must own injected timers and cancellation, including pre-dispatch versus post-dispatch distinction; no generic event framework.

The filesystem adapter uses a stable lock inode plus durable publication and read-recovery protocol from the reviewed spec. Reuse an established lock library only after verifying its exact platform/shared-filesystem semantics. Deliver one contract suite, not separately weakened backends. Pin cross-process/cross-machine fixture configuration; local, NFS and SMB each get a qualification row. Without an actual mount/client/server environment, NFS/SMB are unqualified with an executable recipe, never silently excluded or counted as passed.

First bounded implementation ticket: typed IDs plus the registry API/error/envelope semantics and deterministic unit tests only; extend the existing S3 adapter in the next ticket, then filesystem/time contracts. Do not create unconsumed service skeletons. Implement and test these related tickets as one foundation batch, then independently review and integrate the batch; do not wait for micro-reviews between tickets. All three complete this foundation step.

Acceptance commands (registered in this step):

```sh
go test -race -count=1 ./internal/identity ./internal/registry/...
python3 scripts/prove.py registry-contracts
python3 scripts/prove.py identity-time-contracts
```

Required cases: create races, exact-version replace, identical-body version ABA, partial/truncated records, wrong digest/ID reuse, lost committed response followed by retry conflict, cancellation after dispatch, write crash after rename before directory fsync, read recovery plus fsync failure, lock-holder process death, copied bytes, entropy errors, legacy identity reference round-trip, timer cancellation/late completion and deadline expiry without sleeping. Negative controls must detect unlocked replace, omitted directory fsync and hidden retry ambiguity. Gate review checks the real SDK retry configuration and filesystem crash assumptions, not only mocks.

## 2. Resolve bounded feasibility questions before fixing defaults

Dependencies: step 1; may overlap its independent backend validation. Requirements: [placement and databases](architecture-spec-in-progress.md#placement-databases-and-growth), [libraries](architecture-spec-in-progress.md#library-evaluation-ledger--no-final-adoptions), [control authority](dst-service-contracts.md#coordinator-authority-and-ownership-atomicity), [native seam](dst-service-contracts.md#engine-and-persistence-seam).

Own `benchmarks/placement/`, `benchmarks/storage/`, `benchmarks/workflows/`, `internal/partitions/engine.go`, `internal/partitions/slatedb/` native contract fixtures, and a short decision/evidence ledger in `docs/design/`. Compare the named placement candidates at fixed partition counts/topology changes. Inspect lifecycle/election library fit before choosing a dependency; retain existing gRPC. A candidate that cannot preserve pure decisions, CAS authority and injected time is rejected with the concrete mismatch. Avoid adopting memberlist or a lifecycle framework merely to fill a package.

Measure one database per shard versus grouped shards at the same workload/shard count: idle/loaded memory, requests/bytes, background/maintenance work, own-write durability percentiles, throughput and takeover time. Include matching/global/visibility transaction domains. Measure control-record bytes, serialization cost and renewal/owner-update CAS contention for declared partition counts at 1, 10 and approximately 100 simulated clients, then real registry clients. Produce a measured initial count/grouping/move-concurrency choice and safe growth policy; never rehash populated records after changing a count. A fixed-count ceiling and hot partitions must remain explicit limits. Measure identical embedded Temporal instance footprint separately; simulated control clients do not establish 100-server runtime capacity.

Native API barrier: inspect and exercise the pinned Go binding implementation for consistent transactional read/scan, commit receipt ownership, per-mutation AwaitDurable, nonempty replay/durable-read barriers, terminal fencing, late open and timeout/cancellation/close retention. Adapt to the actual API without weakening the contract. A missing required guarantee blocks dependent mutation integration and gets an explicit experiment/decision; keep independent registry/placement work progressing. No alternative engine or language shortcut without recorded evidence and authorization constraints.

```sh
python3 scripts/prove.py placement-layout-spike
python3 scripts/prove.py control-record-scale
python3 scripts/prove.py native-engine-contracts
python3 scripts/prove.py embedded-temporal-footprint
```

Require failing controls for stale native commit, premature durable read, lost replay result and close-while-in-use. Native late-open schedules must prove the recovered current owner makes progress after all finitely delayed obsolete opens finish. Decide defaults from saved raw results, never a configuration validator accepting 100.

## 3. First production service slice: coordinator, one partition and persistence

Dependencies: steps 1–2, with unresolved native safety blockers closed. Requirements: [layout](dst-service-contracts.md#scope-and-required-layout), [effects](dst-service-contracts.md#events-effects-time-and-lifecycle), [authority](dst-service-contracts.md#coordinator-authority-and-ownership-atomicity), [persistence](dst-service-contracts.md#engine-and-persistence-seam).

Own `internal/cluster/{service,membership,election,placement,rebalance,state}.go`, `internal/partitions/{service,controller,state,engine}.go`, `internal/partitions/slatedb/`, `internal/persistence/{service,replay}.go`, and the minimum `internal/app/{app,config}.go` plus `cmd/xenon/main.go` wiring. Extract/reuse `internal/ownership/{membership,manager,join}.go`, `internal/app/legacy_storage.go`, `internal/node/{shard,journal}.go` and the former `internal/replay/replay.go` (now `internal/persistence/replay.go`); transition shims keep unmigrated operation families working. The real executable must run the new cluster and partition drivers and execute an actual existing shard operation through the new persistence/native seam before this slice is accepted.

Before enabling the new drivers on an existing cluster, implement a versioned legacy-topology/directory-to-control cutover. The initial supported procedure is bounded offline migration: stop and verify all old agents are stopped, drain/fence old writers, and enforce old/new participant exclusion before publishing new authority. Preserve database paths, partition IDs and application records. A new binary rejects legacy/migration-in-progress authority until reconciled; old binaries/managers must be demonstrably refused or isolated by an enforceable launch/access boundary, not an advisory flag they do not read. If old binaries cannot honor a format gate, the migration procedure must revoke their authority access and verify exclusion before new service startup. Save migration phase/identity durably, reconcile ambiguous writes, and resume or safely halt after crashes at every cutover boundary. Do not claim rolling mixed-version coordination compatibility. The cutover fixture starts from populated legacy state and proves unchanged acknowledged data, stable routing references, interrupted/resumed migration and rejection/isolation of a deliberately restarted old manager before step 3 is accepted; this cannot wait for step 7 identity cleanup.

Use one authoritative control record; enforce field-specific owner/coordinator mutations in production functions. Define concrete events/effects, pending effect ownership, stable ordering, injected time and independent RNG streams. Reservation confirmation precedes one native open; ambiguous publication reconciles; delayed completion cannot activate an obsolete assignment/incarnation. Retain serialized partition admission through durability and atomic replay accounting. Reverse-order shutdown reports outstanding native effects without freeing live handles. Core bootstrap/election/movement must work with Temporal unavailable.

```sh
go test -race -count=1 ./internal/cluster ./internal/partitions/... ./internal/persistence ./internal/app
python3 scripts/prove.py authority-cutover
python3 scripts/prove.py service-production-slice
python3 scripts/prove.py native-engine-contracts
python3 scripts/agent-smoke.py
```

Gate requires two real agents, a real registry and native SlateDB, one actual mutation/replay, coordinator loss and owner movement with acknowledged-state recovery. Test ABA assignments, unknown reservation response, stale old-plan CAS, owner field authorization, late/duplicate completions, cancellation and cold local-state loss. Review must trace `cmd/xenon` to new production decisions; passing new packages while old Manager still controls production is insufficient.

## 4. Coupled DST with independent checks and fail-fast evidence

Dependencies: step 3. Requirements: [required test design](architecture-spec-in-progress.md#required-test-design), [runner and invariants](dst-service-contracts.md#independent-checks-schedules-and-runner).

Own `internal/simulation/{scheduler,transport,registry,invariants,runner}.go` and tests beside those files; scenarios in `test/scenarios/simulation/`. Extend the current `internal/simulation` tests using production cluster/partition Steps and persistence replay logic, replacing isolated approximations as coverage becomes equivalent. Model request, volatile change, durable commit and response as distinct events; control local incarnation clocks, registry CAS completion/ambiguity, modeled engine epochs/completions, transport and lifecycle. The independent checker consumes observations, not production eligibility/replay/success helpers.

First saved schedule: coordinator replacement during a move, including a delayed old plan and delayed obsolete native open, then a healthy settle phase. Add the specified renewal races, simultaneous contenders, crash boundaries, response loss/new-owner replay, joins during moves, shared-storage interruption and whole-cluster restart. Preserve full ordered traces and exact replay; minimization retains the original plus the smallest replayable same-invariant regression. Benchmark schedules before assigning a numeric search count.

```sh
go test -race -count=1 ./internal/simulation ./internal/cluster ./internal/partitions/...
python3 scripts/prove.py dst-coupled
python3 scripts/prove.py dst-negative-controls
python3 scripts/prove.py runner-cleanup-controls
```

The same checker must fail deliberate stale-plan publication, post-fence commit, missing atomic replay result, changed-digest application, duplicate application, recursive forwarding and deadline reset. Runner tests inject failure while a workload is blocked, setup failure, expected scheduled kill, unexpected child exit, cancellation, late completion and cleanup failure. Latch the first unexpected cause immediately, stop generation/submission, retain evidence outside disposable resources, and clean only owned resources within bounds. Cleanup error cannot overwrite the primary cause; unreaped resources make the run fail and are named. One expected fault passes only after recovery assertions.

## 5. Origin routing and generated API migration

Dependencies: steps 3–4. Requirements: [transport](dst-service-contracts.md#transport-and-request-budget), [IDs](architecture-spec-in-progress.md#id-requirement), [required layout](architecture-spec-in-progress.md#accepted-codebase-structure--required-target).

Own `internal/routing/{router,transport}.go`, all `api/xenon/v1/*.proto` and generated `*.pb.go`, `scripts/generate-proto.sh`, Rust reference protobuf include paths and adapter callers. Move `proto/xenon/v1` source plus current generated bindings into `api/xenon/v1`; preserve protobuf package, field numbers and wire semantics. Update `go_package`, imports, pinned generation inputs and compatibility manifests atomically. Record old/new wire fixtures; generation must be repeatable with no diff. This move must not rewrite stored keys or Temporal IDs.

Replace recursive interceptors and broad `Unavailable` retries with origin-only bounded attempts, one unchanged operation/digest/deadline, expected incarnation/generation and typed not-executed/unknown/stale errors. Refresh authoritative routes; hints do not authorize dispatch. Local dispatch has the same admission/fencing checks. Retain pooled gRPC connections and bound resources. Preserve deadline at wire edge while using injected monotonic origin budget; model skew without resetting timeouts.

```sh
bash scripts/generate-proto.sh
git diff --exit-code -- api/xenon/v1
python3 scripts/prove.py origin-routing
python3 scripts/prove.py dst-coupled
python3 scripts/check-layout.py
```

Cases: stale hint, destination never recursively forwards, response loss then new-owner replay, digest conflict, protocol mismatch, deadline expiry during backoff/native commit, caller never receives late success, terminal conditional errors unchanged, three-attempt exhaustion and connection reuse/closure. Re-run Go/Rust wire compatibility. Integrate a real forwarded persistence operation from a nonowner agent before acceptance.

## 6. Review and integrate readiness into the actual lifecycle

Dependencies: steps 3–5. Requirements: [time/lifecycle](dst-service-contracts.md#events-effects-time-and-lifecycle), [release loop](architecture-spec-in-progress.md#release-and-benchmark-loop).

Own existing readiness patch in `internal/ownership/manager.go`, tests, `internal/app/legacy_storage.go`, and its final homes in `internal/{app,cluster,partitions,temporal}/`. Review the captured diff independently before adopting it. Convert timing-dependent ABA tests into controlled completions. Define readiness from actual service/routing/owner capability and current authority, with a reviewed policy for zero locally owned partitions; a nonowner that can route should be validated with real operations. Do not report every embedded Temporal component as healthy from a storage flag. Reject invalid topology/configuration early while preserving supported command behavior.

```sh
python3 scripts/prove.py service-readiness
python3 scripts/prove.py dst-coupled
python3 scripts/prove.py nexus-readiness
python3 scripts/agent-smoke.py
```

Assert delayed old readiness cannot resurrect an owner; coordinator absence alone does not revoke a healthy fenced owner, shared-storage/native failures are surfaced, timeout retains native resources, routing-only readiness works as specified, and native/Temporal startup and bounded shutdown failures remain observable. No acceptance inherited from the preexisting patch or an earlier successful build.

## 7. Complete service migration and preserve persisted state

Dependencies: steps 1–6. Requirements: [exact layout](architecture-spec-in-progress.md#accepted-codebase-structure--required-target), [IDs](architecture-spec-in-progress.md#id-requirement), [engine semantics](dst-service-contracts.md#engine-and-persistence-seam).

Own remaining `internal/persistence/{execution,history,matching,visibility}.go` and operation-family files; `internal/temporal/service.go`, `internal/temporal/adapter/`, final `internal/app/`, `cmd/xenon/`, `scripts/check-layout.py`, manifests and operations links. Move `internal/node` semantics and replay, the adapter translation/factory (now together in `internal/temporal/adapter`), and application/storage wiring (now in `internal/app`) in behavior-preserving batches. Keep Temporal types at the adapter boundary and native types in `partitions/slatedb`. Migrate colocated tests with each family; production must use the migrated family before removing its shim. Avoid an all-at-once rewrite.

Move active integration fixtures to `test/scenarios/integration/`; preserve frozen inputs and their hashes, and deliberate links for operations docs moving under `docs/operations/`. Preserve meaningful native/Rust regression artifacts. Record old-to-new file/coverage mapping before retiring duplicate orchestration. Complete the Xenon-ID inventory including run/harness resource IDs; implement versioned persisted migration with explicit old-format read compatibility and stable path/reference translation. Exercise populated old state, interrupted/resumed migration, crash/restart and readback from new code. Preserve user/Temporal IDs and pagination tokens. No hash/count rewrite of populated databases.

```sh
python3 scripts/check-layout.py
python3 scripts/prove.py service-migration
python3 scripts/prove.py persisted-identity-migration
python3 scripts/prove.py go-runtime-stores
python3 scripts/prove.py go-visibility
python3 scripts/prove.py go-shard-compat
python3 scripts/prove.py dst-coupled
python3 scripts/ministack-runtime.py --smoke
```

Layout gate verifies actual package/import boundaries and named required directories, not empty folders. Audit all direct timers, deadlines, wall clocks and random calls in migrated Xenon packages, documenting integration-only exceptions. Independent review compares behavior and legacy tests for every family: history/shard guards, matching subqueue/userdata atomicity, visibility filtering/count/pagination, replay capacity and maintenance recovery. Check old populated state with unchanged workflows/results and transaction domains; an empty-cluster smoke is insufficient.

## 8. Shared scenario search, CLI and real Nexus release profile

Issue #119 closes only against the [machine-checkable acceptance criteria](issue-119-acceptance.md), including concurrent fresh workflows, independent oracles, bounded cleanup and replay/minimization. Gate names there are required deliverables, not claims of current implementation.

Dependencies: steps 4–7. Requirements: [continuous search](dst-service-contracts.md#continuous-failure-search-and-replay), [CLI](architecture-spec-in-progress.md#cli-design-follow-up), [release](architecture-spec-in-progress.md#release-and-benchmark-loop).

Own `internal/simulation` generator/search/replay/minimization, common scenario fixtures under `test/scenarios/{simulation,integration}/`, existing `scripts/{prove,ministack-runtime,corrected_fuzz,omes_workloads}.py` integration, thin CLI commands/tests in `cmd/xenon`, and `docs/operations/testing.md`. Reuse existing pinned Omes overlay/corpus/oracles and retain original failures. Implement a shared scenario envelope, bounded queue and `MaxInFlight=1` case driver; generate fresh supported expanded inputs, separate workload/fault RNG streams, and require settle/recovery checks per case. Record generator capability/version/hash and unsupported combinations as generation errors, never passing cases. Replay loads saved bytes, not regenerated inputs. DST replay is exact event order; real-stack replay preserves workload/fault intent and labels OS scheduling differences.

Evaluate/pin Cobra versus current flags for modern help/completion and explicit typed configuration; do not add Viper or a TUI by default. Preserve `version`, `check-config`, `start`, script entry points and machine outputs. The latest firm user decision requires every supported journey in the single `xenon` CLI: server startup, development topology management, cluster inspection, tests, bounded/continuous search, replay and minimize. Developer journeys may reuse script implementations during migration, but scripts alone are not the delivered user interface. Keep commands thin over typed service/harness APIs; help, configuration validation and version must not initialize backends, and inspection must not start simulation or native proof resources. Standardize configuration precedence/validation, input/output, stable JSON/exit codes, stderr diagnostics and bounded Ctrl-C cleanup across commands. This adds no custom Temporal SDK; existing SDK compatibility remains required. Document and test all CLI journeys, retaining script compatibility where useful. The following manifest gates remain stable internal acceptance entry points:

```sh
python3 scripts/prove.py scenario-search
python3 scripts/prove.py scenario-replay-minimize
python3 scripts/prove.py runner-cleanup-controls
python3 scripts/prove.py cli-contracts
python3 scripts/prove.py real-stack-smoke
python3 scripts/prove.py local-release-ten-minute
```

The last gate runs actual Omes fuzz plus actual Nexus against identical embedded agents for ten minutes of workload, with committed bounded add/stop/restart faults and declared additional setup/recovery/teardown budgets. Assert execution/history/results, visibility/UI, changed owner serving requests, multi-Temporal participation, preserved acknowledged mutations and post-fault progress. Fail on the first unexpected error; never continue the remaining soak after failure. CLI controls also prove every supported journey is reachable through `xenon`, help/config/version perform no backend initialization, config precedence is consistent, stdout JSON is clean, diagnostics use stderr, and cancellation/exit codes agree across commands. Required controls prove Ctrl-C/budget is not reported as a pass, expected injection is classified narrowly, blocked workload notices asynchronous failure immediately, cleanup preserves original inputs/first cause, and fresh workload generation does not perturb saved replay. Fast regressions and short smoke enter CI; continuous and ten-minute profiles are opt-in local commands.

## 9. Capacity evidence, clean-checkout delivery and integration

Dependencies: steps 1–8. Requirements: [100-instance target](architecture-spec-in-progress.md#product-direction-and-accepted-constraints), [benchmarks/release](architecture-spec-in-progress.md#release-and-benchmark-loop), [reproducibility](../handoff-autonomous.md#definition-of-shipped).

Own `benchmarks/{placement,storage,workflows}/`, target-100 declarative topology/resources profile, `.github/workflows/ci.yaml`, README, `docs/design/verification-matrix.md`, `docs/operations/`, release packaging and accurate existing website results. Measure placement/movement, control contention, per-instance engine plus embedded Temporal memory/CPU, pooled-connection costs, requests and progress for the approximately 100-instance target. Scale control simulation alone is not service-scale acceptance. Run a staged real topology up to 100 where existing authorized resources permit; otherwise retain an executable profile and exact resource blocker/observed maximum with target status unproven. Do not spend on new infrastructure without authorization.

Named workflow benchmarks report durable-write versus end-to-end p50/p95/p99, completion rate, visibility lag, recovery time and S3 requests/bytes per completed workflow. Separate idle coordination, compaction/maintenance, retained storage and amortized workload costs. If publishing cost estimates, verify dated region/storage-class prices and distinguish S3 from full deployment cost. Publish only evidence-linked results. NFS/SMB registry qualification and native engine support remain separate; a filesystem registry pass does not establish a filesystem-backed SlateDB deployment.

```sh
python3 scripts/prove.py capacity-target-100
python3 scripts/prove.py workflow-benchmarks
python3 scripts/prove.py clean-checkout-release
```

The clean-checkout gate creates an isolated checkout of the exact reviewed commit and executes documented pinned setup, build, backend/native contracts, DST regressions, short real smoke, ten-minute profile and teardown without inherited `.local` binaries or unsaved files. The reviewer exercises the documented single `xenon` CLI for startup, dev topology, inspection, tests/search/replay and teardown; a Python-only reproduction does not pass the user-journey gate. Internal scripts may remain implementation helpers. Its manifest declares necessary external tools/resources; unavailable dependencies are blockers, not skips counted as passes. Verify API regeneration is clean, artifacts contain source/input/config/tool/image/native hashes, first failure is immutable, and no run-owned resources remain. A separate reviewer follows the README commands and checks delivered code/spec/evidence and feature qualification matrix. Integrate and rerun on the final actual head; green earlier branches do not prove final integration. If hosted CI is still billing-blocked, record exact platform evidence and completed local equivalent checks without inventing CI success.

## Completion and handoff contract

Each slice records its exact source, accepted review findings, commands/results, negative controls, remaining limits and linked issue evidence. Clean manifests retain expanded workload/fault bytes, source/native/tool/image pins, configuration and input hashes, complete trace, recovery assertions and scoped cleanup results. Results explicitly distinguish model, backend, native and real-stack coverage.

Plan completion triggers execution of step 1, not a planning-only final response. Final delivery requires the integrated executable and exact service layout, reviewed readiness and ID/data migration, real registry/native contracts, coupled DST plus replay/minimize/continuous search, fail-fast first-cause cleanup, short CI smoke, ten-minute actual Nexus/Omes proof, benchmarks/100-instance target evidence or precise resource blocker, developer/operations documentation, packaging and authorized integration. Unsupported environments/features and open external gates stay visible. Never convert a reduced profile, accepted configuration, debate or source review into full support or production-readiness claims.

Independent Astra plan review approved execution after the authority-cutover exclusion/recovery gate and mandatory single-CLI journeys were added. The coordinator accepts this review; runtime acceptance remains to be executed.
