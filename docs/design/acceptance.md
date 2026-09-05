# Declarative acceptance contract

Delegated advocate/reviewer decision, 2026-09-05. These are required minimum proof budgets, not measured performance or a statistical reliability certification. Implementation is pending.

## Reproduction is a gate

Commit declarative environment, workload, event-triggered fault schedule and assertion manifests. Pin images by digest and all source/tool versions. Store the actual generated fuzz binaries with hashes, not only seeds. A single clean-checkout entrypoint preflights versions/platform/resources, builds, starts the isolated stack, executes injections and assertions, emits a machine verdict, and tears down only that run's resources on success or failure. Retain declared evidence/object data deliberately. Dirty development runs cannot produce release proof PASS.

Evidence includes source revision and dirty=false, configuration/corpus hashes, image/tool versions, observed fault trigger timestamps, all started workflow IDs and Continue-As-New run chains, raw RPC invocation/attempt durations, S3 operation counts/bytes, visibility lag, CPU/RSS and recovery timeline. A missing scheduled injection fails. Omes exit zero alone does not pass. Fixed inputs/barriers are repeatable; random concurrency scheduling stress is labelled separately.

## Workloads and topology

Pin Omes `c6978ba39aa03551ce28974117e8d7ecf983d2b3`, SDK 1.48.0, against Temporal 1.31.2. Test public compatibility before interpreting scenarios. Capture capability discovery and ensure expected branches ran.

- 100 iterations of `workflow_with_single_noop_activity`.
- 40 iterations of `throughput_stress`, maximum concurrency 4 and timeout 900s. Options: `internal-iterations=4`, `continue-as-new-after-iterations=2`, `include-retry-scenarios=true`, `include-describe=true`, `sleep-time=100ms`, `visibility-count-timeout=60s`.
- Verify histories actually contain required activities, timers, signals, retries, child workflows, updates and Continue-As-New. Enumerate every started workflow and final result/run chain.
- Generate and commit 20 fuzz binary inputs, generator configuration and hashes; replay with and without faults. The selected fuzzer supports seed/output-file and input-file, but seed alone is insufficient.
- Two Temporal instances must each serve requests. Start with two Xenon nodes, add a third at a declared completed-operation barrier under active workload. Move at least two partitions including history and visibility, and observe at least 10 successful served operations on every Xenon node.
- 2,000 frozen visibility records over all four partitions, page sizes 1, 7 and 100; compare exact list/count/group sets through movement. Run separate concurrent-mutation tests. Every selected type/operator, null/missing rule, CHASM/division/schema surface requires fixtures and API/UI assertions independent of Omes.

## Correctness and progress budgets

Zero lost acknowledged persistence operations and zero false condition successes. Every acknowledged operation has an independent recoverable-state oracle. Ambiguous responses reconcile; neither timeouts nor later conflicts imply rollback or success.

Steady-state RPC invocation p99 <=5s and maximum <=30s; visibility convergence <=60s. Measure retries in invocation duration and expose attempts separately. Per-family p99 requires >=100 observations; otherwise report insufficient sample and collect more wherever p99 is a mandatory gate. Declare steady/fault measurement windows before running.

Mixed workload completes within 900s. After the final injected fault, affected work resumes within 120s and drains within 300s. Deadlines are acceptance budgets, not user-facing time estimates. Do not widen budgets after a failed run to manufacture a pass; investigate and record any justified prospective decision change.

## Fault and storage coverage

Commit/await-durable/ACK boundaries, lost RPC and directory-CAS responses, stale routes, competing and delayed owners, serving-process kills and empty-local-cache recovery require deterministic triggers and assertions. Repeat with actual normal WAL/manifest/SST GC and compaction execution; keep WAL fence retention and metadata boundary protections required by the failure model.

The real-S3 profile changes only storage endpoint/bucket/prefix/secret injection. Reuse the same workloads, fault schedules and oracles. Missing authorized real-S3 access is a blocking external prerequisite, never an emulator pass.

## Harness choice and sequence

Use Compose initially: required processes do not depend on Kubernetes behavior. The declarative stack contains S3 emulator, two Temporal instances, two initial Xenon nodes plus a third scale-out service, unchanged pinned UI and pinned runners. PostgreSQL is a test-only visibility oracle and must never be a Xenon durable dependency.

Implement in vertical slices: typed wire contract and shard operation; execution/history; metadata/matching; real workflow; visibility; routing/ownership controller; live scale-out; full faults/oracles; repeatable real S3; independent clean reproduction and packaging. Each slice has a bounded ticket and executable gate. The complete stack is not implemented merely by listing services here.
