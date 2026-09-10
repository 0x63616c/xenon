# Shared production replay scenario

Run from a clean checkout using the pinned Go toolchain:

```sh
GOTOOLCHAIN=go1.27.1 go test -count=1 -v ./internal/persistence ./internal/simulation
```

`replay_test.go` contains the exact fixed schedule and operation bytes: increment
with ID `stable-operation`, durably commit state and outcome, drop the response,
discard volatile staging, reopen durable state, retry, then reject changed input.
There is no random seed or wall-clock scheduling in this scenario. The independent
oracle expects value one, one outcome, and a nonempty committed replay barrier.
The negative control deliberately loses the outcome while persisting effects;
the same oracle detects the resulting double application.

Both the simulator and `node.Owner.journal` invoke `persistence.RunReplay`. Production wraps
its existing native transaction, accounting, family check and process-cut commit
hooks; the scenario supplies an atomic in-memory durable store and controlled
crash. Native handles and Owner admission are unchanged.

`coordination_test.go` adds the first coupled deterministic proof. It loads the
committed schedule at
`test/scenarios/simulation/lost-response-crash-move.json` and drives production
`Join`, `TopologyStore`, `Membership.Step`, `Router.Interceptor`, and
`persistence.RunReplay`. The schedule loses both bounded forwarding responses, crashes the
owner, advances logical time to eviction, and retries through the same endpoint.
The independent checks require one mutation, the original durable result,
movement to the surviving owner, changed-input rejection, stale-owner rejection,
preserved forwarding fields, and an exact event trace, which is emitted into the
retained Go test log on success. Negative controls use the same independent
checkers to reject a missing atomic outcome and a stale-owner acknowledgement.

Run it with retained provenance from a clean checkout:

```sh
python3 scripts/prove.py simulation
```

The coupled proof uses in-memory conditional-S3, atomic durability, node
lifecycle and admission models. It does not exercise production Manager fencing,
native SlateDB lifecycle or real-S3 behavior. Topology transition UUIDs come from
the recorded deterministic source; production retains random UUIDs. The fixed
schedule has no random choices, so seed zero records that fact. Production
directory deadline timers, workload generation, schedule minimization, native
durability-completion ordering, and broader schedule coverage remain open.

For retained evidence, save the exact source revision, hash this schedule and
`internal/persistence/replay.go`, record `go version`, and retain verbose test output.
The integrated declarative proof runner must attach those identities before this
slice can contribute a reproducible release gate. A passing ad hoc test alone
must not be labelled full simulation acceptance.

## Shared bounded search/replay foundation

`Search(ctx, SearchConfig, Generator, *Runner)` and
`Replay(ctx, scenarioArtifactPath, *Runner)` now share a sequential case runner.
`NewCoupledCorpus(savedScheduleBytes...)` and `CoupledDriver` are a concrete
consumer for the production `cluster.Step`/`partitions.Step` scenario above.
The corpus is finite and returns EOF; it does not pretend that repeating a fixed
schedule generates new Temporal, Omes or Nexus workloads. `ExpandCoupled` splits
that schedule into explicit workload, topology and ordered event bytes. Logical
actor ticks and effect delivery order are unchanged. The existing independent
checker asserts recovered-owner progress and no unsettled effects before the
coupled driver can settle successfully.

Callers provide a new run directory, the actual executing source revision and a
version map with `toolchain`, `native`, and `images` entries (explicit `modeled`
and `none` values are appropriate for this driver). These are caller-supplied
provenance, not auto-discovered or independently verified build attestation.
The historical source revision inside a saved schedule is retained separately.
Generator version, SHA-256 and capabilities are mandatory. The coupled corpus
hashes its embedded generator/driver source file, not just its input corpus. Workload and fault
seeds are derived independently using labeled SHA-256 streams and saved per case;
the fixed corpus consumes neither stream. A generated-workflow implementation
and additional capability validation remain future work.

`MaxInFlight` must be 1. All operation/payload/depth, trace, case/time, settle and
cleanup limits are explicit. Zero `MaxCases` requires `Continuous=true`, which
still requires a duration budget. `Clock` and `Timer` are injected; `WallClock`
is the real-time implementation, and tests drive manual time. There is no queue
of speculative cases: the next generator request follows successful recovery
assertions and cleanup. Driver validation must be pure and bounded; it must not
allocate external resources. Drivers must treat scenario bytes as immutable and
honor cancellation. Cleanup must safely stop/drain outstanding Run/Settle work.

Each run saves configuration and provenance, then each request and the exact
expanded scenario with its hash before driver launch. Evidence files are
create-only; directories/files are synced, and observations append to a bounded
synced JSON-lines trace. Observations remain valid through settling and cleanup;
the error latch is checked after every phase and atomically when sealing the
trace, so ignored observation errors cannot count as a passing case. The first observed run/settle failure is synced before
cleanup starts. Cleanup errors are secondary and cannot turn a failure into a
pass. Unresolved goroutines report `ErrPending` (process exit required), with no
new case launched and late trace writes refused. The runner cannot safely kill
arbitrary Go/native work and does not claim to do so. Use a fresh runner/driver
and directory after a failed run; cleanup never deletes evidence or shared data.

Replay verifies the artifact's expanded scenario hash and loads those bytes and
original RNG identities, without calling the original generator. It writes a new
artifact and trace with the replaying source/tool provenance; it never overwrites
the failure. `Completed` counts only cases that ran, settled and cleaned. `budget`
means no failure was found in the completed explored prefix; an unfinished case
is not counted. `canceled` is not a pass. Generation/validation errors are recorded
separately from execution failure phases and stop the search.

Tests cover exact saved replay after generator mutation, byte tampering,
independent RNG streams, unsupported combinations, invalid concurrency/budgets,
first-failure preservation, ignored trace errors, failed recovery, manual-time
cancellation/drain, blocked cleanup and evidence overwrite refusal. These APIs
are not yet wired into `xenon` commands. General randomized event scheduling,
real-stack drivers, Omes/Nexus input generation, minimization and full #119
acceptance remain open.

## Concurrent resident workflow component

`xenon test workflow` accepts `--workflows-per-case` (1..16) and
`--workflow-concurrency` (1..workflows-per-case). Defaults remain one root.
Every case is expanded and saved before execution. A case contains independently
seeded workflow inputs; `BatchWorkflowDriver` gives each member an isolated
`WorkflowRuntime`, bounded worker admission, and its own evidence directory.
Only one case is in flight, and all its members must settle and clean up before
the next case starts. Legacy single-root resident artifacts use the same driver.

Member runtimes must match the tool and fixture pins captured at initial CLI
binding. Failure callbacks are bound to a phase generation, so a late callback
cannot contaminate later phases or cases. Already admitted members are canceled
and drained after failure; no later member is admitted.

The component tests exercise three sequential cases, four roots per case,
concurrency limits of one and four, queued admission cutoff, stale callbacks and
changed tool metadata. This does not yet prove GEN-02: real Temporal overlap,
complete child/Continue-As-New/Nexus census and fan-out limits still require the
real acceptance journey. The component declares no injected faults.
