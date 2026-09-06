# Service contracts and deterministic testing

Status: independently reviewed implementation contracts, 2026-09-06. This document implements the direction in [the interview checkpoint](architecture-spec-in-progress.md); the review record below accepts bounded interface/protocol choices for planning and implementation experiments, not unexecuted runtime guarantees. Current delivery authorization supersedes the checkpoint's historical implementation pause. This document is not runtime acceptance evidence.

## Scope and required layout

Keep one Go executable, identical instances embedding Temporal, an optional coordinator role in each instance, direct S3 SlateDB durability, compatible SDK/UI/Nexus, and disposable local state. Essential coordination must work while Temporal and every data database are unavailable. Target approximately 100 instances; capacity remains an experiment.

The user-approved directories are mandatory:

```text
cmd/xenon/main.go
internal/app/
internal/cluster/
internal/partitions/
internal/partitions/slatedb/
internal/persistence/
internal/routing/
internal/temporal/
internal/temporal/adapter/
internal/registry/
internal/registry/s3/
internal/registry/filesystem/
internal/registry/contracttest/
internal/identity/
internal/simulation/
api/xenon/v1/
test/scenarios/simulation/
test/scenarios/integration/
benchmarks/placement/
benchmarks/storage/
benchmarks/workflows/
docs/design/
docs/operations/
```

Migrate existing implementations in tested slices, preserving existing operational links. Do not create empty scaffolding. Colocate service/controller tests. Put shared registry adapter suites in `internal/registry/contracttest`; persisted schedules, faults, topology, workload inputs and minimized regressions in `test/scenarios`. Keep native types in `partitions/slatedb`, Temporal persistence types in `temporal/adapter`, protobuf transport definitions in `api/xenon/v1`. Generated bindings placement under that API tree must be resolved with generation/import migration in the plan.

`app` owns composition, process contexts, health and reverse-order shutdown. `cluster` owns membership observations, coordinator election, placement and bounded move scheduling. `partitions` owns each local writer's acquire/open/activate/drain/retire state. `persistence` owns atomic operation semantics and replay. `routing` owns origin retry and connection pooling; `temporal` embeds upstream services and its adapter translates operations. Routing and persistence do not need background goroutines simply to qualify as services.

## Events, effects, time and lifecycle

Use concrete package-local event/effect types, not a universal bus or plugin framework. The following Go signatures specify the shape; domain payload types are defined in their owning package and are not `any` bags:

```go
// internal/partitions/controller.go; cluster has its own concrete Step.
func Step(s State, e Event) (State, []Effect)

type EffectID uint64 // incarnation-local sequence, never a persisted identity

type Event struct {
    At Tick
    Incarnation identity.IncarnationID
    Kind EventKind
    Effect EffectID
    Assignment Assignment
    Registry RegistryCompletion
    Engine EngineCompletion
}
type Effect struct {
    ID EffectID
    Kind EffectKind
    Timer TimerRequest
    Registry RegistryRequest
    Engine EngineRequest
}
```

`Kind` selects exactly one valid payload. Constructors/validation reject malformed combinations. State contains the pending effect table and incarnation; completions include both so old-process or duplicate delivery cannot resurrect an owner. Step consumes no context, clock, randomness, goroutines or I/O. It returns ordered effects; sort IDs before converting maps into decisions. The simulator calls these same production functions. Cluster planning is a pure snapshot/policy calculation; it does not open engines.

Effects cover registry read/create/replace, timer set/cancel, engine open/close/transaction/durability, outbound transport and caller reply. Completion is an explicit event, including failure. Separate effect request, volatile application, durable commit and response delivery; a single success callback cannot represent all four phases in simulation. The first implementation need only express the coupled coordinator-replacement-during-move slice, then extend these concrete types as behaviors require.

```go
// Service drivers and bounded goroutine components only.
type Tick int64 // process-local monotonic nanoseconds since incarnation start
type Clock interface {
    Now() Tick
    NewTimer(time.Duration) Timer
}
type Timer interface {
    C() <-chan Tick
    Stop() bool
}
```

All behavior-affecting time in Xenon component tests and DST must use injected time: membership observation, election renewal/expiry observation, polling, retry/backoff/jitter, readiness, ownership deadlines, admission, native completion watchdogs, RPC budgets, shutdown and workload schedules. Implement repetition with one-shot timers so no separate ticker seam is needed. Timeouts inside the deterministic controller are timer effects. Bounded goroutine drivers use this clock or a verified `testing/synctest` bubble under the pinned toolchain. Ordinary `context.WithTimeout` backed by wall time is not an acceptable fake-clock implementation: a clock-aware helper creates a cancelable context and owns/stops its injected timer. Caller cancellation is a separate injected event. Real timestamps for evidence display must not influence decisions.

Event.At and Clock.Now are local Tick values. A separate wall-clock read at the transport/evidence edge supplies Unix timestamps; it must not decide ownership. DST stores ticks only together with their process incarnation and simulated clock mapping, never as cross-process comparable time. Production uses monotonic elapsed observations for suspicion; serialized wall times are diagnostic unless explicitly part of a transport deadline. DST represents process clocks explicitly and schedules jumps/skew where relevant. Process-local monotonic ticks must never be persisted or transmitted as timestamps comparable across processes. No shared synchronized clock is required for election safety below. Native SlateDB/Rust timers, actual OS/network behavior and embedded Temporal internals are real in backend/real-stack tests; those are explicitly outside deterministic scheduling and must not be advertised as mocked. Model their externally visible completions in DST, qualify the model with real tests. Audit all direct `time.*`, context deadlines and random calls in migrated Xenon packages; list each retained real-time call and its integration-only role.

A service driver owns started effects until completion, even after the requesting caller times out. Before dispatch cancellation means known-not-started. After dispatch cancellation stops waiting/delivery, not an assumed rollback. Late completions update internal reconciliation and release resources exactly once; they cannot deliver a second response or activate a superseded incarnation. A timed-out native operation retains its handle and per-partition exclusion until native completion. Quarantine that writer against new admission; never close a handle still in use. App shutdown stops admission, cancels timers/network waits, drains with a bounded budget and reports outstanding effects. If native shutdown cannot finish, process termination is the safe containment boundary; it is not proof the operation failed to commit.

## Identity and randomness

```go
// internal/identity/ids.go
// Distinct named string types: ClusterID, NodeID, IncarnationID,
// PartitionID, OperationID, TransitionID.
type Source interface { NewID(prefix string) (string, error) }
```

Proposed format: `<prefix>_<22 base62 characters>` representing a uniformly sampled 128-bit value with canonical fixed-width encoding. Prefixes: `clu`, `nod`, `inc`, `prt`, `op`, `trn`. Production samples cryptographic entropy with errors propagated; simulation injects and records bytes. Validate type-specific prefix and encoding at ingress. These are proposed delegated choices, not claims that Calum selected the vocabulary. Ephemeral effect sequence numbers, counters, SHA-256 digests, opaque backend versions and required Temporal/user IDs are not Xenon-owned resource IDs.

Create incarnation once per process start, transition once per logical attempt, operation once before first submission. Retries retain IDs. Random placement inputs/jitter use a separate injected seeded source; controllers receive the recorded choice as an event or precomputed input. Changing ID generation must not perturb fault scheduling. Existing UUID/nonconforming references require a format inventory, read compatibility and explicit persisted migration; do not silently rename a populated partition or rewrite Temporal IDs.

## Registry: conditional publication and ambiguity

```go
// internal/registry/store.go
// Version is opaque, scoped to key; callers only test equality.
type Key string
type Version string
type Record struct { Body []byte; Version Version }
type Write struct {
    Transition identity.TransitionID
    Digest [32]byte // canonical body + key + expected condition
    Body []byte
}
type Store interface {
    Read(context.Context, Key) (Record, error)
    Create(context.Context, Key, Write) (Record, error)
    Replace(context.Context, Key, Version, Write) (Record, error)
}
type Conflict struct { Key Key }
func (*Conflict) Error() string

type UnknownOutcome struct {
    Key Key
    Transition identity.TransitionID
    Cause error
}
func (*UnknownOutcome) Error() string
func (*UnknownOutcome) Unwrap() error
```

Use typed `NotFound`, `Conflict`, `UnknownOutcome`, `Unavailable`, `Invalid` and `Corrupt`; inspect with `errors.As/Is`, never string matching. `Create` means absent only; `Replace` requires a nonempty exact observed version. Success means whole-record durable publication and a returned version; failed conditional checks mean that attempt did not publish. Disable hidden SDK retries of conditional mutations, or reconcile aggregate attempts: commit plus lost response plus automatic retry conflict is `UnknownOutcome`, never a definite `Conflict` without matching-transition readback. A request possibly sent but with uncertain completion returns `UnknownOutcome`, including cancellation after dispatch. `Unavailable` means known not to have published. Reads return a complete coherent record at a linearization point, not necessarily the latest value at response delivery. Return owned byte copies. These error guarantees describe the aggregate Store method, not merely the final SDK attempt; an earlier ambiguous attempt dominates a later conflict unless reconciled.

The backend must provide linearizable per-key read/CAS, durable successful replacement and no version ABA across accepted changes, including identical payload replacement. Each replacement therefore includes a fresh transition identity/revision; a content-only ETag must not be treated as a globally monotonic counter. No multi-key transaction, list-snapshot, lease or unconditional overwrite is assumed. S3 uses conditional requests; filesystem publication requires locks shared by every contender, fsync of file and containing directory, and crash-safe rename while holding the stable lock. The lock inode is separate from the replaceable data inode; reread and compare the current record under that lock before publication. Initial filesystem Read also takes the stable exclusive lock, validates the complete record and establishes file plus directory fsync durability before returning success. This deliberately handles a writer crash after rename but before directory fsync, which releases its OS lock without completing publication durability. A read may make that previously unknown publication durable; failed fsync returns an error, never success. Contract tests must include that exact crash window. A more efficient shared-lock read path requires a separately verified recovery/publication protocol. NFS/SMB are neither automatically excluded nor qualified: run the same cross-process/cross-machine tests on declared mount/client/server configurations. Separate local directories are not shared storage.

Store the transition ID/digest in the published record envelope. On ambiguous completion, reread: matching transition and digest proves this publication; matching ID/different digest is invalid reuse; original version still present permits retry with the *same* condition/identity/body. A later transition is not proof that this one succeeded or failed. Reconcile desired state from that later version; never report historical success from a guess. For a transition whose historical outcome must remain queryable, retain its receipt in the same authoritative record until all dependents have resolved it. No unbounded generic registry journal is required. Conflicts after an earlier unknown attempt do not erase that ambiguity.

## Coordinator authority and ownership atomicity

Proposed initial protocol: one bounded **control record** contains coordinator incarnation and renewal sequence, assignment revision, desired assignments, and each partition's reservation/generation/ready owner. Member heartbeat records remain separate advisory observations. This co-location is a deliberate bounded implementation for approximately 100 instances; measure record bytes and CAS contention before choosing partition count. It eliminates cross-record authority checks followed by unguarded plan writes. Do not split this record for efficiency without a separately reviewed fencing protocol.

Election initially conditionally creates control. A contender observes the same leader identity/renewal sequence unchanged for the configured elapsed suspicion interval, then rereads and CASes the exact latest version, incrementing the coordinator generation and changing incarnation. Any observed renewal resets suspicion; unrelated control edits do not. Concurrent renewals/takeovers serialize via CAS. Suspicion can be wrong without violating control-record safety; it may cause disruption. A resumed former leader can only update a snapshot naming itself leader, and must CAS that snapshot's version. Takeover makes its previously prepared effect conflict. After rereading another leader it steps down; it must not adopt the new version and write as old leader. Renewal cadence, suspicion interval and jitter are explicit profile inputs to benchmark, not a dependency adoption.

Only the named coordinator publishes assignment changes. Owner controllers may CAS only their own reservation/readiness fields, preserving all other fields, while validating the exact assignment/incarnation/generation from that same snapshot. A coordinator turnover need not invalidate unchanged ready owners. A reservation records a unique transition, desired owner incarnation, assignment revision and incremented partition generation; engine open begins only after reservation publication is confirmed. Start native open at most once per reservation, even on timeout/cancellation; retain its pending effect until completion. A new attempt requires a new valid reservation after reconciling the old one. An ambiguous reservation must be reconciled before opening.

After open/recovery and native fencing, the opener rereads control, validates its still-current assignment revision/reservation (including A-to-B-to-A assignment changes) and conditionally publishes ready. Concurrent assignment changes force a conflict and retire the open handle. Cached ready views are hints; local admission independently validates current authority and executes through the fenced engine. Coordinator generation, partition generation, Temporal RangeID and native writer epoch are distinct values with distinct jobs.

The control record **cannot fence native data writes**. An old opener paused before native open can later fence a newer writer even if its registry CAS will fail. Quarantine the displaced writer on native fencing errors; the stale opener closes without publishing ready and does not endlessly retry its obsolete reservation. The current controller obtains a new valid reservation and recovers. Require eventual progress after finitely many such stale opens complete; do not claim availability under infinitely paused/resuming contenders. Native experiments must establish that a fenced writer cannot produce a new durable commit, and that successful pre-fence commits remain recovered. A response delivered after takeover may legitimately report a pre-takeover durable result; the checker must distinguish commit authority from response time.

## Engine and persistence seam

```go
// internal/partitions/engine.go; no native SlateDB types escape.
type Engine interface {
    Open(context.Context, OpenRequest) (Writer, error)
}
type Writer interface {
    Begin(context.Context) (Transaction, error)
    AwaitDurable(context.Context, CommitReceipt) error
    ReadDurable(context.Context, ReadRequest) (ReadResult, error)
    Close(context.Context) error
}
type Transaction interface {
    Get(context.Context, []byte) ([]byte, error)
    Scan(context.Context, ScanRequest) (ReadResult, error)
    Put([]byte, []byte) error
    Delete([]byte) error
    Commit(context.Context) (CommitReceipt, error)
    Abort() error
}
```

`OpenRequest` binds stable database path, partition, assignment revision and reservation incarnation/generation. The driver associates returned handles with one effect and closes superseded handles. `CommitReceipt` is scoped to a writer and batch; receipt success alone is not durability. A Transaction provides one consistent read view and atomic writes. Persistence reads and checks Temporal conditions and stages operation effects plus replay digest/result and replay accounting in that same transaction. The initial per-partition serialized admission excludes other transactions through durability; do not drop that exclusion without proving point/range conflict semantics. Transaction ownership stays with the native driver through Commit completion, then the receipt owns any pending durability wait. Abort is only valid before commit dispatch; it cannot roll back an uncertain commit. Do not flatten these read/condition/write semantics into blind writes. `AwaitDurable` must refer to this mutation's commit. A timeout/engine I/O error after submission is an unknown result, not absence. A native fence error permanently retires that writer handle.

`ReadDurable` must not expose volatile writes that another caller could act on and lose after crash. Start with existing per-partition serialized admission and a proven durable barrier/snapshot policy, retaining atomic transaction domains. Replay verifies the canonical digest and operation family, then obtains a nonempty native durability/authority barrier before returning a stored result. Replay must never reapply a mutation or reinterpret an unknown commit as a new ID. Keep existing bounded replay capacity with explicit capacity error; expiry/GC requires a separate safe retry-horizon protocol. No extra application batching until measured evidence requires it.

## Transport and request budget

```go
// internal/routing; protobuf equivalents live in api/xenon/v1.
type Envelope struct {
    Protocol uint32
    Partition identity.PartitionID
    Operation identity.OperationID
    Digest [32]byte
    DeadlineUnixNano int64
    ExpectedIncarnation identity.IncarnationID
    ExpectedGeneration uint64
    Forwarded bool
    Method string
    Payload []byte
}
type Transport interface {
    Invoke(context.Context, string, Envelope) (Response, error)
}
```

The adapter/origin creates ID, canonical method+partition+payload digest and finite deadline once. Only expected owner metadata changes on retry; addresses never enter durable application identities, digests or pagination tokens. Origin retries at most three owner attempts under one nonextending caller budget; this bound is a proposed default. Recompute remaining transport timeout from the origin's monotonic budget; preserve absolute envelope deadline and clamp at the destination to its context deadline. Clock skew can reduce availability, never extend the origin's response acceptance window. DST drives budget exhaustion explicitly.

Receiving a forwarded request never forwards again. Before execution, a stale incarnation/generation returns typed `StaleOwner` with optional hint and known-not-executed status. Origin refreshes authoritative routing, treats hints only as lookup aids and retries. An initial stable-endpoint request may originate this loop; the `Forwarded` bit prevents recursive chains. Owner validation and engine fencing still apply on local dispatch. Preserve typed Temporal conditional errors, digest errors, capacity/fenced/stale errors and `UnknownOutcome`. Only safe known-not-executed failures or replay-protected unknown outcomes may retry; never blindly interpret every `Unavailable` as stale routing. Connection pools are bounded and reused.

An expired caller gets no later success delivery. A durable completion after caller expiry remains replayable and is part of history. Transport cancellation cannot transfer native handle ownership back to the caller or permit another unsafely overlapping operation.

## Independent checks, schedules and runner

The simulator executes production Steps and persistence replay logic but maintains an independent observation history/checker. The checker must not call production ownership eligibility, digest/replay decisions, placement validation or success predicates to decide correctness. Record externally observed requests/results, modeled durable batches, native writer epochs, control-record linearization and crash/recovery. Check: acknowledged effects survive recovery; each ID/digest applies at most once; changed digest never applies; atomic data/outcome pairs recover together; obsolete native epochs cannot newly commit; stale assignments cannot become ready; recursive redirects never occur; budgets do not reset; and required work completes within a declared logical bound after faults stop and fair delivery resumes.

Mandatory first schedule is coordinator takeover during a move, including delayed old plan publication and delayed stale engine open. Add renewal/takeover races, ambiguous control writes, simultaneous contenders, crash around every reservation/open/ready phase, lost durable response followed by retry at new owner, joins during moves, storage interruption and whole-cluster cold restart. Negative controls deliberately allow stale-plan publication, post-fence commit, missing replay result, deadline reset and duplicate application; the same checker must fail each. Native contract suites separately validate model assumptions, including compaction/GC behavior where relevant.

Saved artifacts include full ordered event/effect/completion trace (not just seed), exact workload bytes, topology and fault schedule, separate RNG streams, source revision plus dirty patch hash if present, config/input hashes, tool/image/native versions, assertions and first failure. Replay consumes the saved event sequence and fails explicitly if effect preconditions no longer match. Minimization retains a replayable failure with the same invariant, stores the smaller schedule as a fast regression, and never deletes the original failure. Benchmark execution cost before setting a random-schedule count.

Runner behavior is part of correctness:

1. Validate manifest/tool pins and allocate a unique run ID. Create run-scoped process/container/network/storage prefixes and an evidence directory outside disposable resources. Write provenance before starting work.
2. Classify expected injected faults by exact schedule step, target, window and allowed result. An expected process kill or selected transport failure does not fail the runner; an invariant violation is never expected. Unmatched crashes, assertion failures, setup errors and exceeded recovery budgets are unexpected failures.
3. Monitor invariant, process-exit and deadline channels concurrently with active workload commands; do not wait for a long-running command to finish before noticing a failure. On the first unexpected failure, atomically latch the primary failure, stop new workload/fault steps and cancel outstanding orchestration. Immediately persist the trace, schedule and available logs; bounded best-effort diagnostics cannot delay cleanup indefinitely. Do not continue remaining rounds to collect more failures.
4. Always enter scoped teardown on setup/run failure, interruption or success. Stop only owned child process groups and run-labeled containers/networks; remove only the run's explicitly allocated disposable resources. Retain evidence and original inputs. Record cleanup errors separately without overwriting the primary failure. If cleanup fails, mark the run failed and report exact remaining resource IDs.
5. Exit nonzero on any invariant/unexpected/cleanup failure. Expected injected faults pass only if post-fault recovery assertions pass. Outer CI timeout remains a final guard, not the normal cleanup mechanism.

The local release target remains ten minutes of Omes fuzz with actual Nexus and bounded agent add/stop/restart schedule, execution/history/visibility checks and post-fault progress. It is not the normal CI gate. Fast regressions plus short real-stack smoke belong in CI. Real AWS, broad upgrade matrices and managed service work remain outside this first-release task; local S3-emulator success is not AWS qualification.

## Continuous failure search and replay

Provide an explicit opt-in continuous search mode alongside the bounded ten-minute profile and fast CI regressions. It generates new workloads/inputs until the first failure, user cancellation or declared wall/logical/work budget. A budget ending without failure means “no failure in this explored prefix,” not exhaustive correctness. Failure search reuses the production seams, common schedule format, independent checker and fail-fast runner above.

```go
// internal/simulation; real-stack driver consumes the same scenario envelope.
type Generator interface {
    Next(context.Context, GenerateRequest) (Scenario, error)
}
type GenerateRequest struct {
    Index uint64
    WorkloadSeed uint64
    Limits WorkloadLimits
}
type Scenario struct {
    Version uint32
    Workload []byte // expanded canonical operations/workflow inputs
    Topology []byte
    Faults []byte   // expanded schedule, never an opaque seed alone
}
type SearchConfig struct {
    MaxCases uint64 // zero only in explicit continuous mode
    MaxDuration time.Duration
    MaxInFlight int
    SettleBudget time.Duration
}
type RunResult struct {
    Completed uint64
    StopReason string // first_failure, canceled, budget, completed
    EvidencePath string
}
func Search(ctx context.Context, cfg SearchConfig, gen Generator,
    runner *Runner) (RunResult, error)
func Replay(ctx context.Context, artifactPath string,
    runner *Runner) (RunResult, error)
```

`WorkloadLimits` bounds operations, workflow depth, payload bytes and allowed features. Start with valid supported Omes fuzz inputs and named workflow profiles, including compatible Nexus combinations in real-stack mode. Do not promise arbitrary SDK workflow-language synthesis. Reject unsupported generated combinations before execution and record generation errors separately; invalid inputs must not consume a passing case. Generator version/hash and capability manifest are part of evidence.

DST generates persistence/coordination workloads and also schedules logical time, messages, registry commits/responses, native-engine modeled completions and lifecycle faults. It does not simulate all Temporal or real workflow execution. Real-stack mode submits expanded Omes/workflow inputs to actual embedded Temporal and SDK workers, exercises Nexus and verifies history/visibility; actual native/network time remains real. Both modes retain exact expanded workload/fault bytes. DST additionally retains every chosen event order; real-stack records actual observed order/times and labels replay as the same workload/fault intent, not a guarantee of identical OS scheduling.

Use separate recorded workload-generator and scheduler/fault RNG streams; a workload change cannot silently reseed event exploration. Persist scenario bytes and hashes before launching it, then append effects/observations during execution. On failure, flush the partial trace, source/tool/native/image versions, generator/version/input hashes and both stream identities, with first-failure evidence. Replay loads expanded artifacts rather than invoking the generator again; a changed generator cannot alter a saved regression.

Start with `MaxInFlight=1` scenario, using bounded operation concurrency within it. A bounded queue applies backpressure to generation; stop generation immediately when the primary failure latch trips. Each case has declared fault and healthy settle phases: stop injecting faults, drain accepted work, restore declared healthy topology/storage, advance logical time or wait only to the settle budget, and run recovery/durability/progress assertions. A case is not passed merely because submission returned. Continuous mode may reuse a stack for efficiency only with explicit per-case namespace and observed pre/postconditions; resource growth and unresolved work must remain bounded. New failures stop all outstanding submissions, monitor late completions until bounded teardown, preserve evidence, and clean only run-scoped resources. User cancellation uses the same cleanup path and records canceled rather than passed.

## Source evidence and unresolved experiments

Inspected baseline: `fc4180d9b73c7d39dcdc0790f8d876ee10798ea1`, with existing uncommitted readiness work visible. Source evidence is not proof that proposed behavior exists:

- `internal/ownership/membership.go`: Step already accepts explicit time; manager polling and native admission/close in `internal/ownership/manager.go` and `internal/node/shard.go` still use real timers.
- `internal/routing/router.go`: current interceptor forwards recursively under two-hop/two-attempt bounds and broadly retries `Unavailable`; migrate to origin-only typed redirects.
- `internal/replay/replay.go`: shared durable replay decisions, changed-digest rejection, nonempty replay barrier and bounded capacity are reuse points.
- `internal/node/shard.go` and `internal/node/journal.go`: native commit waits on `AwaitDurable`; caller timeout quarantines the writer while native work retains its gate. Preserve and exercise these resource rules through the new seam.
- `internal/simulation/README.md`: current coupled scenario excludes production Manager/native lifecycle and broader timer/effect scheduling. It is not this proposed full DST boundary.
- `go.mod`: toolchain Go 1.27.1, Temporal Server 1.31.2, SlateDB Go 0.16.0 and gRPC 1.83.2 are current declared pins. Verify fetched/native/build artifacts when running experiments.

Independent review must resolve control-record size/CAS contention and ownership update authorization, exact late-open recovery proof, native cancellation/handle retention feasibility, cross-machine filesystem qualification, generated API migration, and ID migration compatibility. Library adoption (placement, lifecycle, election), database grouping/count/growth, scheduling defaults and embedded Temporal resource footprint remain bounded experiments. Do not turn those open measurements into invented production claims or expand the first slice into a generic framework.

## Review and adjudication — 2026-09-06

Dedicated Astra spec author: `/root/spec_author`. Independent Astra reviewer: `/root/spec_review`. Coordinator adjudication: accept these service/DST contracts as the implementation target, subject to the explicit experimental gates above. No remaining blocking specification finding in the final review.

Review corrections incorporated: bind coordinator authority and plan into conditional publication; assignment-revision ABA protection; one-shot native opening and displaced-writer recovery; aggregate SDK conditional-write ambiguity; explicit logical ticks/incarnations and transaction seam; stable filesystem lock plus read-side durability recovery after crash between rename and directory sync; first-failure preservation/cleanup and continuous search replay semantics. No native runtime, scale, remote filesystem qualification or full acceptance pass is implied. The separate implementation plan must retain those executed proof requirements.
