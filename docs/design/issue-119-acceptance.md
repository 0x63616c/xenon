# Issue #119: machine-checkable definition of done

Status: acceptance specification, not evidence of implementation or passing gates.
Owner: [#119](https://github.com/0x63616c/xenon/issues/119).
Baseline inspected: main `e4ce428` (2026-09-06).

This refines [delivery plan section 8](delivery-plan.md) and the
[shared service contracts](dst-service-contracts.md). Keep the existing
service-oriented layout and use production seams. The CLI is a thin caller of
`internal/app` and `internal/simulation`; it must not own a second coordinator,
storage implementation or simulation-only copy of production decisions.

## Meaning of done

A clean checkout builds one Xenon CLI that starts and inspects a local cluster,
generates fresh bounded concurrent workflow workloads, searches for correctness
failures with declared faults, preserves the first failure, replays saved inputs,
and minimizes the same failure. Deterministic simulation and real-stack execution
share versioned scenario/evidence formats but make different replay guarantees.
All criteria below require executed evidence on the integrated candidate revision.
An absent test, empty test selection, skipped environment, timeout, dirty build,
unverified cleanup or deliberately reduced workload is not a passing gate.

Performance benchmarking is #116/#92. Broad OpenMetrics/logging/tracing design is
#92. Full ten-minute release soak, full UI acceptance and release-wide clean
checkout qualification remain #94/#107. This issue still requires its own short
real CLI journey and clean-checkout proof; those cannot be deferred to #94.

## Execution and command contract

Retain `start`, `check-config`, `version`, `generate workflow`, `test simulation`,
`test smoke`, `test local-release-ten-minute`, `search` and `replay`. Add
`dev up`, `dev down`, `inspect`, `test workflow` and `minimize`. These are required
user journeys, not all currently implemented commands. Existing saved-corpus
flags remain usable; generated search adds explicit `--mode simulation|real`,
`--continuous` or `--max-cases N`, `--duration`, `--workload-seed`, `--fault-seed`,
`--workflow-concurrency`, and `--evidence`. Real mode accepts an explicit fixture
binding/configuration; it must not discover or adopt arbitrary running clusters.
Replay and minimize accept `--artifact`, an explicit target mode/fixture when
needed, a new evidence directory and bounded execution/cleanup budgets.

One scenario is in flight at a time (`MaxInFlight=1`). A scenario can contain
multiple fresh workflow inputs and bounded concurrent root executions. Keep
scenario concurrency, root-workflow concurrency and child/activity fan-out as
separate counters and limits. This implements concurrent workflow exploration
without making separate fault scenarios interfere with each other.

Work-command machine output is schema-versioned JSON; diagnostics use stderr.
Existing human output remains compatible; add `--output json` where necessary.
Search/replay/minimize exit codes: 0 = declared finite target completed and all
assertions/cleanup passed; 1 = failure/invalid input; 2 = exploration or reduction
budget exhausted; 130 = user cancellation. Pending cleanup is always nonzero and
explicit. First correctness failure takes precedence over later cancellation.
A continuous search stopped by time budget reports `budget`, never `passed`.
A finite replay expected to reproduce a bug returns nonzero with its fingerprint;
its acceptance test passes by checking that expected nonzero result.

## Acceptance criteria

### CLI-01: side-effect-free command boundaries

Run every help path, version, completion and valid/invalid check-config through
an instrumented command harness. Assert zero backend mutations, native engine
opens, child-process launches and network calls. Verify unknown flags, missing
configuration, unsupported schema and invalid limits fail before provisioning.
JSON output parses as exactly one documented terminal object (or explicitly
versioned JSONL for foreground server output); errors never contaminate stdout.
Conflicting supported configuration sources follow documented precedence with
an executed test for every pair; unsupported overrides are rejected, not ignored.

### CLI-02: actual local lifecycle and inspection

From a clean checkout, invoke CLI `dev up` with a pinned three-node local MinIO
fixture and compatible worker. Await observable readiness, execute an SDK
workflow and a real Nexus operation, then use CLI `inspect` to report the same
cluster ID, node incarnations, partition IDs, desired/ready owners and generations
as independently read authoritative state. Inspect causes zero writes, engine
opens or resource provisioning. Unavailable authority yields an explicit
unknown/unavailable result, not fabricated readiness. `dev down` removes only
owned processes/containers/network resources; a separately created sentinel
resource survives. Repeating down is safe. Local object data is preserved unless
explicit ephemeral-fixture teardown was selected; evidence is always preserved.

### GEN-01: fresh, bounded and deterministic generation

For workload seeds 1, 42 and 2026090701, generate 100 cases per seed twice. Verify
canonical expanded input hashes match by seed/index/version across repetitions,
and each sequence has at least two distinct workload hashes. Record unique-count
statistics; do not claim all generated workflows must be unique. Changing only
fault seed leaves workflow bytes identical. Replay must work with generator
binaries removed. Each input satisfies configured action/depth/payload/fan-out
limits and capability schema before dispatch. A forced generator error produces
phase `generation`, zero submissions and no completed-case increment for that
case; earlier completed-case counts remain intact. No silent skip or
resampling to a passing seed. Save expanded protobuf/action and fault bytes before
any case effect. Omes support means the pinned supported grammar, not arbitrary
user workflow synthesis.

### GEN-02: concurrent real workflows, not serial corpus throughput

Use three generated cases with four independent root workflows each and
`--workflow-concurrency 4`. A barrier fixture makes all four roots observable as
Running before release. Assert peak admitted unfinished roots is exactly four,
never five, and at least two roots overlap in actual Temporal histories. With
concurrency one, peak is one. Child, activity and Nexus fan-out obey their own
explicit limits. Count root starts, child starts, Continue-As-New runs and Nexus
handler runs separately. A case is complete only after independent assertions and
settle, not after submission or Omes process exit. No fixed 20-case replay loop
can satisfy fresh-generation acceptance.

### DST-01: production code and virtual time

The simulation driver calls the production cluster/partition/persistence decision
APIs through injected clock, registry, transport, engine and lifecycle seams.
A committed call-path test/manifest identifies those production entry points;
a second implementation of the algorithms fails review. Run 1,000 bounded
schedules across at least 100 seeds, twice. Canonical decision/event traces,
assertion results and failure fingerprints are identical for each replay, omitting
only declared provenance fields such as output path. Advance a 24-hour logical
timer through virtual time; instrumented adapters assert zero wall-clock sleeps,
network calls or native engine opens in the deterministic execution path.
Hold workflow bytes fixed while varying fault seeds: require at least two distinct
expanded event-order hashes and executed coverage of every named fault cut below.
The coverage receipt reports counts per cut; repeating one schedule 1,000 times
does not satisfy this gate. Measure per-schedule execution cost and commit the
logical-event and wall-watchdog budgets with the fixture; report measurement
revision and derivation instead of guessing a performance limit.
A separate wall-clock watchdog stops harness bugs and cannot make the case pass.
No claim is made that this simulates Temporal's SDK/OS/native scheduling.

### FAULT-01: explicit fault capability matrix

Required simulation schedules cover: owner crash before commit, after durable
commit before response, stale owner resume after replacement, coordinator loss
mid-move, overlapping joins, lost registry CAS response, storage delay/error,
dropped/delayed/duplicated messages and stale routes including same-address new
incarnation. Include cuts before/after reservation publication, native Open and
Ready publication; renewal versus takeover; renewal response lost then changed
authority; assignment-revision ABA; and displaced writer fencing during Open.
Each has explicit trigger, target, occurrence count and bounded
healthy settle phase. Required real scenarios cover join, owner kill, takeover,
restart at the same address and complete process cold restart with disposable
local state. Storage/network faults additionally run through the corresponding
real adapter/native contract where supported. Unsupported fault/mode combinations
fail validation before execution and are listed as unsupported, never silently
ignored or presented as real-stack coverage.

Triggers use observed state: ready, operation admitted, owner serving a partition,
commit/durable barrier, or takeover plus acknowledged progress. Tests assert the
named trigger was reached and the fault actually occurred exactly as declared.
No multi-minute delay is required to reproduce a failure state; virtual time or
explicit bounded fault-adapter controls establish it. Real timers remain real.

### ORACLE-01: independent, nonvacuous correctness assertions

Maintain an independent ledger of submitted logical operations and acknowledged
results plus immutable operation identities/digests. Verify acknowledged state
survives takeover/cold recovery; retries neither duplicate application nor accept
a changed digest; stale authority cannot acknowledge; atomic records recover
wholly. Scenario predicates define expected workflow results, terminal statuses,
child/Continue-As-New/Nexus relationships and expected histories/visibility.
Capture complete paginated histories and compare inventory with the expected
execution graph, including late child starts and Nexus handlers. Matching list and
count alone is insufficient because both can omit the same execution.

Healthy settle stops fault injection, restores declared services and proves new
acknowledged progress within its budget, with zero unresolved expected operations.
Run deliberate mutants for lost acknowledged write, duplicate application, wrong
digest, stale acknowledgment, partial atomic recovery, omitted child, corrupt
result, omitted durable replay result, retry deadline reset, post-fence commit
and stalled progress. Each must fail its named invariant; an unrelated
panic/timeout does not satisfy the negative control. The checker must not use the
implementation under test to derive expected state.

### STOP-01: first failure, admission cutoff and ordering

Use controlled scheduling to race workload failure, asynchronous node death,
trace write failure, budget expiry and Ctrl-C in both orders. Assert the earliest
observed cause is immutable; later cleanup/cancellation errors are secondary.
After latch, no new case/root/fault admission occurs. Already admitted effects
may finish and must be accounted for. A blocked generator or request must not
prevent background failure from stopping the run. Failure evidence is flushed
before destructive cleanup. Exact saved case bytes survive forced cleanup errors.
A trace/evidence limit reached is an explicit failure or budget outcome, not a
successful truncation. Callback panics become recorded failures.

### CLEAN-01: bounded ownership and safe fixture reuse

Every subprocess/native operation has one owner through cancellation and drain.
One absolute cleanup deadline covers all stages; retries cannot renew it.
After successful per-case cleanup, independently enumerate case-owned
PIDs/process groups and active executions: zero remain, while shared fixture
resources return to their declared baseline. After final run-owned fixture
teardown/dev down, independently enumerate its PIDs/process groups, containers
and active executions: zero owned resources remain. Externally supervised fixture
resources are retained and explicitly recorded as externally owned. A failed/uninterruptible child
produces `cleanup_verified=false` plus pending ownership and prohibits another
case or reuse of that driver/fixture. Never detach pending work and call cleanup
successful. Test a paused child, late workflow creation, unavailable visibility,
cleanup exception, full evidence disk and hung termination. Failure to establish
an exact remote execution census retires/resets the fixture before another case.
Reset is permitted only for an explicitly disposable run-owned fixture.

Fixture reuse requires per-case scope, exact pre/post execution census and no
unresolved work. Bind audit/cleanup success to the current case identity; a prior
case's successful audit cannot certify the next. Across 100 deterministic cases,
assert bounded live handles/processes/queues with zero leaked owned resources.
Across the three real concurrent cases assert the same resource counts return to
the declared baseline. Evidence growth is bounded by configured quota; reaching
it stops cleanly with a non-pass budget result, never deletes old evidence.

### REPLAY-01: exact inputs and honest scheduling claims

With generator unavailable, replay a saved passing case and a saved deliberate
failure in simulation. Assert expanded bytes, selected event order and normalized
trace/fingerprint match; originals remain byte-identical. Corrupt one input byte,
remove an event, alter schema or tool hash and verify rejection before execution.
Real replay uses saved workload and fault triggers and records actual observations;
it does not promise identical scheduling or a failure on every attempt. Replaying
into a nonempty or incompatible fixture is refused before mutation. A declared
compatible version override is explicit and records both original and replay
versions; it cannot be used as exact-revision acceptance evidence.

### MIN-01: reduction preserves the same failure

Use a committed failing case with at least 20 removable actions and 3 faults,
including known irrelevant work. Reduce with deterministic candidate ordering,
explicit attempt/time limits and valid dependency repair (no dangling child,
signal or operation references). Require strictly smaller lexicographic size
(action count, fault count, total encoded bytes), still producing the original
stable invariant fingerprint. Fingerprints use invariant ID and normalized
failure mechanism, not unstable timestamps/addresses. A different failure or
invalid candidate is rejected. Replay the reduced simulation case three times
with identical fingerprint. For a real failure require three of three bounded
attempts with the same fingerprint before accepting a reduction; record all
attempts and reject intermittent candidates without claiming determinism.
Cancellation/budget preserves the best verified candidate and original artifacts,
reports incomplete reduction, and cleans up. No claim of globally minimal output.
A regression control includes a candidate that fails for a different reason.

### EVID-01: complete evidence and truthful accounting

Versioned run/case/result schemas record source commit and dirty status, tool/
native/image/config/input hashes, generator capability/version, workload/fault
seeds, expanded inputs, chosen simulation schedule or real observations, faults
requested/triggered/completed, operation/run counts, first fingerprint and
secondary errors, settle outcome and cleanup ownership/result. Files are hashed
and written before referenced effects; finalized receipts include output hashes.
Validate checksums on read. Reject missing required assertions and mismatched
counts, including submitted-but-unresolved operations. `completed_cases` increments
only after assertions, settle and verified cleanup. Continuous budget means
`no_failure_in_explored_prefix`, not full correctness or release acceptance.

### DELIVER-01: executable gate, not a checklist that certifies itself

Register these five gates with `scripts/prove.py` and committed manifests:

| Gate | Required criteria |
| --- | --- |
| `cli-contracts` | CLI-01, command/exit compatibility controls |
| `workflow-search-dst` | GEN-01, DST-01, simulation FAULT-01, ORACLE-01 mutants, STOP-01, CLEAN-01 controls |
| `workflow-search-real` | CLI-02, GEN-02, real FAULT-01, real ORACLE-01, real CLEAN-01, EVID-01 |
| `workflow-replay-minimize` | REPLAY-01 and MIN-01 in simulation and a real controlled-failure fixture |
| `issue-119-acceptance` | all four gates from a clean checkout of one candidate, plus source/layout and evidence validation |

These names are deliverables; their existence and passage are not asserted here.
Manifests enumerate exact expected tests, fixtures, images/tools, source inputs,
limits and setup/run/teardown. Tests have explicit assertions and expected counts;
zero selected tests and skipped tests fail. Machine-readable requirement mapping
lists each criterion ID and the receipts/assertions proving it. The aggregator
verifies child receipts and negative-control outcomes instead of trusting a
handwritten `passed` flag. Short deterministic/CLI regressions enter CI; bounded
real gates are opt-in or existing appropriate CI jobs. No test may substitute
sleeping for work to meet a duration target.

Independent review checks the spec, implementation and integrated evidence.
Close #119 only after the aggregate gate passes on the integrated revision and
the documented commands execute successfully in that clean checkout. Keep #94,
#92, #116 and release status separate. Current serial corpus, preparation-only
generation and unreviewed resident driver are partial work, not this end state.

## Partial passive inspection contract

`xenon inspect --config FILE [--output json] [--timeout 5s]` reads the format2
manifest and the authoritative control object using S3 GETs. It does not call
bootstrap/preparation, create membership, probe processes or open native storage.
The timeout must be positive and at most one minute; the configured registry
budget can shorten it. An explicit JSON configuration is required. Application
settings come only from that file; unsupported CLI overrides fail validation.
The existing S3 connection environment applies: `AWS_DEFAULT_REGION` takes
precedence over `AWS_REGION`, with `AWS_ENDPOINT` optionally selecting a target
and credentials supplied externally. Configuration validation does not require
credentials or contact that target.

Inspection stdout is one schema-1 object: `schema`, `status`, optional
`authority_version`, `control`, and `error`. `observed` includes the exact decoded
control publication and returns exit 0. `unknown` means missing, incompatible or
corrupt authority; `unavailable` means access/transport failure or cancellation;
`invalid` means invalid configuration/budget. These return exit 1 with diagnostics
on stderr. Argument-parser failures (unknown flags or invalid flag syntax) report
only stderr. The control's desired owners, incarnations, generations and Ready
bits are persisted observations, not current health or a complete live-node
census. Unknown/unavailable results never include a control snapshot.

`xenon check-config --config FILE --output json` emits one schema-1 object with
`status: valid|invalid` and optional `error`; its default text mode preserves
`XENON_CONFIG_VALID`. Neither mode initializes storage. These component tests are
partial CLI-01/CLI-02 evidence; they do not establish the instrumented full
command gate or three-node real lifecycle acceptance above.
