# Xenon replacement dossier

Status: **PROPOSED — approval required before implementation**  
Reference revision: `2c9e4d209ae462ca9913c7ceea34d606c909173f`  
Interactive walkthrough: [rewrite-dossier.html](rewrite-dossier.html)

This review pause follows Calum's latest explicit request to inspect and approve
the rewrite plan before implementation. It temporarily narrows the standing
autonomous delivery delegation for this decision only.

## Decision in one sentence

Rebuild Xenon's process composition, package structure, configuration and
developer experience in a clean worktree, while mechanically preserving or
porting its proven coordination, storage, replay, wire and Temporal correctness
kernels. Treat the current implementation as an executable reference until every
replacement gate passes. It is supporting evidence, never the sole oracle.

This is a ground-up codebase design, but not a clean-room rewrite of distributed
protocols or durable formats. The distinction is deliberate: the present tree is
hard to navigate, while several of its least visible details prevent duplicate
writes, stale-owner acknowledgement and unreadable persisted state.

## What exists today

The supported process path is:

```text
Temporal SDK / UI
       |
       v
cmd/xenon -> app.Run
               |-- embedded Temporal Server
               |       `-- temporal/adapter
               |              `-- Xenon persistence RPC
               |
               `-- ServiceRuntime
                       |-- S3 registry
                       |-- cluster coordinator + membership
                       |-- partition controllers + SlateDB
                       `-- routed gRPC server
                               |-- local authoritative execution
                               `-- one-hop forwarding to owner
                                       `-- SlateDB -> S3
```

Concrete entrypoints are `cmd/xenon/command.go`, `internal/app/app.go`,
`internal/app/service_runtime.go`, `internal/temporal/service.go`, and
`internal/routing/binding.go`.

### Repository facts at the reference revision

| Measure | Value |
| --- | ---: |
| Tracked files | 1,111 |
| Go files | 414 |
| Production Go lines | 46,957 |
| Go test lines | 29,643 |
| Python files | 77 |
| JSON files | 205 |
| Files under `proof/`, `scripts/`, and `test/` | 360 |
| Command directories | 13 |
| Local packages reachable from `cmd/xenon` | 27 |
| Default `cmd/xenon` binary | 184,167,954 bytes |

The size is not proof of bad code. It is evidence that product runtime, developer
tools, release laboratory, historical proof systems and a legacy runtime are not
well separated.

### What is already valuable

- One Go binary embeds upstream Temporal Server and exposes the standard Temporal
  SDK and UI surfaces.
- SlateDB writes directly to S3-compatible storage; local state is disposable.
- Registry records use exact-version conditional writes with explicit ambiguous
  outcomes and ABA-resistant transitions.
- Coordinator, placement and partition lifecycle decisions have pure `Step`
  functions.
- Complete persistence operations have durable IDs, input digests and saved
  outcomes for safe replay.
- Routing rejects stale owners, preserves deadlines and prevents recursive
  forwarding chains.
- All Temporal persistence families are implemented against the pinned Temporal
  version.
- The new Go DST runs thousands of virtual schedules quickly and saves exact,
  minimizable failures.
- Three bounded real journeys cover native SlateDB/MinIO, multi-node ownership,
  and Temporal/Nexus/visibility cold recovery.

### Why the current tree is difficult

1. **Two runtime generations coexist.** The active format-2 runtime uses
   `cluster`, `partitions`, `persistence`, and `routing`; the legacy path uses
   `directory`, `ownership`, and `node`.
2. **The product binary imports its laboratory.** `cmd/xenon` directly imports
   simulation and integration packages, which in turn pull proof and process
   tooling into the executable.
3. **Every node constructs controllers for every configured partition.** Work can
   grow with nodes multiplied by physical partitions.
4. **Authority reads occur in the persistence request path.** Route resolution
   and local admission both read cluster control from shared storage.
5. **The real driver bypasses the best DST seam.** Pure state machines exist, but
   runtime heartbeat, polling, readiness and shutdown use direct wall clocks and
   tickers.
6. **Durable contracts are scattered.** Stable keys, envelopes, layout hashes,
   page tokens, fences and outcome records live beside their implementations.
7. **Temporal adapters repeat connection and operation-envelope mechanics.** A
   family codec should not own transport policy.
8. **Operator configuration exposes implementation topology.** Ports, timing
   loops, partition layout and internal limits all appear together.
9. **Observability is too shallow.** Routing counters exist, but lifecycle,
   authority, storage latency, unknown outcomes and telemetry drops are not
   coherently visible.
10. **Historical evidence dominates navigation.** Valuable receipts exist, but
    their directory layout looks like active application architecture.

## Contracts the replacement must preserve

### Product contract

- One identical Go `xenon` process type, runnable with or without Kubernetes.
- Each ready process accepts complete Temporal persistence operations.
- It executes locally when authoritative and forwards directly otherwise.
- Standard Temporal SDKs and UI remain unchanged.
- S3 is the sole durable application dependency; process disks are disposable.
- SlateDB remains the initial engine and writes directly to S3.
- Target design ceiling is approximately 100 Xenon nodes in one region.
- Xenon-owned identities retain prefixed 128-bit base62 form such as `clu_`,
  `nod_`, `inc_`, `prt_`, `op_`, and `trn_`.

### Registry contract

Versions are opaque and scoped to one key. `Create` requires absence and
`Replace` requires the exact observed version. A successful mutation means a
whole durable record. Cancellation after dispatch is an unknown outcome. A later
conflict cannot prove that an earlier attempt did not commit. Every accepted
change carries a fresh transition identity and digest-bound envelope.

### Layout and coordination contract

- Physical partition identity, path and slot order remain stable after data is
  populated.
- Layout digest binds format, planner, order, logical names, IDs and paths.
- Coordinator generation, cluster assignment revision, per-partition assignment
  revision, writer generation and reservation are distinct.
- Assignment is intent. Readiness is published only after reservation, native
  open/recovery, current-assignment validation and conditional publication.
- A restarted node has a new incarnation.
- Stale coordinators and stale owners cannot publish authority or acknowledge new
  work.
- Delayed stale native opens may fence a newer writer; liveness depends on finite
  one-shot opens draining and the desired owner recovering.

### Engine and acknowledgement contract

- `Commit` submits work; `AwaitDurable` establishes acknowledgement.
- Cancellation does not imply native rollback.
- Unknown completion quarantines or retains admission until resolution.
- Foreign commit receipts are rejected.
- A fenced or retired writer cannot silently reopen itself.
- Whole-operation admission spans child transactions and durability.
- Durable reads use the required remote barrier instead of pending memory.

### Operation replay contract

Every complete operation carries a stable operation ID and canonical input
digest. The recorded logical response or typed error is committed with mutable
state. Reuse with another digest fails. Replay after owner movement returns the
recorded result. Capacity fails closed until safe reclamation exists.

Stable persisted material includes current protobuf field numbers, `v1/...` data
keys, outcome/count/usage/barrier records, control and manifest encodings, SlateDB
fences, derived history identities and pagination tokens.

### Routing contract

The destination validates the complete authority tuple: cluster, layout digest,
partition, node, incarnation, assignment revision, reservation and generation.
The origin may refresh and retry within the caller deadline. A forwarded request
cannot be forwarded again. The same request object, operation ID, digest and
deadline survive retry. An unknown execution result dominates a later routing
error.

### Temporal contract

The adapter implements the pinned Temporal execution and visibility interfaces,
including typed condition failures, mutable output fields, oneofs, opaque blobs,
task categories, query behavior and pagination. History prewrites are not
incorrectly collapsed into one stronger atomic transaction. The internal adapter
is a version-pinned integration seam, not a promised generic Temporal plugin API.

## Proposed architecture

```text
cmd/xenon/                  thin CLI composition
internal/xenon/             config, process lifecycle and readiness
internal/control/           membership, election, placement and ownership reactor
internal/storage/           complete operation execution and durable formats
internal/storage/slatedb/   native direct-S3 adapter
internal/transport/         authoritative local dispatch and one-hop forwarding
internal/temporal/          embedded Temporal and family codecs
internal/telemetry/         typed events, metrics, logs and traces
internal/sim/               virtual runtime, checkers, replay and minimization
internal/integration/       three real boundary journeys
internal/compat/            old-format readers and golden vectors
api/xenon/v1/               stable protobuf source and generated Go
testdata/                   minimized failures and compatibility fixtures
deploy/
docs/
website/
```

The replacement has six primary deep modules. Telemetry and compatibility are
supporting modules, not alternate authorities.

### 1. Xenon process

The process module owns construction order, readiness and reverse-order shutdown.
It exposes no generic lifecycle framework.

```go
package xenon

type Config struct {
    Cluster     identity.ClusterID
    Node        identity.NodeID
    Listen      string
    Advertise   string
    ObjectStore ObjectStoreConfig
    Layout      LayoutConfig
}

func LoadConfig(path string) (Config, error)
func Run(ctx context.Context, cfg Config) error
```

Use contexts and a small error group. Introduce a lifecycle library only if the
finished design demonstrates behavior the standard library cannot express
clearly.

### 2. Control

Control owns membership observations, coordinator tenure, placement intent and
partition activation as one deterministic reactor. The state machine is pure;
its private driver performs effects.

```go
package control

type Machine struct { /* immutable decision state */ }

func New(Config) (Machine, error)
func (m Machine) Step(Event) (Machine, []Effect, error)
func (m Machine) View() View

type CASStore interface {
    Read(context.Context, Key) (Record, error)
    Create(context.Context, Key, Mutation) (Record, error)
    Replace(context.Context, Key, Version, Mutation) (Record, error)
}

type PartitionHost interface {
    Open(context.Context, OpenSpec) (Partition, error)
}
```

Only partitions desired, opening, active, draining or recently lost on this node
have local actors. One cluster observer maintains an immutable, versioned
`ControlSnapshot` for routing. A request borrows a route and complete `OwnerToken`
from one snapshot. Before admission, the destination performs exactly one
authoritative conditional-store read and compares it with both the request token
and its locally activated fenced token. No acknowledgement can rely on cached
authority. A typed stale-owner rejection forces one snapshot refresh and one new
resolution. Coordinator/assignment/owner generations make A→B→A different from
the original A and prevent cache ABA. Poll or notification loss may delay routing
convergence but cannot make stale authority valid; DST covers lost updates,
delayed snapshots and A→B→A. Removing or amortizing this final read requires a
separately proven revoke/drain handshake or lease protocol and is outside the
initial replacement.

### 3. Storage

Storage hides transaction choreography, durability and replay behind explicit,
typed persistence-family ports. Invalid method/payload combinations are not
representable. A private dispatcher may use protobuf messages at the wire edge,
but generic byte payloads never become the storage domain API.

```go
package storage

type Command[T any] struct {
    ID        identity.OperationID
    Partition identity.PartitionID
    Digest    [32]byte
    Input     T
    Expected  control.OwnerToken
}

type ExecutionStore interface {
    CreateWorkflowExecution(context.Context,
        Command[CreateWorkflowExecutionRequest])
        (CreateWorkflowExecutionResponse, error)
    GetWorkflowExecution(context.Context,
        Command[GetWorkflowExecutionRequest])
        (GetWorkflowExecutionResponse, error)
    // Every pinned Temporal method is named in the compatibility manifest.
}

type Store interface {
    ShardStore
    MetadataStore
    ClusterStore
    QueueV2Store
    MatchingStore
    NexusStore
    HistoryStore
    QueueStore
    ExecutionStore
    HistoryTasksStore
    ExecutionTasksStore
    VisibilityStore
}

type Partition interface {
    Store
    Close(context.Context) error
}
```

The public module interface does not expose `Begin`, `Get`, `Put`, `Commit`, or
`AwaitDurable`. SlateDB's private adapter retains those concepts because the
implementation and its contract tests need them.

### 4. Transport

Transport resolves once, executes locally or forwards once, and owns one pooled
gRPC client.

```go
package transport

type Resolver interface {
    Owner(context.Context, identity.PartitionID, Refresh) (Route, error)
}

type Client interface {
    Invoke(context.Context, Route, *Envelope) (*Envelope, error)
    Close() error
}

type Handler interface {
    Invoke(context.Context, *Envelope) (*Envelope, error)
}

func NewRouter(Resolver, LocalRegistry, Client) Handler
```

`Envelope` is a closed protobuf oneof generated from the pinned family methods;
it is validated and decoded into the typed storage port at the local boundary.
There is no generic retry middleware. Registry CAS ambiguity, native unknown
completion, durable replay and stale-route retry have different safety rules.

### 5. Temporal

Temporal owns upstream server embedding and translates persistence methods into
the common storage operation. Family codecs remain explicit because their typed
semantics differ; transport mechanics are shared.

```go
package temporal

type Server interface {
    Run(context.Context) error
    Ready(context.Context) error
    Close(context.Context) error
}

func New(Config, storage.Store) (Server, error)
```

### 6. Deterministic simulation

Simulation runs the production control reactor, router and admission logic. It
models registry, engine and transport effects; it never reimplements placement or
ownership algorithms.

```go
package sim

type Scenario func(*T)

func Run(seed uint64, cases int, scenario Scenario) Result

func (t *T) Advance(time.Duration)
func (t *T) Crash(identity.NodeID)
func (t *T) LoseResponse(EffectSelector)
func (t *T) Heal()
func (t *T) Check(...Invariant)
```

Scenarios are Go functions. JSON remains a generated replay/evidence format, not
an authoring language.

## Testing strategy

```text
                 release qualification
             AWS S3, Omes, soak, capacity
                       /\
              three real journeys
          native | multi-node | Temporal
                     /    \
          backend/native contracts
                   /        \
        thousands of virtual DST schedules
                         /\
             ordinary colocated Go tests
```

### Ordinary tests

Run on every edit. Cover pure transitions, codecs, typed errors, identities and
operation semantics. No Docker, network, native engine, Python or wall-clock
sleep.

### DST

Run production decisions with virtual time and controlled effect completion.
Required families include coordinator/member/owner ABA, simultaneous takeover,
lost conditional-write response, stale coordinator publication, delayed stale
open, crashes around reserve/open/ready/commit, lost acknowledgement followed by
new-owner replay, duplicate/digest mismatch, joins during movement, cold cluster
restart, forwarding limits and deadline exhaustion.

Each invariant has an independent checker and a negative mutant. Differential
agreement with the old implementation is supporting evidence, not the oracle.

### Contract tests

- CAS semantics against in-memory, MinIO/S3 and filesystem adapters.
- SlateDB submission/durability, foreign receipts, unknown completion, fencing,
  cold reopen, GC/compaction and resource drain.
- Golden wire and durable bytes produced by the current implementation.
- Compiler inventory for every pinned Temporal persistence interface method.

### Exactly three integration journeys

1. SlateDB + MinIO: lost response, durable reopen and displaced writer fencing.
2. Three nodes: writes during movement, owner kill, same-address fresh
   incarnation, stale rejection and preserved progress.
3. Temporal: two instances, activity, timer, signal/update, child,
   Continue-As-New, Nexus, history, visibility, owner loss and cold restart.

### Release qualification

Real AWS, longer Omes/fuzz workloads, capacity and performance live outside the
fast integration tier. Their absence is reported honestly and cannot be hidden by
MinIO success.

## Reuse, rewrite, archive

| Decision | Current material | Rule |
| --- | --- | --- |
| Reuse closely | `identity`, registry contract/envelope, protobuf field numbers | Preserve semantics and golden bytes |
| Port pure kernels | cluster/partition `Step` logic, layout/control codecs, router authority tuple | Move with existing tests; simplify their host |
| Port behavior | persistence families, Temporal conversions, visibility and typed errors | New structure must pass old and upstream suites |
| Reuse native adapter | SlateDB lifecycle/fence/durability implementation | Extract behind a private interface after review |
| Reuse testing core | Go DST scheduler, artifacts, fingerprint replay/minimizer | Remove real-process and authored-manifest concerns |
| Reimplement | process lifecycle, config, active partition management, shared adapter transport, telemetry | Design against the new module interfaces |
| Compatibility only | legacy formats, old config and populated-prefix reader | Offline migration/read support; never a second runtime |
| Archive after coverage map | `proof/`, `experiments/`, historical receipts | Move to an immutable indexed archive; retain manifests, artifacts, commands and repaired links |
| Delete after replacement | Python harnesses, Makefile, obsolete helper binaries and duplicate runtime | Only after equivalent gates pass |

## Migration sequence

### Phase 0 — Freeze the reference

Tag the accepted current revision. Generate a contract corpus containing protobuf
descriptor hashes, canonical config/control/layout bytes, every durable key and
page token, typed Temporal errors and outputs, and populated MinIO state. Record
the existing method inventory and test commands.

**Exit:** the corpus detects field, key, format, ordering, error and method
omissions.

### Phase 1 — Clean vertical slice

Create the isolated replacement worktree. Implement the small CLI/process shell,
one shared transport and one node owning the existing full explicit logical
layout over real SlateDB/MinIO paths. Embed Temporal and complete a compact SDK
workflow plus visibility and cold reopen. The slice does not consolidate domains,
change slot order or create a new durable format.

**Exit:** the slice is materially smaller, uses no legacy runtime, and passes
golden formats plus a real workflow.

### Phase 2 — Deterministic control

Port the pure coordination kernels into the new control module. Drive the same
node reactor with real and virtual clocks. Implement membership, election,
placement, reserve/open/ready/fence and ownership-local actors.

**Exit:** the full named coordination DST catalog and negative mutants pass.

### Phase 3 — Horizontal operation

Add pooled one-hop routing, second and third identical nodes, automatic movement,
stale owner rejection and acknowledged-write replay.

**Exit:** the multi-node journey passes, and modeled 100-node steady work is
bounded per node rather than nodes multiplied by all partitions.

### Phase 4 — Temporal compatibility

Port every persistence family and explicit codec. Run the compiler method
inventory, old semantic fixtures, pinned upstream suites and differential traces.

**Exit:** no omitted method, typed-output regression, wire drift or supported
behavior difference remains.

### Phase 5 — Operations and release proof

Add typed telemetry, minimal operator config, container/deploy artifacts and the
three clean-checkout journeys. Run release-only Omes, real AWS and capacity work
under their separate qualifications.

**Exit:** replacement-ready gates are green and limitations are published.
Release-ready remains a separate status until real AWS and the selected release
qualifications pass.

### Phase 6 — Cutover

Create old state, quiesce it, and migrate it offline into a destination prefix
owned by a new workload identity. Revoke the old workload identity's access to
the destination, start the replacement there with empty local disks, verify all
records/workflows/tokens/replay, and inject a cutover crash and resume.
Replacement-ready requires MinIO policy tests plus declarative IAM-policy
evaluation proving that the old identity is denied every destination mutation.
Live AWS denial is a separate release-ready qualification. Reusing the same
prefix and credentials is not an acceptable exclusion proof. Mixed old/new
coordination is unsupported.

Only then replace `main`. Archive the old tree in git history and remove replaced
source in one reviewed batch.

## Machine-checkable replacement gates

The replacement must add `internal/compat/manifest.go` as the checked-in
exhaustive inventory of pinned Temporal methods, protobuf oneofs, durable codecs,
keys and page tokens. Its test must compare generated descriptors and interface
method sets with the manifest, so a new or omitted method fails before fixtures
run. Every gate must emit one JSON receipt using `test/evidence/v1` with source
revision, manifest/config/input hashes, environment versions, command, numeric
observations and artifact paths.

- [ ] Reference tag exists; its tree is clean and pushed.
- [ ] Protobuf descriptor and generated-code drift checks pass.
- [ ] Golden registry, control, layout, manifest, durable-key and page-token
  vectors pass old-write/new-read and new-write/new-read tests.
- [ ] Compiler inventory covers every pinned Temporal execution and visibility
  method; an omitted-method mutant fails.
- [ ] Registry ambiguity, SlateDB acknowledgement, fencing and cold-reopen
  contracts pass against real boundary adapters.
- [ ] Named DST failures run through production decisions, at least 1,000
  schedules and 100 seeds within the #119 budget.
- [ ] Independent negative controls fail with the intended stable fingerprint.
- [ ] Replay and minimization reproduce exact saved failures.
- [ ] `go test ./internal/control/... -run TestRequestPathControlReads` proves
  exactly one authoritative control-store read before admission, including
  cache-hit routing, A→B→A and lost snapshot updates; no second route-resolution
  read occurs unless a typed stale-owner rejection forces refresh.
- [ ] `go test ./internal/sim/... -run TestScale100Nodes512Partitions` proves at
  100 nodes/512 partitions that a steady node has one cluster observer, no actor
  for an unowned stable partition, at most `owned + moving` partition actors, and
  no more than 32 pooled peer connections.
- [ ] All Temporal semantic, typed error and pagination fixtures pass.
- [ ] The three real integration journeys pass from a clean checkout.
- [ ] `just xenon test cutover` proves populated-prefix offline migration,
  crash/resume, empty local disks, destination-prefix policy and access-denied
  mutation attempts from the old workload identity.
- [ ] `go test ./...` and `just xenon test dst` require no Python or Make.
- [ ] `just xenon` runs current source and reuses verified caches.
- [ ] `go list -deps ./cmd/xenon` contains none of `internal/sim`,
  `internal/integration`, `proof` or migration command packages.
- [ ] One user entry point remains: `xenon test ...` execs the separately cached
  sibling `xenon-lab` binary without linking it; `just xenon ...` builds and
  dispatches the same pair. Production images contain only `xenon`.
- [ ] `just xenon test journeys` records zero child processes and containers
  after each forced failure and returns native-handle counts to the pre-run
  baseline (allowance zero).
- [ ] Replacement-ready and release-ready are separate receipt statuses. Real
  AWS must pass for release-ready; MinIO is never substituted as that proof.
- [ ] Independent standards and spec reviews have no unresolved high or medium
  findings.

## Stop and reassess conditions

Stop the rewrite if the first vertical slice introduces a new durable format,
weakens typed errors, duplicates production predicates in its tests, needs a
second coordination authority, or is not materially easier to understand. If it
must import most old runtime packages through shims, prefer a narrower in-place
composition refactor.

Do not abstract a generic KV database, distributed coordinator, lease library or
retry middleware. Those surfaces erase safety distinctions. Introduce a seam only
where two real adapters exist or deterministic testing requires controlled
effects.

## Decisions requiring approval

1. Approve a clean composition rebuild that preserves proven kernels and durable
   contracts, rather than a clean-room protocol rewrite.
2. Approve the proposed six-module architecture and interfaces.
3. Approve same-repository isolated-worktree development, with current main as
   the executable reference until cutover.
4. Approve the migration sequence and replacement gates.
5. Approve archiving and later deleting replaced proof/runtime machinery only
   after the coverage and compatibility gates pass.

No replacement implementation begins until these decisions are approved.
