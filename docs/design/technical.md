# Xenon technical specification

Status: implementation direction selected on bounded primitive evidence, 2026-09-05. Direct S3 storage is a user requirement. SlateDB clears the initial engine-choice gate; the ownership protocol and complete Temporal backend are not yet proven. [#5](https://github.com/0x63616c/xenon/issues/5) records the engine/layout decision; [#6](https://github.com/0x63616c/xenon/issues/6) and [#7](https://github.com/0x63616c/xenon/issues/7) retain their separate executed gates.

## 1. Components and version boundary

Retain Temporal Server v1.31.2, commit `19a774302c613da9adc4436ab14278ccdca8e0a5`. A Go adapter implements its execution and visibility persistence interfaces and routes complete operations over versioned protobuf/gRPC to Rust Xenon nodes. Nodes embed SlateDB v0.16.0, commit `3fb9e8abab0c9f5833f0c154140ceef009fea02a`, using S3 directly. Both data and Xenon placement metadata are durable only in object storage. Local caches are disposable.

Prefer one Rust node lifecycle containing operation handlers, partition admission, SlateDB handles, compaction and recovery. This avoids a second Go-handler/Rust-engine RPC transaction lifecycle. Such a split would still require translating Temporal's SQL semantics into KV operations and would add handler death, orphaned engine gates and partial transaction sessions. It is not the initial implementation seam. The Go adapter never orchestrates remote Get/Put transactions. Retain upstream protobuf payloads and semantic tests; do not reimplement Temporal's workflow engine.

This is a delegated advocate/reviewer recommendation, not a claim that Calum selected a language. Revisit the seam only with a concrete compiling experiment showing materially less engineering while preserving whole-operation admission and failure semantics. See [ADR 0001](../adr/0001-direct-s3-engine.md).

## 2. Logical partitions and keys

Stable logical partition IDs identify SlateDB prefixes; owner addresses do not enter data keys. A node owns several partitions. Adding nodes moves existing partitions without changing Temporal HistoryShardID count or splitting a transaction domain. Partition count and mapping are versioned bootstrap configuration; changing the mapping is a separate migration.

| Domain | Initial placement candidate | Atomic boundary |
|---|---|---|
| History/execution | Fixed execution partitions containing whole HistoryShardIDs | Shard guard, current execution, related runs, mutable maps, buffered events, CHASM nodes and generated tasks stay together. History trees and branches remain in the shard domain. |
| Matching and control | One movable control partition initially | All ordinary/fair matching subqueues; namespace-wide task-user-data batches and build mappings; namespace notification catalog; cluster metadata/membership; Nexus table version and endpoints; generic queues and QueueV2 metadata/messages. |
| Visibility | Dedicated fixed logical partitions; exact policy pending #7 | A document, its version/deletion record and every index entry changed with it share a partition. |

The control partition deliberately has a single-writer throughput ceiling. History partitions provide independent scale-out even within one namespace. More granular matching/control placement requires enumeration and atomicity tests before changing placement. A hot indivisible domain remains limited by one partition writer.

Keys begin with encoding version and domain tag, followed by unambiguous length-delimited identifiers or order-preserving numeric fields. Distinguish namespace, workflow, run, archetype, task category and matching version explicitly. Specify signed integer ordering, timestamp precision and inclusive/exclusive range endpoints in codec fixtures before use. Opaque Temporal DataBlob bytes remain opaque. Per-operation key layouts are implementation deliverables, not implied by this general prefix scheme.

Cross-partition reads require explicit routing: GetAllHistoryTreeBranches scans logical history partitions; visibility list/count fans out according to #7. Pagination tokens identify logical partitions, query identity and ordered cursor positions, never transient owner addresses. No successful partial result when a required partition is unavailable. Frozen-dataset completeness and behavior during mutation are separate tests; live pagination is not a snapshot promise.

## 3. Admission, transaction and acknowledgement

Each partition has one admission gate shared by all externally visible reads and writes. Hold it from authority validation through the operation's condition checks, transaction commit and `WriteHandle::await_durable`. Default SlateDB Memory reads must never expose another operation's pending volatile state. The initial serialized policy is intentional; concurrent durable snapshots require a separate proof.

Complete same-domain state mutations, condition checks and durable RPC outcomes commit together in a SlateDB transaction. Prefer SerializableSnapshot and tracked reads/ranges. Preserve Temporal's actual method semantics: history appends may precede a later execution-state transaction, so a logical error does not imply no effects. Audit such methods explicitly and reconcile uncertain substeps; do not promise atomicity stronger than implemented.

Return success only after object-store durability. On failed or unknown durability, quarantine the handle and recover; releasing readers onto uncertain memory is forbidden. Cancellation does not prove rollback. Fenced handles retire and cannot reopen themselves. Lifecycle tests must establish whether abandoning/closing an uncertain handle can flush additional state and ensure that recovered durable outcomes remain authoritative.

Ownership checks and reads need a freshness protocol as well as durable bytes. Directory generation is routing intent, Temporal RangeID is a semantic shard guard, and SlateDB epoch fences engine writers. None replaces another. A read must validate current admission authority during its invocation; a cached route or durable-but-stale engine handle is insufficient. The exact read-authority implementation is an open #6 gate, particularly between a delayed contender's engine open and its failed readiness publication.

## 4. Ownership state machine candidate

Store the per-partition directory separately from SlateDB-owned files using conditional object updates. Proposed fields: format version, partition ID, directory generation, desired node incarnation, transition UUID and state (`opening`, `ready`, `retiring`). Node restarts use new incarnations. Stable prefixes survive movement. Never edit SlateDB manifests directly.

1. A controller conditionally reserves an opening transition for the desired owner. Reconcile an unknown directory write by reading its transition UUID.
2. Cooperative old owners stop admission and settle in-flight work. Failed owners are suspected, not presumed dead.
3. The designated incarnation makes at most one SlateDB open attempt for that reservation. Opening recovers through normal engine fencing.
4. Publish `ready` only with a conditional update proving the same reservation is still current. Superseded openers retire without serving.
5. On fencing, the desired owner retires its handle and obtains a fresh reservation before another attempt. No automatic reopen loop under an old reservation.

A paused superseded opener can still fence a ready owner. The proposed protocol tolerates finite interference instead of claiming that a post-open check prevents it. Once faults stop, controllers must stop competing, finite outstanding one-shot opens must drain, and the desired owner must regain bounded useful progress. Deterministic desired assignment over a versioned membership set is the candidate controller policy. Failure suspicion, backoff and numerical convergence targets require #6/#8 decisions and tests; no lease-safety argument based on unsynchronized clocks is assumed.

Pinned source shows manifest initialization retries conflicts before claiming a writer epoch, then WAL fence retries refresh that same epoch. A wrapper must not convert a fenced open into another open under the same reservation. This observation is not yet an executed liveness proof.

## 5. Wire contract and uncertain outcomes

Use operation-specific protobuf messages with an envelope containing protocol version, operation kind, logical partition, routing generation, operation ID and canonical-input digest. Matching mode and QueueType are part of operation context. Retries of one invocation retain operation identity; unrelated Temporal invocations do not inherit that identity automatically.

Persist the exact logical response, typed error and mutable-request outputs with the mutation. A reused ID with a different digest fails explicitly. A replay after intervening mutations or owner replacement returns its recorded result. Retention must not permit expired duplicate IDs to execute again silently: retain records for the bounded proof and fail explicitly at enforced limits until a safe expiration protocol exists.

Preserve concrete condition errors and fields, serviceerror details, int64 precision, protobuf oneofs/unknown fields, opaque blobs, task categories and optional output pointers. In particular, reconstruct WorkflowConditionFailedError and CurrentWorkflowConditionFailedError rather than returning generic Internal. Preserve UpdateTaskQueueUserData Applied/Conflicting flags even on error. GetOrCreateShard's local callback becomes concrete data only when needed; no callback crosses RPC. The [pinned wire audit](../research/pinned-wire-contract.md) is the inventory and fixture checklist; its SQLite material is exploratory history, not the storage design.

Transport errors remain distinct from durable logical errors. Unknown outcomes trigger reconciliation; a later condition conflict cannot be rewritten as earlier success. Bound frames, transactions, retries and deadlines explicitly, and report rejected size/availability conditions without fabricating persistence success.

## 6. Visibility and compatibility

Execution durably creates visibility tasks; visibility workers apply documents asynchronously. No cross-store distributed transaction is required for that path. Document/index mutation and version checks are atomic within one visibility partition. Preserve close-before-delete ordering and design late-write protection before tombstone reclamation. Reusing Temporal's generic query converter is preferred, but SlateDB supplies no SQL query engine.

Issue #7 must settle the exact behavioral oracle, typed predicates, Text tokenization, missing/null semantics, ordering, grouping, CHASM divisions, search-attribute administration, distributed merge pagination and schema evolution. Existing SQL behavior is evidence, not a blanket compatibility waiver. Required missing surfaces remain tracked blockers. Unchanged Temporal UI and SDKs require executed frontend exercises in addition to persistence tests.

## 7. Evidence and gates required before advancement

`cargo test --locked -p slatedb-probe -- --nocapture` passed four real SlateDB tests: atomic batch/conflict/reopen, serializable read-dependency write skew, durable-wait/read-admission behavior, and competing-writer fencing. See [test output](../evidence/primitive/tests.txt). `./scripts/probe-local.sh` passed a durability/reopen smoke test using the S3 API against the local emulator; see [emulator output](../evidence/primitive/emulator.txt). These support engine selection, not process-crash, ownership-protocol, GC or real-S3 acceptance.

The fuller gates below remain **PLANNED / NOT EXECUTED**, except for the bounded primitive subset just identified. Record exact commands and evidence in the [verification matrix](verification-matrix.md); a source audit is never a pass. Keep fence GC `dry_run` locked to the safe pinned setting. Other normal engine maintenance remains enabled, but a short test with default settings does not demonstrate that GC or compaction actually ran; require observed maintenance events in lifecycle evidence.

| Gate | Required experiment | Blocks |
|---|---|---|
| Engine continuation | Extend passed primitive batches/conflicts/read-gate/reopen to process kill before/after acknowledgement and empty local cache | Full durability acceptance; initial engine selection has bounded evidence |
| Ownership | Old writer, two paused superseded openers released after readiness, fresh desired-owner recovery after each; competing controllers; unknown reservation result; stale reads/routes | #6 acceptance and scale-out claims |
| Wire | Every operation round-trip, typed errors/outputs, real gRPC shard operation, lost response after durability, replay after owner movement | Adapter acceptance |
| Execution | Pinned upstream suites plus Continue-As-New, history partial-step recovery, matching cross-subqueue/user-data rollback | First real workflow acceptance |
| Visibility | Differential query results and complete records, reordered mutations/deletes, pagination across movement, API/UI assertions | #7 coverage and release |
| Storage lifecycle | Repeat ownership/recovery with normal WAL GC, compaction and protected reads; no cleanup-disabled final proof | Robustness acceptance |
| System | Multiple Temporal instances, Omes mixed workloads, live node addition and actual traffic redistribution, loss of all local state | Shipping gates |
| Real S3 | Repeat relevant engine, ownership and system runs on authorized real S3 resources | Final shipping gate; emulator alone insufficient |

## Primary evidence

- [Temporal persistence interfaces at the selected pin](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/persistence_interface.go).
- [SlateDB transaction commit and durability](https://github.com/slatedb/slatedb/blob/3fb9e8abab0c9f5833f0c154140ceef009fea02a/slatedb/src/db_transaction.rs).
- [SlateDB writer fencing](https://github.com/slatedb/slatedb/blob/3fb9e8abab0c9f5833f0c154140ceef009fea02a/slatedb/src/fence.rs), [manifest ownership](https://github.com/slatedb/slatedb/blob/3fb9e8abab0c9f5833f0c154140ceef009fea02a/slatedb/src/manifest/store.rs), [WAL initialization](https://github.com/slatedb/slatedb/blob/3fb9e8abab0c9f5833f0c154140ceef009fea02a/slatedb/src/wal/slatedb/writer_init.rs).
- [Transaction-domain research](../research/partition-boundaries.md), [durability research](../research/durability-ownership.md), [visibility research](../research/visibility.md). The partition/visibility reports use an earlier source snapshot; rerun relevant contract checks at the selected release before implementing each surface.
