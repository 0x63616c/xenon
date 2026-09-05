# Durability and ownership: SlateDB evidence for Xenon

Research for [issue #3](https://github.com/0x63616c/xenon/issues/3), 5 September 2026. **Static source review only: no adapter, cluster, benchmark, or failure test was run.**

## Finding

SlateDB has credible primitives for an S3-backed Xenon storage partition: atomic transactional batches, tracked read/write conflicts, explicit durable acknowledgement, and writer fencing with WAL recovery. Those are building blocks, not a complete distributed database service. Xenon must supply request semantics, safe read publication, placement/routing, ownership admission, and recovery behavior.

**Recommendation for Calum's decision:** retain SlateDB as the leading validation candidate. Make durability/read publication and overlapping-owner tests hard gates before treating scale-out as proved. This note does not select an ownership protocol or raise the earlier subjective feasibility percentages.

The review pins SlateDB v0.16.0 to commit `3fb9e8abab0c9f5833f0c154140ceef009fea02a` and Temporal v1.31.2 to `19a774302c613da9adc4436ab14278ccdca8e0a5`. Pinned release source is the authority; earlier prose is not evidence. The previous assessment contained some literal `/blob/undefined/` links; this report uses resolved revisions.

## 1. Transactions and the actual acknowledgement boundary

[DbTransaction::commit](https://github.com/slatedb/slatedb/blob/3fb9e8abab0c9f5833f0c154140ceef009fea02a/slatedb/src/db_transaction.rs) returns an optional WriteHandle. A nonempty commit applies its batch to in-memory WAL/memtable and returns **without waiting for object-store durability**. The caller must await the handle's `await_durable` before returning successful persistence to Temporal. An empty batch returns no handle; the implementation does check tracked read conflicts, despite an adjacent comment suggesting a no-op without database interaction.

[TransactionManager](https://github.com/slatedb/slatedb/blob/3fb9e8abab0c9f5833f0c154140ceef009fea02a/slatedb/src/transaction_manager.rs) captures the last committed sequence at transaction registration. Snapshot isolation tracks write/write conflicts; SerializableSnapshot additionally tracks read keys and scan ranges for read/write and phantom conflicts. The transaction commit path materializes scan dependencies before batch submission. Tracking can be bypassed through explicit APIs, so Xenon must not use untracking as a performance shortcut for ownership or version guards.

**Proposed adapter contract:** one complete Temporal persistence mutation reaches one Xenon partition; ownership/version checks, state changes, pending tasks, and any RPC deduplication outcome belong to its transaction. Return persistence success only after its durability wait succeeds. This is a design requirement inferred from the APIs, not an implementation present upstream.

A cancelled RPC or failed durability wait must not be interpreted as proof that no mutation occurred. The caller may have lost the response after persistence succeeded, and a commit can already be visible in memory while the caller is still waiting. Failures need an explicit unknown-outcome policy; automatic retries must preserve the original operation identity and Temporal conditions.

## 2. Durable writes are insufficient without safe reads

[ReadOptions and ScanOptions](https://github.com/slatedb/slatedb/blob/3fb9e8abab0c9f5833f0c154140ceef009fea02a/slatedb/src/config.rs) default to DurabilityLevel::Memory. That includes committed data still awaiting object-storage flush. Remote restricts results to durable data; `dirty: false` only excludes uncommitted data and does **not** make Memory reads durable. The [transaction source's durability test](https://github.com/slatedb/slatedb/blob/3fb9e8abab0c9f5833f0c154140ceef009fea02a/slatedb/src/db_transaction.rs) deliberately demonstrates Memory seeing a value while Remote does not.

The concrete failure to avoid is: a pending transaction creates a task; a concurrent queue scan reads it from memory; work is dispatched; the storage owner dies before the task/state reaches S3. Awaiting the original write's response does not stop that other reader.

Two candidate policies need experiments:

- **Conservative per-partition admission gate:** hold a partition operation gate across commit and its durability wait, including all reads that can expose or act on state. This deliberately limits concurrency but is simple to test. On an ambiguous/error durability outcome, stop serving that handle and recover through the ownership protocol rather than release readers onto uncertain memory. Whether recovery requires aborting rather than gracefully closing a handle must be specified.
- **Concurrent transactions with durable read publication:** maintain a proven durable read boundary or use Remote snapshots for externally visible reads while retaining correct transactional conflict semantics. This may preserve batching/concurrency, but has a larger proof obligation.

Do not indiscriminately switch all conditional transactions to Remote. A transaction can start at a committed sequence that includes a not-yet-durable write; a durability-filtered read can omit that write even though conflict tracking regards it as preceding the transaction. The interaction must be tested, rather than assuming SerializableSnapshot automatically fixes a lagging read view. This is an inference from the separate committed sequence and durability filters, not a reported upstream defect.

Separately, **durable is not necessarily current**. A read-only replica or an old owner can have durable but stale state. [DbReader](https://github.com/slatedb/slatedb/blob/3fb9e8abab0c9f5833f0c154140ceef009fea02a/slatedb/src/db_reader.rs) follows manifests and WAL with checkpoint management; that does not establish linearizable routing or read-after-write behavior for Xenon. Initially route correctness-sensitive reads through the owner. Even then, a directory lookup performed earlier is not a proof that the owner remains current when returning a read; stale-owner read handling belongs in the ownership protocol.

## 3. Three different generations

| Generation | Authority and role | What it cannot replace |
|---|---|---|
| Temporal RangeID | Temporal shard ownership persisted with guarded operations | Does not assign SlateDB writer ownership |
| SlateDB writer epoch | Engine-level exclusion of older writers for one database prefix | Does not place partitions, pick network addresses, or establish client routing |
| Xenon directory generation | Proposed placement record version used by adapter and nodes | Does not itself prevent an old engine handle from writing |

[Temporal's SQL shard implementation](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/sql/shard.go) compares the expected RangeID while holding a database transaction lock and returns ShardOwnershipLostError on mismatch. Xenon must preserve that guard inside its transaction even if it has already accepted the correct routing generation.

[WriterFencer::fence](https://github.com/slatedb/slatedb/blob/3fb9e8abab0c9f5833f0c154140ceef009fea02a/slatedb/src/fence.rs) claims a new manifest writer epoch, delegates WAL fencing/recovery, then refreshes the manifest to validate it still owns the latest epoch. [SlateDbWalWriterInit](https://github.com/slatedb/slatedb/blob/3fb9e8abab0c9f5833f0c154140ceef009fea02a/slatedb/src/wal/slatedb/writer_init.rs) installs a WAL fence, accounts for replay boundaries advanced by a prior writer, and retries WAL-ID collisions while refreshing ownership. The new writer receives an iterator that must be replayed as part of recovery.

The distinction matters during a race: the old writer can still durably write during an early phase of takeover before WAL fencing completes. Such writes need to be included in recovery. A directory generation change is therefore not the engine's commit cutover boundary.

The upstream [fencing tests](https://github.com/slatedb/slatedb/blob/3fb9e8abab0c9f5833f0c154140ceef009fea02a/slatedb/src/fence.rs) include:
- `test_fence`;
- `test_fencer_fenced_after_claiming_epoch`;
- `test_fence_handles_fenced_writer_flush`.

They pause fencing, let old writers flush, run WAL GC, introduce another writer, and assert fenced errors or recovered data. They are valuable reusable test patterns, **not tests we ran**, and use an in-memory object store in the inspected harness.

A fenced process must not respond by repeatedly reopening the prefix: each reopen is a new takeover attempt and can fence the desired replacement. Ownership admission must prevent that churn. Single-writer fencing supplies safety primitives; it does not elect a stable owner or guarantee availability.

## 4. What S3-only coordination can and cannot provide

AWS documents [conditional object writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html): If-None-Match can create a missing record, and If-Match can replace a record only when its ETag matches. Conflicts can return 412 or 409; deletion introduces additional cases. These are suitable primitives for experimenting with a per-partition placement record.

A candidate record could contain partition ID, generation, intended owner identity/address, handover state, and a unique transition ID. Treat this as an experiment schema. Keep routing metadata separate from engine-owned files and do not edit SlateDB's manifest directly.

A possible handover sequence to analyze is:
1. Conditionally reserve a transition in the directory.
2. Stop old-owner admission when cooperative and let in-flight operations settle.
3. Open/recover the designated replacement through SlateDB's normal fencing mechanism.
4. Conditionally publish readiness only if the transition remains current.
5. Refresh adapters and retire the previous handle.

**This sequence is not yet a proved protocol.** The directory and SlateDB manifest are different objects. A paused contender can resume opening after its directory reservation is superseded; that open can fence the current writer before the contender notices it lost its reservation. Rechecking afterward prevents stale publication but does not prevent the availability disruption. Lease expiry also requires a specified time/failure model. A heartbeat timeout identifies suspicion, not proof that an old process stopped.

The research supports an S3-only coordination investigation. It does not establish that a CAS directory plus a few checks is sufficient. The decision ticket must specify stale-opener behavior, controller concurrency, node identity/incarnation, authority to reopen, retry limits, read freshness, and recovery after every intermediate state. No extra durable service is selected.

## 5. Ambiguous RPC outcomes and retries

The gRPC layer adds a transport failure boundary around the engine's commit. Xenon needs to distinguish:

| Situation | Required handling |
|---|---|
| Definitely rejected before execution | Retry according to the typed rejection and refreshed routing |
| Engine committed and durable; response lost | Recover the same result, or follow the operation's established conditional retry semantics |
| Commit submitted; durability outcome unknown | Do not claim rollback; reconcile against recovered state |
| Retry reaches new owner | Use durable state, not an old process's in-memory retry cache |

One candidate is a client-generated operation ID plus payload digest and durable result record, written atomically with the mutation in the same partition. The adapter must reuse that ID for transport retries of the same persistence invocation; a new Temporal invocation is not automatically the same operation. A duplicate ID with different payload must be rejected. Retention/expiry and delayed duplicates need a defined policy before garbage-collecting result records.

This is **proposed design**, not something SlateDB supplies automatically. Some Temporal methods already express conditional/idempotent semantics; preserving those may avoid a universal journal. Audit methods individually, including operations with pre-transaction history writes. Do not translate a version conflict into success merely because an earlier attempt might have succeeded. Activity external effects keep Temporal's usual at-least-once/idempotency considerations.

## 6. Compaction and GC are correctness participants

[GarbageCollector](https://github.com/slatedb/slatedb/blob/3fb9e8abab0c9f5833f0c154140ceef009fea02a/slatedb/src/garbage_collector.rs) coordinates reclamation of manifests, WAL, compacted SSTs, and compaction records, using references/checkpoints plus age controls. [CompactedGcTask](https://github.com/slatedb/slatedb/blob/3fb9e8abab0c9f5833f0c154140ceef009fea02a/slatedb/src/garbage_collector/compacted_gc.rs) protects active manifest/checkpoint references and uses compaction state to conservatively bound deletion. [WAL GC](https://github.com/slatedb/slatedb/blob/3fb9e8abab0c9f5833f0c154140ceef009fea02a/slatedb/src/wal/slatedb/gc.rs) treats regular WAL and fence objects separately. Retention is not simply “delete objects older than a threshold.”

[Compactor](https://github.com/slatedb/slatedb/blob/3fb9e8abab0c9f5833f0c154140ceef009fea02a/slatedb/src/compactor.rs) has a separate compactor epoch. Writer movement therefore must account for compactor lifecycle as well as the writer handle; do not invent duplicate background owners or external object cleanup.

[DbReaderMode](https://github.com/slatedb/slatedb/blob/3fb9e8abab0c9f5833f0c154140ceef009fea02a/slatedb/src/db_reader.rs) defaults to managed checkpoints. Its unprotected follower mode explicitly offers no GC protection, so reads from an old manifest may fail after referenced files disappear. This matters for long visibility scans, pagination strategies, and future read replicas. A paginated API does not automatically hold an engine checkpoint across requests.

Keep normal engine GC in the failure matrix. Disabling GC can help isolate an initial engine test, but an end-to-end pass with GC permanently disabled is insufficient evidence. Xenon-specific directory and deduplication retention must be analyzed separately from engine GC.

## 7. Falsifiable validation gates

These are proposed experiments for later implementation approval, not completed work.

| Gate | Fault/interleaving | Pass criterion |
|---|---|---|
| Atomic durability | Delay WAL upload; commit state and pending task; kill owner before and after durability acknowledgement; recover with empty cache | Every acknowledged mutation is recovered wholly; no partial state/task batch |
| Safe publication | Run queue scans and conditional transactions while WAL durability is blocked | No externally actionable volatile state; no condition accepted against a durability-lagged snapshot |
| Transaction conflicts | Race matching version checks, shared ownership guards, write skew and range insert phantoms | Outcomes match the chosen isolation/Temporal contract; conflicting operations cannot both incorrectly succeed |
| Lost response | Commit successfully, drop RPC response, retry against same and replacement owner | No duplicated mutation, correct result/error semantics, no false rollback claim |
| Overlapping writers | Pause old writer, new fencer, and delayed third contender at upstream failpoints | All durable acknowledgements recover; stale writers cannot corrupt accepted history; ownership eventually stabilizes after faults stop |
| Directory failure | Kill at every reservation/open/ready boundary; stale adapters; lost CAS responses; simultaneous controllers | No stale readiness publication; no uncontrolled reopen/fencing loop; correctness-sensitive stale reads handled |
| GC and compaction | Repeat takeover during flush, compaction, WAL/fence GC, and checkpoint refresh | Recovered data complete; live scans protected or fail/retry according to an explicit contract; no required object deleted |
| Dynamic scale-out | Omes active; add Xenon node; move partitions; kill owner; remove local disks | Both nodes demonstrably serve assigned partitions; acknowledged transitions survive; execution and visibility converge correctly |

Use deterministic failpoints to establish interleavings first, then repeat relevant cases on real S3 with the selected SDK/configuration. An emulator passing conditional-write tests does not establish the real service's complete behavior. Record operation IDs, partition/owner generations, durable-ack events, and recovery results so failures have an auditable oracle.

## Decision inputs

Evidence favors testing SlateDB, not implementing a new workflow engine. Open decisions are: read-publication policy; RPC outcome reconciliation; placement/ownership protocol; GC/checkpoint policy; and the transaction partition boundaries covered by the separate research ticket.

Dynamic scalability remains a requirement. A serialized operation gate for an individual partition is a candidate correctness baseline, not a decision to restrict the whole system to one server. Adding nodes can redistribute independent transaction domains once their ownership protocol is proved.
