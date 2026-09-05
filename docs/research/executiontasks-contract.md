# Explicit history task insertion and replication DLQ

Issue #56 completes the six methods absent after the workflow, history and history-task read/completion components. The composed `adapter.ExecutionStore` has all 25 operational methods, lifecycle and branch utility, backed by real Go RPC handlers and one shared connection. This compile-time completeness does not prove Temporal boot or shipping readiness.

Pinned evidence: Temporal v1.31.2 `19a774302c613da9adc4436ab14278ccdca8e0a5`:

- [Complete low-level interface](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/persistence_interface.go)
- [AddHistoryTasks and replication DLQ SQL semantics](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/sql/execution_tasks.go)
- [PostgreSQL DLQ range/insert/delete queries](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/sql/sqlplugin/postgresql/execution.go)
- [Pinned task suite run unchanged](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/tests/execution_mutable_state_task.go)

AddHistoryTasks reuses `stageExecutionTasks` from #45, under the shard range guard and shared owner gate. The same immediate/scheduled keys are used for mutable-state-generated and explicitly inserted tasks. Namespace/workflow/archetype fields are preserved on the wire but impose no extra guard absent in SQL. A duplicate or unsupported category aborts all staged keys before the logical error is journaled separately. Shard ownership errors retain the shard ID. Native failures remain unavailable and quarantine the handle.

The DLQ domain is `v1/replication-dlq/<hex source cluster>/<shard width10>/<signed task ID hex>`. This separates source cluster and shard even when names contain slashes. Put serializes the complete ReplicationTaskInfo, including unknown fields, and preserves immutable first-write-wins semantics on an existing task ID. It does not replace a duplicate payload. Reads return opaque blobs and exact int64 IDs. Individual delete is idempotent; range read/delete use inclusive minimum and exclusive maximum. Read cursors bind source/shard and bounds and continue strictly after the last emitted key. Pages cap the whole encoded result at 3 MiB. Cursor format is Xenon's opaque protocol, not SQL's integer token.

IsReplicationDLQEmpty retains the API's tail-search semantics: request category, maximum, page size and token do not restrict its search. A delegated coordinator decision corrects one pinned SQL edge: SQL searches `[minimum,MaxInt64)` even though Put admits MaxInt64; Xenon includes MaxInt64 in the empty check. This prevents a false empty result over admitted data. Explicit read bounds remain exclusive, and single delete can remove the maximum ID. The proof includes the maximum-ID-only tail regression. This decision was delegated to agents, not asserted as Calum's personal design choice.

All six methods use durable outcomes. Replaying an acknowledged Put after a later delete/reopen does not resurrect the task. Read/empty results and logical failures use the same nonempty durable fencing barrier on replay. No retention expiry is introduced.

The committed manifest runs the exact upstream 15-test ExecutionMutableStateTaskSuite through the composed adapter and an actual Go service process, plus dedicated DLQ RPC and native fault tests. The upstream suite's random seed is fixed by the committed fixture; its runtime-generated times are compared by the upstream oracle. Importing the upstream suite adds test dependency checksums to go.mod/go.sum. No SQL application storage is used. These are memory-object-store proofs; full workflow suites, routing, S3 process failures, Temporal/UI/SDK boot and Omes remain independent gates.
