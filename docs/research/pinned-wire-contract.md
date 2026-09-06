# Pinned Temporal persistence wire contract

Independent source audit for the Xenon operation RPC, 2026-09-05. Baseline: Temporal **v1.31.2**, commit `19a774302c613da9adc4436ab14278ccdca8e0a5`. This is a bounded source audit and acceptance plan, not executed codec conformance. No codec implementation is approved by a successful SQLite snapshot experiment.

## Recommendation

The user subsequently explicitly required direct S3 storage; the coordinator returned the engine candidate to SlateDB. SQLite observations below are historical wire/behavioral-oracle evidence only, not a storage recommendation. The proposed wire contract is engine-independent.

Keep Temporal's low-level store interfaces on the client and dispatch complete operations to Xenon. Define versioned operation-specific protobuf request/response messages, with explicit conversion for ordinary Go structs; retain concrete upstream protobuf fields as typed protobuf messages or binary protobuf payloads decoded to a statically known message type. Do not serialize arbitrary Go objects with generic JSON or gob and assume fidelity.

The request envelope carries protocol version, operation kind, logical partition, owner generation, operation ID and request digest. Factory configuration carries queue type and ordinary/fair matching mode into each applicable request. Context is transport state, never payload. Responses carry both the operation's normal return values and its typed logical error, plus explicit mutable-request outputs. Persist the exact logical outcome with the mutation before acknowledgement; gRPC transport status remains distinct from a recorded operation outcome.

Use explicit bounded frame/database limits. Reject unknown operation versions, unknown category identities and malformed payloads before invoking SQL. Derive retry digests from a canonical encoding of the immutable logical input; exclude deadlines, current routes and output-pointer addresses. Proto deterministic serialization alone is not a promise of canonical encoding across arbitrary implementations or versions; pin the encoder/version and normalize relevant map/set ordering.

## Exact operation inventory

The following is the pinned low-level operation inventory. Local factory/identity/helper methods are listed separately. [Store interfaces][interfaces], [visibility interface][visibility].

| Interface | Operations |
|---|---|
| ShardStore | GetOrCreateShard, UpdateShard, AssertShardOwnership |
| TaskStore (both ordinary and fair factories) | CreateTaskQueue, GetTaskQueue, UpdateTaskQueue, ListTaskQueue, DeleteTaskQueue, CreateTasks, GetTasks, CompleteTasksLessThan, GetTaskQueueUserData, UpdateTaskQueueUserData, ListTaskQueueUserDataEntries, GetTaskQueuesByBuildId, CountTaskQueuesByBuildId |
| MetadataStore | CreateNamespace, GetNamespace, UpdateNamespace, RenameNamespace, DeleteNamespace, DeleteNamespaceByName, ListNamespaces, GetMetadata |
| ClusterMetadataStore | ListClusterMetadata, GetClusterMetadata, SaveClusterMetadata, DeleteClusterMetadata, GetClusterMembers, UpsertClusterMembership, PruneClusterMembership |
| ExecutionStore | CreateWorkflowExecution, UpdateWorkflowExecution, ConflictResolveWorkflowExecution, DeleteWorkflowExecution, DeleteCurrentWorkflowExecution, GetCurrentExecution, GetWorkflowExecution, SetWorkflowExecution, ListConcreteExecutions, AddHistoryTasks, GetHistoryTasks, CompleteHistoryTask, RangeCompleteHistoryTasks, PutReplicationTaskToDLQ, GetReplicationTasksFromDLQ, DeleteReplicationTaskFromDLQ, RangeDeleteReplicationTaskFromDLQ, IsReplicationDLQEmpty, AppendHistoryNodes, DeleteHistoryNodes, ReadHistoryBranch, ForkHistoryBranch, DeleteHistoryBranch, GetHistoryTreeContainingBranch, GetAllHistoryTreeBranches |
| Queue | Init, EnqueueMessage, ReadMessages, DeleteMessagesBefore, UpdateAckLevel, GetAckLevels, EnqueueMessageToDLQ, ReadMessagesFromDLQ, DeleteMessageFromDLQ, RangeDeleteMessagesFromDLQ, UpdateDLQAckLevel, GetDLQAckLevels |
| QueueV2 | EnqueueMessage, ReadMessages, CreateQueue, RangeDeleteMessages, ListQueues |
| NexusEndpointStore | CreateOrUpdateNexusEndpoint, DeleteNexusEndpoint, GetNexusEndpoint, ListNexusEndpoints |
| VisibilityStore | RecordWorkflowExecutionStarted, RecordWorkflowExecutionClosed, UpsertWorkflowExecution, DeleteWorkflowExecution, ListWorkflowExecutions, CountWorkflowExecutions, GetWorkflowExecution, ListChasmExecutions, CountChasmExecutions, AddSearchAttributes; ValidateCustomSearchAttributes is a local policy candidate described below |

Method names are scoped by interface: execution and visibility DeleteWorkflowExecution are distinct operations, as are Queue and QueueV2 methods.

Keep Close, GetName, GetClusterName, GetIndexName and GetHistoryBranchUtil local. DataStoreFactory constructors produce local wrappers and carry configuration; they are not arbitrary remote object construction. Keep HistoryBranchUtil's serialization helper implementation pinned upstream. Compile-time interface assertions must cover every wrapper, including NewFairTaskStore and QueueV2.

## Exact exceptions to ordinary struct conversion

| Methods / values | Hazard and required treatment |
|---|---|
| GetOrCreateShard | InternalGetOrCreateShardRequest.CreateShardInfo is a function with `json:"-"`. Existing-shard lookup must not eagerly invoke a potentially failing callback. Prefer an initial read-only request; on NotFound invoke the callback locally once, then submit a create-or-get request with concrete RangeID and DataBlob. Race with another creator returns the existing shard. Mirror upstream callback-error wrapping. Upstream mutates the callback to nil on duplicate insert as loop control; do not attempt to serialize executable callbacks. [Shard implementation][shard] |
| CreateWorkflowExecution, UpdateWorkflowExecution, ConflictResolveWorkflowExecution, SetWorkflowExecution | Nested InternalWorkflowSnapshot/InternalWorkflowMutation contain ExecutionInfoBlob and ExecutionStateBlob tagged `json:"-"`; SQL consumes the blobs. Transmit them explicitly as well as decoded ExecutionInfo/ExecutionState used for conditions. Do not assume JSON's omission is safe or silently regenerate potentially different bytes. [Execution helpers][execution] |
| Those four workflow methods plus AddHistoryTasks | `map[tasks.Category][]InternalHistoryTask`: Category has unexported id/type/name and MarshalText but no UnmarshalText. Encode repeated category/task groups with numeric ID, type and name checked against the pinned registry, or an equivalent explicit DTO. Preserve every task blob and key; an empty map is not a successful task conversion. [Category][category] |
| GetHistoryTasks, CompleteHistoryTask, RangeCompleteHistoryTasks | TaskCategory is a struct value with unexported fields; plain JSON can silently produce `{}`. Apply the same category conversion. [Public request structs][requests] |
| UpdateTaskQueueUserData | Applied and Conflicting are optional `*bool` output pointers within each map entry. SQL sets Conflicting on the offending queue and Applied after the complete transaction, including error outcomes. Response must include keyed presence/value outputs, then write through the caller's existing pointers; replacing request objects does not update caller variables. Preserve initially true values where upstream does not overwrite them. Persist outputs for retry replay. [User-data implementation][userdata] |
| ValidateCustomSearchAttributes | Signature is `map[string]any` to `map[string]any`, with no context. Generic JSON converts numbers to float64, losing int64 precision above 2^53 and concrete list/time types. Pinned SQL implementation returns the input unchanged: use that same local policy for SQL baseline, explicitly not a claim of full validation. If validation becomes remote, define an exhaustive tagged value union and test types/precision; protobuf Struct is insufficient for arbitrary int64 fidelity. [SQL visibility][sqlvisibility] |
| ListChasmExecutions, CountChasmExecutions | Requests already are concrete visibilityservice protobuf messages; retain typed binary protobuf serialization. Never decode generated oneof interfaces with ordinary Go JSON. [Visibility interface][visibility] |
| All protobuf-bearing methods | Generated persistence messages can have oneofs, e.g. WorkflowExecutionInfo.LastWorkflowTaskFailure. Use protobuf encoding, preserve unknown fields, distinguish nil/absent from present empty message where callers care. Existing DataBlob data is opaque; preserve EncodingType and bytes exactly. [Generated execution messages][protos] |
| Queue.ReadMessagesFromDLQ | Returns messages, a separate page-token byte slice and error: preserve all outputs, including any nonnil outputs paired with error. Queue.EnqueueMessageToDLQ returns an allocated int64. Queue type is constructor state required in the operation identity and routing. |
| CompleteTasksLessThan, CountTaskQueuesByBuildId, GetTaskQueuesByBuildId, SaveClusterMetadata, IsReplicationDLQEmpty | Responses are int, int, string slice, bool, bool respectively, rather than uniform response pointers. Use signed bounded integer wire fields and checked Go-int conversion; preserve false/zero as actual results. |
| Every context-bearing method | Forward deadline and cancellation through gRPC context. A cancelled/lost response does not establish rollback. On retry reuse operation ID and payload, reconcile durable outcomes, and never map an uncertain commit to a definite condition failure or fabricated success. |
| Every pageable method | Tokens are opaque bytes locally; global fanout needs a versioned logical-partition cursor. Preserve byte sequences; bind tokens to namespace/query and never encode ephemeral owner addresses. Do not return successful partial lists/counts when one partition failed. |

Additional shared scalar obligations: int64 versions/TaskIDs/timestamps cannot round-trip through floating-point intermediary values; preserve pointer presence, empty versus absent collection behavior where SQL branches on it, time instants including zero time and nanoseconds, duration units, enum numeric values, UUID bytes/IP byte slices, CHASM archetype IDs, and int64-keyed maps/sets. Time monotonic clock components are process-local and must not become equality requirements for distributed timestamps.

## Error union

Reconstruct the concrete persistence error types from [data_interfaces.go][requests], with all fields:

- InvalidPersistenceRequestError, AppendHistoryTimeoutError, ConditionFailedError, ShardAlreadyExistError, TimeoutError, TransactionSizeLimitError: Msg.
- ShardOwnershipLostError: ShardID, Msg.
- WorkflowConditionFailedError: Msg, NextEventID, DBRecordVersion.
- CurrentWorkflowConditionFailedError: Msg, RequestIDs (protobuf RequestIDInfo values), RunID, State, Status, LastWriteVersion, optional StartTime.

Preserve upstream serviceerror status/details using the pinned serviceerror conversion mechanism; specifically test namespace already-exists/not-found and invalid-argument/unavailable cases. Reconstruct context cancellation/deadline identities where appropriate. Unexpected backend errors need an explicitly documented fallback and observability; do not turn all errors into `Internal` or reinterpret an engine conflict as a Temporal condition failure. Tests must assert `errors.As`/`errors.Is` and structured fields, not only Error() text. Transport failure and application error must remain distinguishable in the client retry policy.

SQL methods sometimes commit history before later execution-state conditions fail. A returned logical error cannot imply an unchanged snapshot. Snapshot the resulting state and persist the exact outcome when required; visibility and user-data error/output fidelity are separate from successful mutation replay.

## SQLite experiment review

Read-only review of `internal/experiment/sqlite_test.go`: it creates a real pinned shard, updates RangeID, checks a typed stale RangeID failure, uses VACUUM INTO, restores to a distinct pathname and reads back the blob. That supports a narrowly stated standalone snapshot smoke test if its command passes. It does not exercise RPC, S3, lost acknowledgements, same-path rollback, process death, mutable request flags or any workflow/category conversion.

Upstream [SQLite connection pool][pool] deliberately leaves the underlying connection open, and its Close path does not write its local decremented entry back to the pool. Repeated fresh path opens can leak handles; factory.Close does not prove release. VACUUM INTO avoids depending on connection closure for this smoke snapshot, but production lifecycle still requires bounded connection ownership and safe disposal/reload. Verify journal mode and all operations are gated before claiming snapshots are consistent under cancellation or concurrency. No tests were executed by this audit.

## Runnable acceptance plan (not present/passed yet)

Implement these named suites with checked-in fixtures; commands below are required future acceptance commands, not reports of existing runnable tests:

1. `go test ./internal/wire -run TestPinnedInterfaceCoverage -count=1`: compiler assertions for every store; registry comparison against the exact pinned method inventory; fail on omitted/new methods.
2. `go test ./internal/wire -run 'TestRoundTrip|TestMalformed' -count=1`: fixture for every operation; explicit workflow protobuf oneofs/unknown fields, excluded blobs, task categories/maps, int64 extremes, times, nil pointers, tokens and multi-return responses. Reject unsupported schema/category/types before SQL.
3. `go test ./internal/temporal/adapter -run 'TestShardCallback|TestUserDataOutputs|TestTypedErrors' -count=1`: callback unused for existing shard, callback error, concurrent creators; two user-data queues with one conflict and nil/non-nil output pointers; every concrete error field and caller-visible identity.
4. `go test ./internal/temporal/adapter -run 'TestLostResponse|TestPayloadMismatch|TestDeadline' -count=1`: real gRPC transport response loss after durable mutation, same/replacement owner replay after intervening commits, duplicate ID with changed payload rejected, cancellation at pre/post-publication boundaries.
5. `go test ./internal/temporal/adapter -run 'TestContinueAsNew|TestTaskCategories|TestChasmWire' -count=1`: SQL-backed create/update/reset/Continue-As-New through actual wire codec; verify state and tasks, every category's key ordering, CHASM requests and visibility records.
6. `go test ./internal/experiment -run TestTemporalShardSnapshotRecovery -count=1 -v`: existing bounded snapshot experiment only. Extend lifecycle tests separately to repeated restores, cancellation, process recovery and resource counts before claiming a production snapshot mechanism.

[interfaces]: https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/persistence_interface.go
[requests]: https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/data_interfaces.go
[visibility]: https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/visibility/store/visibility_store.go
[shard]: https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/sql/shard.go
[execution]: https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/sql/execution_util.go
[category]: https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/service/history/tasks/category.go
[userdata]: https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/sql/task_user_data.go
[sqlvisibility]: https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/visibility/store/sql/visibility_store.go
[protos]: https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/api/persistence/v1/executions.pb.go
[pool]: https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/sql/sqlplugin/sqlite/conn_pool.go
