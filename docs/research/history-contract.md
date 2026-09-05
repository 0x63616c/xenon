# History branch and node contract

[Issue #34](https://github.com/0x63616c/xenon/issues/34) implements the seven history operations of pinned Temporal v1.31.2. `adapter.HistoryStore` is a component, not a falsely complete ExecutionStore. The same Go node endpoint registers HistoryPersistence; methods run through the shared owner and journal with result oneof field6. Shard2/namespace3/cluster4/queue5 remain reserved for their existing families.

Primary source commit `19a774302c613da9adc4436ab14278ccdca8e0a5`: [history_store.go](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/sql/history_store.go), [PostgreSQL events.go](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/sql/sqlplugin/postgresql/events.go), [interfaces](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/persistence_interface.go), [branch utilities](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/history_branch_util.go).

## Operation behavior

| Operation | Implemented behavior |
| --- | --- |
| AppendHistoryNodes | Preserve events/encoding, node/previous/current transaction IDs. PostgreSQL uses exact-key upsert, so duplicates overwrite that key rather than always returning ConditionFailed. New-branch node and opaque tree info commit atomically with replay outcome. |
| DeleteHistoryNodes | Delete exact node/transaction key, idempotently. Reject deletion below the branch begin node (last ancestor EndNodeId, or1) with InvalidPersistenceRequestError. |
| ReadHistoryBranch | Parse branch token using pinned serializer; read the requested BranchID within token's tree. Range [min,max). Forward node ascending/transaction descending; reverse opposite. Metadata-only omits Events and preserves sequence. Live cursor stores int64 node/transaction fields; no float conversion. |
| ForkHistoryBranch | Upsert opaque tree info under new branch UUID in existing tree. Pinned low-level SQL does not validate fork-node existence or rewrite ancestry; Temporal's manager prepares serialized tree info. Preserve request tokens, branch-info protobuf/unknown fields, fork node and cleanup info on wire. |
| DeleteHistoryBranch | Atomically remove target tree record and all requested ancestor/branch node ranges with NodeID >= BeginNodeId. Validate all range UUIDs before staging writes. |
| GetAllHistoryTreeBranches | Ordered shard/tree/branch scan with opaque cursor, inside this configured logical partition. Multi-partition fanout and routing remain required before factory/Temporal shipping. |
| GetHistoryTreeContainingBranch | Return all opaque tree-info blobs for requested shard/tree, empty when absent. Fail explicitly if complete nonpaginated result cannot fit response budget; never return partial success. |

## Delegated PostgreSQL corrections

Root source verification and independent advocate review approved intended reverse semantics. Pinned events.go supplies `filter.MaxTxnID` to a SQL argument comparing `node_id`, and selects the forward metadata query even for reverse options. Xenon uses node DESC/transaction ASC for reverse and the same sequence in metadata-only mode. Forward remains node ASC/transaction DESC. Tests deliberately separate node IDs1/3/8 from transaction IDs1001/7001/9001/12001, split ties across pages, and compare complete forward/reverse/metadata-only sequences. This is an explicit delegated decision, not a claim of Calum selecting query internals or blanket PostgreSQL equivalence.

Ordered keys encode signed node values and descending signed transaction IDs without PostgreSQL's negation overflow. Native descending scan reverses both components. Local and remote cancellation of non-new append becomes AppendHistoryTimeoutError; new-branch transaction cancellation becomes Unavailable, matching txExecute's wrapping. Other uncertain engine outcomes remain Unavailable and quarantine the owner. The wire supports logical ConditionFailed and AppendTimeout errors but ordinary PG exact-key upserts do not emit duplicate errors.

History nodes and trees live under versioned keys containing HistoryShardID, treeUUID and branchUUID. A stable history shard is the transaction domain. This checkpoint does not implement ownership routing or assert that all history prewrites and workflow mutable-state operations are one SQL transaction. The existing complete-operation journal preserves IDs, command digests, result family and durable replay barrier.

## Bounds and proof

`python3 scripts/prove.py go-history` builds the pinned native engine and single Go binary then runs four named tests. The clean report binds source/config/library/binary hashes. Actual gRPC covers all seven operations, declared lost response, ordering and range boundaries, upsert, fork and pagination. Native tests abort a staged new branch+node before journal publication, verify all three keys absent, reopen acknowledged state, replay without overwriting a later upsert, and fence the old owner. Large-node pages fit the default receive limit and continue without omissions. Cancellation tests distinguish exact Temporal error types.

Payloads are <=1MiB each; the shared node's 2MiB full-request gRPC ceiling also applies (two maximum blobs plus envelope cannot fit). Full responses are <=3MiB, page counts1..1000; GetTree has no partial continuation and fails ResourceExhausted beyond that budget. These are explicit checkpoint ceilings, not full Temporal payload compatibility claims. Response pagination may return fewer than requested records when byte-limited. A nonpaginated large tree needs an internal chunked RPC or coherent larger transport before claiming unbounded tree compatibility.

The exported upstream `NewHistoryEventsSuite(t, p.ExecutionStore, logger)` requires a complete ExecutionStore and creates an ExecutionManager. It cannot honestly run against this component without implementing the remaining interface; no fake successful stubs were added. Full upstream suite execution remains gated on the workflow execution store. Real S3, process-crash recovery, global scans/forwarding and Temporal/Omes remain separate shipping requirements.
