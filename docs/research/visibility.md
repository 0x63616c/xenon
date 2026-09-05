# Visibility compatibility and S3-backed indexing

Research for [Xenon #4](https://github.com/0x63616c/xenon/issues/4). Date: 2026-09-05. Status: source investigation; recommendations await Calum's approval. No implementation or runtime tests were performed.

## Finding

Visibility is feasible to investigate on an S3-backed KV engine, but SlateDB supplies storage primitives rather than the Temporal query/index implementation. Temporal offers a reusable generic query converter and a separate visibility-store extension interface. Its existing asynchronous visibility processing means execution state and visibility indexes do not require one distributed transaction. A custom store still needs durable, idempotent mutations, compatible typed predicates, pagination, namespace isolation, and CHASM handling. [Store interface][store], [converter][converter], [executor][executor].

Recommended candidate for validation: canonical documents and transactional secondary indexes in dedicated logical visibility partitions, moved between Xenon owners without changing their logical identities. Start the behavioral oracle with a scan evaluator; prove indexed and scan results equivalent. This is a design proposal, not a selected implementation or proof of scalable performance.

## Scope and evidence

The primary Temporal snapshot is commit `891d1b648b7252925142cc36f13e40a0e2ed4244`. Source files linked below were fetched at that exact revision. SlateDB transaction source was read at `v0.16.0`. The earlier assessment's local copy contains unresolved `undefined` revision links; those were not used as citations here.

The final supported Temporal release, Temporal UI release, enabled dynamic configuration, and SQL-versus-Elasticsearch behavioral policy are still decisions. This document does not claim every upstream store has identical observable semantics. A UI release has not been pinned or its network calls audited in this investigation; UI compatibility remains a specific validation gate, not a conclusion from implementing the store interface.

## Compatibility matrix

| Surface | Pinned source behavior | Xenon obligation / unresolved choice |
|---|---|---|
| Write lifecycle | Started, closed, upsert, delete are separate methods. Requests include namespace, workflow/run identifiers, TaskID and (for write base) ShardID. | Preserve request semantics and errors; do not reduce all operations to unconditional document replacement. |
| Read lifecycle | List, count, get plus CHASM list/count. Returned records include memo, search attributes, parent/root identifiers, times, status, history metrics and task queue. | Preserve complete records and typed payloads, not only workflow ID/status. |
| Search attribute administration | Validation returns valid values or InvalidArgument; AddSearchAttributes must be idempotent. | Persist schema/aliases through the correct existing manager path; define schema-change and index-backfill behavior. An empty administrative stub is not full compatibility. |
| Attribute types | Generic converter recognizes Bool, Int, Double, Datetime, Keyword, KeywordList and Text. Datetime input supports RFC3339Nano strings and integer Unix nanoseconds. | Typed sortable encoding, precision policy, negative/numeric boundaries, canonical time normalization; reject invalid query values consistently. |
| Boolean and scalar filters | Converter supplies AND/OR/NOT, comparisons, IN/NOT IN, ranges and IS expressions with type-specific restrictions. | Implement predicates including absent/null behavior; never interpret NOT as merely subtracting from an attribute's present-value index. |
| Keyword | Equality/comparison, IN and prefix operators are accepted; ranges are supported. | Exact-value and ordered/prefix indexes, correct string comparison and escaping. |
| KeywordList | Equality and IN are membership-style operations; inequality/NOT IN are supported. Prefix/range ordering predicates are rejected. | Index each member; de-duplicate matching documents. Evaluate absent/empty lists against the selected backend oracle. |
| Text | Equality/inequality are full-text predicates, not exact string equality. Blank query strings fail validation; IN/range predicates are rejected. | Tokenization and negation must be explicit; do not implement substring matching and label it compatible. |
| Grouping | Generic converter permits one field: ExecutionStatus, TemporalNamespaceDivision, or a TemporalLowCardinalityKeyword-prefixed field. | Count and group result types and validation matter. Arbitrary SQL GROUP BY is not the contract. |
| Ordering | SQL rejects explicit ORDER BY. ES conditionally permits it; Text sorting is rejected by generic conversion. | Choose and publish a policy; SQL-only behavior cannot silently stand in for every ES-supported feature. |
| Pagination | SQL encodes close time/start time/run ID. ES uses SearchAfter and optional manual predicate pagination. | Preserve chosen ordering, missing values and tie-breaking. Tokens must survive owner replacement without depending on a lost in-memory iterator. |
| Isolation/divisions | Namespace filters and default TemporalNamespaceDivision filtering are applied by the query layers. CHASM mapping changes field resolution and archetype filtering. | Namespace ID must constrain every scan, index, count, get and token. A division is not a namespace security boundary. |
| Eventual visibility | Execution's visibility tasks are processed separately; store success allows processing to progress. | A visibility mutation must be durably committed before acknowledgement; query lag must be defined separately from durability. |

Evidence: [interface][store], [converter][converter], [SQL store][sql], [ES store][es], [PostgreSQL query builder][pgquery], [shared converter cases][cases].

### Differences that require an explicit policy

1. **Custom ordering.** SQL returns an unsupported-clause error; ES checks a namespace dynamic setting and can accept custom ordering. Supporting the SQL surface is a viable first milestone only if recorded as such, rather than declaring all visibility features complete. [SQL][sql], [ES][es].
2. **Text analyzers.** For a query such as Text = 'foo bar', shared test cases emit PostgreSQL token OR queries, SQLite FTS queries, MySQL natural-language search, and ES match queries. This establishes different execution machinery; it does not establish identical stemming, punctuation, Unicode or stop-word behavior. Use real backend results as oracles. [Cases][cases], [PostgreSQL token conversion][pgquery].
3. **Missing attributes under negation.** PostgreSQL KeywordList != 'foo' becomes NOT of a JSON containment expression, whereas ES uses must_not term clauses. Missing/null fields can therefore diverge. This is an inference from their generated predicates that requires differential runtime confirmation, not a test result. [Cases][cases].
4. **Time precision and pagination.** PostgreSQL formats microseconds and uses a maximum-time substitute for absent CloseTime. Default order is CloseTime descending, StartTime descending, then RunID ascending. ES's inspected default sort has CloseTime and StartTime with missing-first handling; do not assume the SQL tie-breaker applies to every ES query. [PostgreSQL query builder][pgquery], [ES][es].

## Mutation ordering and durable acknowledgement

The SQL store sets visibility Version from TaskID. PostgreSQL start insertion leaves an existing row unchanged; upsert replaces only when the incoming version is strictly greater. A replay of the same TaskID must therefore not regress state. [SQL mapping][sql], [PostgreSQL mutations][pg].

Deletion is especially important: PostgreSQL physically deletes by namespace/run ID without a retained version guard. The visibility task executor has an optional close-before-delete dependency check specifically to prevent a late close task resurrecting a record. ES passes TaskID on both index and delete bulk requests. Preserve the upstream task-processing ordering; do not assume an upsert version alone solves deletion. [PostgreSQL][pg], [executor][executor], [ES][es].

Proposed KV mutation transaction: compare the canonical document version, remove superseded index entries, write new document/version, add replacement index entries, and commit atomically within one visibility partition. On delete, remove document/index entries and consider a versioned tombstone. Tombstone retention/collection needs an explicit safe replay horizon; deleting it immediately recreates the late-write problem. Do not invent an unbounded global TaskID order across unrelated records.

SlateDB's transaction commit returns a write handle whose durability can be awaited. Xenon must wait for the durable acknowledgement boundary before returning success, and ensure its query readers do not depend on volatile index state that could disappear on owner replacement. Document and indexes must become query-visible consistently. [SlateDB transaction implementation](https://github.com/slatedb/slatedb/blob/v0.16.0/slatedb/src/db_transaction.rs).

A lost RPC response after commit is an ambiguous outcome: retry must be harmless. Fault tests need to pause before commit, after commit but before durability, after durability but before response, and after response before visibility-task acknowledgement.

## Placement and index options

These are proposed designs inferred from the contracts above; none has been benchmarked or validated.

| Option | Query/transaction benefit | Cost and scaling limit |
|---|---|---|
| Colocate visibility indexes with execution partitions | Familiar shard routing for writes; keeps per-document index changes local. | Every namespace query may fan out across many execution partitions. Execution and search compete for CPU/cache; execution ownership movement becomes query routing churn. Colocation is not needed for cross-store atomicity. |
| One visibility partition per namespace | Local list/count/group and document/index transactions. Namespaces independently move between nodes. | One large namespace remains limited by a single writer/partition; adding nodes does not scale that namespace. |
| Fixed logical visibility subpartitions per namespace | Parallel writes and scans; move existing partitions among nodes without repartitioning keys. | Fan-out list and count; distributed merge and pagination; many small databases can add background work. This is the strongest candidate for demonstrating storage-node scale-out within one namespace. |
| Separate distributed global indexes | Fast selective lookups can avoid scanning all document partitions. | A document mutation affects independently owned index partitions: distributed transactions or a durable asynchronous indexing protocol with watermarks/reconciliation. More correctness machinery. |
| SQLite/VFS over S3-backed storage | Potential reuse of SQL predicate, index and collation behavior. | Introduces page/snapshot/commit and writer-ownership concerns; does not itself solve distributed query or namespace scale-out. Requires a separate feasibility gate. |

For the KV candidate, useful key families are namespace/partition-scoped canonical documents, default-order keys, typed attribute/value/document postings, Text token postings, and version/tombstone records. Store raw typed fields as the final predicate authority. Candidate-index scans must recheck predicates and de-duplicate hits, especially KeywordList and OR expressions. Range encoding, reverse order, null markers and index schema versions need tests.

An initial full scan can prove predicate behavior on small fixtures. It cannot establish useful search performance. Counts can initially aggregate exact scans; precomputed counts require transactional updates or an explicitly lagging projection. Negation, low-selectivity filters and arbitrary sort order can remain expensive even with indexes. Slower is acceptable, but object GET/PUT costs, compaction, memory and query fan-out must be measured separately.

## Pagination during scale-out

Keep **logical partition IDs stable when changing owners**. A query planner resolves current owners for a fixed partition set; moving a partition should not change which documents belong to that query.

For a default globally ordered list, each partition returns a local sorted batch. The coordinator merges batches and emits the first requested number of rows. A proposed durable token records format/version, namespace, normalized-query identity, sort definition, logical partition-set version, and the last emitted global sort key (or per-partition positions where needed). It should not expose node addresses as durable cursor identity. Bind tokens to their query and namespace and reject malformed/mismatched tokens.

Under a globally unique total-order key, every partition can continue strictly after the last emitted key. A stable unique document identity is necessary; timestamp ties alone are insufficient for a deterministic Xenon merge. Unsupported custom ordering needs a clear error rather than a silently different order.

**A stable routing token is not a cross-page snapshot.** Concurrent upserts can move a document across the pagination boundary, and independently read partitions need not share one instant of truth. Two policy options:
- Match a chosen upstream store's live keyset behavior, documenting that ongoing mutations can change subsequent pages.
- Provide snapshot pagination with durable per-partition snapshot identifiers, retention, expiry and recovery. This is a stronger and more expensive additional design.

Do not claim exactly-once, snapshot-complete pagination merely because rebalance retries succeed. Counts computed independently of lists may differ during writes. For the proof, test a frozen dataset for no omissions/duplicates across rebalance, then separately characterize concurrent-mutation behavior against the selected oracle.

On owner loss, retry only the affected logical partition's operation with the same cursor semantics. Never return a successful partial count/list because one partition is unavailable. Node addition should move ownership, not split partitions in the same experiment; key-space splitting introduces another partition-set migration protocol.

## CHASM and UI

CHASM list/count are actual methods in the pinned VisibilityStore interface. The executor places ArchetypeID in TemporalNamespaceDivision and adds CHASM memo data; mapper and system-field override paths exist. Generic conversion defaults ordinary workflow queries to the appropriate division restriction and can be supplied a CHASM mapper. Omitting these paths is an explicit coverage gap, not an invisible implementation detail. [Interface][store], [executor][executor], [converter][converter], [SQL][sql].

Retaining the Temporal frontend makes SDK/UI protocol reuse plausible. Visibility conformance alone does not prove the UI: it also reads workflow history and other frontend APIs. Pin an existing UI release, inspect its workflow-list/filter/count/detail interactions, and exercise those with unchanged UI code. This investigation has not verified a specific UI build.

## Proposed conformance gates

1. **Choose baseline before implementation:** Temporal revision/release, dynamic flags, SQL/ES policy, CHASM proof scope, UI release, and expected live-versus-snapshot pagination behavior. Preserve fuller compatibility as a goal; label exclusions.
2. **Upstream store suites:** adapt existing visibility cases including TestBasicVisibilityTimeSkew, TestBasicVisibilityShortWorkflow, TestPaginationEdgeCase, TestAdvancedVisibilityPagination, TestUpsertWorkflowExecution, TestDeleteWorkflow, TestGetWorkflowExecution and TestCountGroupByWorkflowExecutions. [Suite][suite].
3. **Query differential fixtures:** every supported type/operator, malformed expressions, unknown attributes/aliases, Unicode/token punctuation, missing/null/empty lists, numeric/time limits, ties, group validation, namespace collisions, CHASM division. Run the same inputs against Xenon and the selected real upstream stores. Converter-output tests alone are not result-set tests. [Cases][cases].
4. **Mutation faults:** duplicate and reordered start/upsert/close/delete, lost response, delayed object writes, restart with empty cache; verify no acknowledged document/index mutation disappears or stale record resurfaces.
5. **Scale-out search:** populate multiple logical visibility partitions in one namespace, query with small pages, move ownership/add a node between pages, kill old owner, resume from token. Frozen dataset must return the expected full set without duplication. Verify counts include every partition.
6. **End-to-end:** Omes drives actual execution and search-attribute updates where covered; explicit API probes check eventual visibility and query results; unchanged pinned UI lists/filters/opens workflows before and after crashes. A successful Omes workload alone does not establish these guarantees.

## Decision for Calum

Approve or revise **dedicated fixed logical visibility partitions with transactional KV indexes** as the first design to validate, and select the compatibility baseline. The main trade-off is scalable writes within a namespace versus distributed query/pagination work. No option here is a confirmed drop-in store.

## Primary sources

[store]: https://github.com/temporalio/temporal/blob/891d1b648b7252925142cc36f13e40a0e2ed4244/common/persistence/visibility/store/visibility_store.go
[converter]: https://github.com/temporalio/temporal/blob/891d1b648b7252925142cc36f13e40a0e2ed4244/common/persistence/visibility/store/query/converter.go
[sql]: https://github.com/temporalio/temporal/blob/891d1b648b7252925142cc36f13e40a0e2ed4244/common/persistence/visibility/store/sql/visibility_store.go
[pg]: https://github.com/temporalio/temporal/blob/891d1b648b7252925142cc36f13e40a0e2ed4244/common/persistence/sql/sqlplugin/postgresql/visibility.go
[pgquery]: https://github.com/temporalio/temporal/blob/891d1b648b7252925142cc36f13e40a0e2ed4244/common/persistence/sql/sqlplugin/postgresql/query_converter.go
[es]: https://github.com/temporalio/temporal/blob/891d1b648b7252925142cc36f13e40a0e2ed4244/common/persistence/visibility/store/elasticsearch/visibility_store.go
[executor]: https://github.com/temporalio/temporal/blob/891d1b648b7252925142cc36f13e40a0e2ed4244/service/history/visibility_queue_task_executor.go
[cases]: https://github.com/temporalio/temporal/blob/891d1b648b7252925142cc36f13e40a0e2ed4244/common/persistence/visibility/store/tests/query_converter_test.go
[suite]: https://github.com/temporalio/temporal/blob/891d1b648b7252925142cc36f13e40a0e2ed4244/common/persistence/tests/visibility_persistence_suite.go
