# Visibility specification

Status: delegated design decision for [Wayfinder #7](https://github.com/0x63616c/xenon/issues/7), 2026-09-05. Implementation and all conformance tests below are pending. This document makes no runtime compatibility claim.

## Behavioral contract

Retain Temporal Server v1.31.2 (`19a774302c613da9adc4436ab14278ccdca8e0a5`) and implement its complete visibility interface. Use the pinned Temporal PostgreSQL SQL visibility implementation as the differential behavioral oracle. PostgreSQL exists only in isolated conformance tests; Xenon stores documents, indexes, versions, schema and ownership directly in S3 through SlateDB. No SQL service participates in Xenon durability or query execution.

The required implementation includes started/closed/upsert/delete, get/list/count/group, every supported search-attribute type and predicate, full returned records, search-attribute administration, namespace aliases, CHASM list/count and division mapping. SQL rejects explicit `ORDER BY`; Xenon returns the corresponding validation error rather than silently ignoring the clause. This is an explicit SQL compatibility policy, not a general permission to leave feature surfaces unsupported. Elasticsearch-only behavior is not claimed. Missing required behavior remains an implementation blocker.

## Placement and isolation

Use exactly four global logical visibility partitions: `vis-v1-0`, `vis-v1-1`, `vis-v1-2`, `vis-v1-3`. Parse namespace and run UUIDs to their 16-byte representations. Compute SHA-256 over the 32-byte concatenation `namespace_uuid || run_uuid`; interpret the first eight digest bytes as an unsigned big-endian integer and take modulo four. Reject malformed identifiers; never hash case-dependent textual UUID representations.

Persist partition-format version 1 and count four in bootstrap configuration. The mapping is immutable for this format. Rebalancing moves owners of stable partitions, not documents between partition IDs. Four bounds engine-background overhead while supporting useful placement across two or three nodes. All namespaces share these partitions, so global throughput and noisy-neighbor ceilings must be measured and documented. Future partition-count changes require an explicit migration, beyond ownership movement.

Every get, scan, count and index lookup is constrained by namespace. Workflow queries apply the pinned default namespace-division behavior; CHASM queries use the proper archetype and mapper. The namespace is authenticated request context and must not be supplied solely by an AST or continuation token. Namespace collisions, cross-division records and hostile tokens are mandatory fixtures.

## Query conversion and types

Implement Temporal's generic `StoreQueryConverter[ExprT]` in Go with `ExprT` representing Xenon's typed AST. Retain the pinned parser, attribute resolution, type checking, grouping restrictions and CHASM mapping. Send explicit protobuf nodes from the Go adapter to the Go storage node; do not send SQL strings or generic JSON values and do not parse Temporal query syntax again in the node.

Represent Boolean operators, comparisons, IN/NOT IN, ranges, null predicates and type-specific Keyword/KeywordList/Text operations. Scalar values preserve bool, signed int64, double, normalized datetime and string; lists preserve element type. Distinguish missing/null from false, zero and empty collections. Evaluation uses TRUE/FALSE/UNKNOWN where the PostgreSQL oracle does: only TRUE selects a record, and NOT UNKNOWN remains UNKNOWN. Do not derive negation solely by subtracting present-value postings.

The AST carries schema/alias version and mapped physical field/type identity. The Go RPC boundary rejects unknown node versions, invalid value types and excessive depth/size. Adapter conversion does not eliminate storage-node request validation. Normalize the AST deterministically before computing its query digest; pin the normalization format and test equivalent map/order handling.

All types are required: Bool, Int, Double, Datetime, Keyword, KeywordList and Text. Preserve raw typed search attributes and opaque memo payloads in returned records, along with workflow/run IDs, task queue, parent/root identifiers, status, timestamps and history metrics. Preserve CHASM record payloads and system-field overrides through the existing mapper contract.

## Exact semantic questions and required differential tests

The PostgreSQL oracle is the chosen policy; the following details need executable fixtures before compatibility is asserted. An unresolved detail is not permission to approximate it.

| Surface | Pinned evidence / required result |
|---|---|
| Text | The schema casts stored strings to `tsvector`; query conversion space-splits then joins terms with OR and casts to `tsquery`. This is not English stemming or a proven whitespace-membership algorithm. Test lexical quoting, escapes, punctuation, case, Unicode, blank queries and invalid lexemes against the actual pinned PostgreSQL path. |
| Double | Custom slots use `DECIMAL(20,5)` in the inspected schema. Establish rounding, range rejection and comparison behavior with boundary fixtures; float64 equality alone is insufficient evidence. Preserve original returned payloads separately from comparison normalization. |
| Datetime | Establish microsecond normalization, timezone equivalence, integer-nanosecond input, zero/extreme bounds and close-time sentinel behavior through fixtures. |
| Keyword | Match exact comparison, ordering, prefix and escaping behavior; document and pin test-database collation. Do not inherit host-dependent locale ordering. |
| KeywordList | Match membership and negation for missing, null, empty and duplicate entries. OR/index unions must de-duplicate records. |
| Boolean logic | Cover UNKNOWN through nested AND/OR/NOT, IS NULL/IS NOT NULL, negative numeric bounds and invalid operators. |
| Grouping | Preserve the pinned single-field grouping restriction and allowed ExecutionStatus, TemporalNamespaceDivision and TemporalLowCardinalityKeyword fields, including absent-group representation. |

The conformance harness must pin the PostgreSQL image/digest, schema, collation and relevant settings before recording results. No exact database-container pin or analyzer-equivalence pass is established by this specification.

## Documents, indexes and mutation ordering

Each partition contains canonical documents keyed by namespace/run identity, per-document mutation version, deletion tombstones, default-order entries and typed attribute indexes. All changed document/index/version keys commit in the same SlateDB transaction under the partition admission/durability policy. Preserve TaskID-based ordering only within the same document; do not infer a global TaskID clock.

Started, closed and upsert retain their distinct pinned semantics. Duplicate and older updates cannot regress a newer canonical record. Delete removes visible document/index entries and retains a versioned tombstone. Preserve upstream close-before-delete processing; test delayed starts/closes/upserts after deletion. Tombstones remain retained for the bounded proof until a safe replay/GC horizon is demonstrated. Enforced capacity limits must fail clearly before acknowledgement; silently expiring replay protection is forbidden.

Canonical records are the final predicate authority. Index candidates must be rechecked, including OR and list de-duplication. Initial exact scans may implement difficult predicates while indexes are completed, but shipped performance must satisfy the acceptance targets. Index schema versions identify whether a partition can answer from an index; incomplete backfills use a correct canonical scan or explicit retry, never incomplete successful results.

Search-attribute definitions and aliases are durably versioned through the control domain and existing Temporal administration path. Repeated compatible administration is idempotent; incompatible type changes return a clear error. Query conversion and execution agree on the schema/alias version. Schema/index rollout must support correct fallback while partitions catch up, without requiring a cross-partition document/index transaction. Test schema persistence after cache loss, alias reuse, concurrent schema/query operations and backfill recovery.

## Lists, counts and continuation

Namespace queries fan out to all four logical partitions. Each applies namespace/division restrictions and returns correctly ordered matching records. Merge using the pinned SQL default order: coalesced CloseTime descending, StartTime descending, then RunID ascending. Use the pinned maximum-time substitution for missing CloseTime, and establish exact time normalization in differential fixtures. RunID provides the final unique key within the namespace.

A continuation token includes token-format version, namespace, normalized AST digest, schema/alias version, partition-format version and last globally emitted sort key. Tokens contain no node addresses or process-local iterator IDs. Validate every binding and field before querying. Changed schema/aliases or mismatched queries invalidate a token with a clear error rather than silently restarting pagination. Re-resolve owners on retries while retaining logical cursor identity.

Each partition resumes strictly after the last global key. Returned pages must respect requested bounds. Never return a successful partial page, count or group aggregate when a required partition fails. Counts and group counts aggregate exact per-partition results; expose no approximate count as exact.

Pagination is live keyset pagination, consistent with the selected SQL model, not a cross-page or cross-partition snapshot. Concurrent updates may move records across a page boundary; counts and lists can observe different instants. Frozen datasets must yield every expected record exactly once even when owners move between pages. Concurrent-mutation characterization is a separate differential test, not an excuse for frozen-data omissions.

## Acceptance and falsifiers

All items are **PLANNED / UNTESTED**. Link exact commands/results to the [verification matrix](verification-matrix.md).

1. Run pinned upstream visibility suites plus a result-set differential corpus covering every type/operator, records, aliases, grouping and CHASM. Converter-output tests alone are insufficient.
2. Execute duplicate/reordered start/upsert/close/delete, lost-response, failed durability and cold-cache recovery tests. No acknowledged mutation disappears and no deleted record resurfaces incorrectly.
3. Populate one namespace across all four partitions; page through a frozen dataset while moving owners and killing old nodes. Check exact set, uniqueness, count and group totals. Exercise malformed, cross-namespace and schema-invalidated tokens.
4. Validate search administration and interrupted backfill against canonical truth, including restart with no local state.
5. Exercise unchanged pinned Temporal UI list/filter/count/detail/history and existing SDK visibility APIs against Xenon. Pin the UI before recording a pass.

Any semantic mismatch, namespace leak, premature index publication, stale resurrection, incorrect token acceptance, successful partial aggregation or failure to resume after faults falsifies acceptance. Fix the implementation or refine an explicitly documented policy through the decision process; do not relabel failing required features as unsupported.

## Primary sources

- [Generic query converter](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/visibility/store/query/converter.go).
- [SQL visibility store](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/visibility/store/sql/visibility_store.go).
- [PostgreSQL query converter](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/sql/sqlplugin/postgresql/query_converter.go) and [visibility schema](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/schema/postgresql/v12/visibility/schema.sql).
- [Visibility persistence suites](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/tests/visibility_persistence_suite.go).

## Delegated deletion decision (issue #61)

The coordinator adjudicated unconditional terminal deletion after an independent
priority advocate challenge. DELETE creates a retained per-(namespace,run)
tombstone even when its TaskID is zero or the run has not yet been inserted.
START, CLOSE and UPSERT cannot resurrect that run, including tasks with a newer
TaskID. This intentionally differs from pinned SQL's ability to reinsert a
previously deleted run for repair/reindex. No tombstone expiry is safe without a
proven replay horizon. This is a delegated agent decision, not a claim that Calum
personally selected this policy. Delete-before-start, newer delayed updates,
duplicate deletion and reopen/replay are mandatory regression cases.
