# History-task component proof

Run `python3 scripts/prove.py go-historytasks` from a clean committed checkout. The shared runner builds the pinned native library and Go server, records source/config/input hashes, checks exact test names, and emits machine assertions. Test inputs are committed literal task IDs, category IDs, timestamps and payload sizes in the named tests; the process test also consumes `proof/execution/case.json`.

Pinned Temporal v1.31.2 `common/persistence/sql/execution_tasks.go` defines immediate ID ranges [min,max), scheduled timestamp ranges [min,max) (initial task ID ignored), idempotent completion, and permitted BestEffort no-op. Execution issue #45 defines the shared writer key/value contract. Categories of different types remain separate even with the same category ID. Timestamps truncate to microseconds, including preepoch times.

Explicit correction: pinned SQL scheduled cursors increment the last task ID, overflowing at MaxInt64. Xenon tokens bind shard/category/type/bounds and resume strictly after the last exact key without arithmetic. Tokens are opaque and not interchangeable with SQL tokens. All pages include a continuation when nonempty; a final extra read may be empty. Count limit 1000 and complete encoded response limit 3MiB apply together. Replay returns the original durable page, not a refreshed scan. BestEffort completion is explicitly ignored as allowed by the public interface and done by pinned SQL.

This proves the bounded component against the memory object store only. Full Temporal boot, dynamic ownership, real S3 and process-crash acceptance remain required.
