# Legacy matching queues and tasks

Run `python3 scripts/prove.py go-matching` from a clean checkout. The shared runner pins the Go/native SlateDB build, records source/config/tool/binary hashes and checks exact automated assertions. Tests use actual Go adapter→gRPC→Go owner→SlateDB memory object store. Fault schedule and input IDs/blobs/limits are committed in case.json.

Implemented: Create/Get/Update/List/DeleteTaskQueue and Create/Get/CompleteTasksLessThan. Every task subqueue is colocated; CreateTasks prevalidates duplicates and SubqueueZero range before any write. The pinned SQL v1 store ignores create TaskPass and expiry fields; nonzero read/complete pass is rejected. Queue/task pages are opaque key continuations and capped at 3MiB; callers may receive a final empty page. Max blob1MiB, command1800KiB, page/completion limit1000.

The five namespace user-data/build-ID methods are required work in issue40. MatchingStore deliberately does not assert full TaskStore satisfaction yet; there are no silent stubs. This is the issue39 slice, not full Temporal boot, distributed listing, FairTaskStore, or S3 durability proof.

Source semantics: pinned Temporal common/persistence/persistence_interface.go, sql/task_v1.go, sql/task_queues.go, and sql/task_user_data.go. Multi-subqueue inserts and range condition share one transaction; namespace user-data batches form a separate namespace-wide transaction domain. No request JSON conversion is used.
