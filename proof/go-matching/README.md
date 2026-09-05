# Legacy matching queues and tasks

Run `python3 scripts/prove.py go-matching` from a clean checkout. The shared runner pins the Go/native SlateDB build, records source/config/tool/binary hashes and checks exact automated assertions. Tests use actual Go adapter→gRPC→Go owner→SlateDB memory object store. Fault schedule and input IDs/blobs/limits are committed in case.json.

Implemented: Create/Get/Update/List/DeleteTaskQueue and Create/Get/CompleteTasksLessThan. Every task subqueue is colocated; CreateTasks prevalidates duplicates and SubqueueZero range before any write. The pinned SQL v1 store ignores create TaskPass and expiry fields; nonzero read/complete pass is rejected. Queue/task pages are opaque key continuations and capped at 3MiB; exhausted pages return nil continuation. Max blob1MiB, command1800KiB, page limit1000 and positive int32 completion limits.

The namespace user-data/build-ID methods and full TaskStore assertion are covered by [the issue40 extension](USERDATA.md). This manifest retains the narrower issue39 slice, not full Temporal boot, distributed listing, FairTaskStore, or S3 durability proof.

Source semantics: pinned Temporal common/persistence/persistence_interface.go, sql/task_v1.go, sql/task_queues.go, and sql/task_user_data.go. Multi-subqueue inserts and range condition share one transaction; namespace user-data batches form a separate namespace-wide transaction domain. No request JSON conversion is used.
