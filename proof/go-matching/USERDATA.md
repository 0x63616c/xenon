# Complete legacy TaskStore user data

Run `python3 scripts/prove.py go-matching-userdata` from a clean checkout. The manifest retains the first matching slice and adds namespace-wide user-data/blob/index transactions, typed Applied/Conflicting outputs, restart/replay and byte-page assertions, plus the pinned upstream TaskQueue, TaskQueueTask and TaskQueueUserData suites. The adapter now statically satisfies legacy TaskStore. FairTaskStore is a separate interface.

Version0 creates version1; conditional updates increment once. Entire namespace batches and build-ID mappings commit together. Sorted queue updates make the first reported version conflict stable; all Applied outputs become false on batch failure, only reported Conflicting pointers become true. Duplicate build-ID additions produce Unavailable and roll back data, as pinned PostgreSQL inserts do; removing absent mappings succeeds. No JSON request roundtrip is used.

Upstream conformance exposed two first-slice gaps now corrected: completion accepts positive int32 limits (including1024), and exhausted task/queue pages return nil continuation. Tests use explicit1ms WAL flush to execute1024single-row durable reads within upstream30s deadline; production default remains100ms and AwaitDurable is retained. Upstream tests generate random UUIDs/IDs; their pinned workload is reproducible but those values differ across runs. Fixed custom inputs and schedules are committed.

Limits: request1800KiB, individual blob1MiB, namespace batch/list1000, response3MiB; full build-ID result fails ResourceExhausted above the nonpaginated budget. Version overflow rejects explicitly. Full interface coverage does not prove S3, dynamic routing/global listing, Temporal boot, or FairTaskStore.
