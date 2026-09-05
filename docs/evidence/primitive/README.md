# Initial direct-S3 engine probe

2026-09-05. SlateDB 0.16.0; Rust 1.94.0; dependencies in Cargo.lock.

`cargo test --locked -p slatedb-probe -- --nocapture`: four actual-engine tests passed, independently rerun by the adversarial reviewer. See [output](tests.txt).

`./scripts/probe-local.sh`: awaited write and reopen passed against pinned local MinIO using the S3 API. See [output](emulator.txt). The script uses a unique prefix and retains the object-store volume. `make stop` stops the service without deleting that data. This is emulator evidence, not AWS S3 evidence.

Coverage: atomic batch/conflict rollback, serializable read-dependency conflict, observed Memory-versus-Remote publication hazard with an experimental mutex, remote WAL reopening while the original writer remains open, and stale writer fencing before durable acknowledgement.

Limitations: the mutex is a probe, not an implemented persistence admission layer. No process was killed. Delayed superseded openers, forced compaction/GC, real S3, RPC translation, upstream persistence suites, Temporal/Omes/UI, multi-node scale-out and benchmarks remain unpassed. Normal GC/compactor configuration is retained, but tiny tests do not prove maintenance ran. Fence GC remains dry-run because deleting fences is unsafe under the required arbitrary-pause model.

Review: independent source review and rerun found no remaining blockers for this narrowly scoped foundation. Missing shipping gates remain in the verification matrix and Wayfinder.
