# Go namespace persistence proof

Run `python3 scripts/prove.py go-namespace` from a clean checkout. The pinned native build, Go node and all registered tests are recreated by the runner; results bind source, inputs, native library and test names. Development mode cannot produce a proof pass.

This exercises all namespace adapter methods through actual gRPC into the Go SlateDB owner, including guarded rename, name collisions, notification versions, atomic rollback, lost-response replay, cross-family identity rejection and byte-bounded pagination. Native reopen verifies the namespace and outcome journal together, followed by a competing-writer fenced replay check. The shared journal retains existing shard wire field 2 and adds namespace field 3 without moving durable keys.

The backend is an in-memory object store for deterministic tests. This is not process-crash or real-S3 evidence, production routing, complete Temporal boot or a full upstream conformance claim. The older Rust namespace checkpoint supplied the reviewed semantic and pagination fixtures; the executable under this proof is Go.
