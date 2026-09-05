# ADR 0001: Direct S3 persistence with SlateDB

Date: 2026-09-05. Status: **engine/layout selected on bounded primitive evidence; full correctness and ownership gates pending**. This ADR supports the coordinator's #5 resolution; it does not itself mutate the tracker, close #6, or claim a working Temporal backend.

## Context and authority

Calum explicitly reiterated direct S3 storage: “nope i want s3” and “unless thats physically impossible”. No evidence demonstrates impossibility. The earlier disposable SQLite snapshot exploration is excluded from the implementation direction. Durable execution, visibility and ownership metadata must reside in object storage; local disks remain disposable.

This is a **delegated agent decision under Calum's autonomous-delivery authorization**. The independent user-priority advocate and adversarial systems reviewer agreed on the recommendation below. Their agreement is a design review, not correctness evidence, and does not imply Calum personally chose the engine internals or implementation language.

## Original recommendation (language superseded below)

Use pinned SlateDB v0.16.0 directly on S3. Retain pinned Temporal v1.31.2 with a Go persistence adapter issuing complete operation RPCs to Rust Xenon nodes embedding SlateDB. Keep operation semantics, admission gates, engine ownership and lifecycle in the owning Rust process. Do not expose remote transactions to Temporal clients.

Place complete history shards in stable execution partitions. Initially colocate matching and control domains in one movable partition, preserving all subqueues and namespace-wide transactions; disclose its throughput ceiling. Visibility uses separate logical partitions with its precise query and placement decision still pending. See the [technical specification](../design/technical.md).

## Alternatives and objections

| Option | Assessment |
|---|---|
| Rust nodes embedding SlateDB, Go Temporal adapter | Preferred validation path. One owning lifecycle and one complete-operation RPC boundary; requires explicit translation and testing of Temporal persistence semantics. |
| Go handlers plus colocated Rust SlateDB gRPC engine | Deferred. It does not eliminate SQL-to-KV semantic implementation and adds handler/sidecar failure, orphaned gates and transaction-session recovery. Reconsider only if a bounded implementation proves an actual reduction without weakening atomicity. |
| SQLite snapshot objects | Rejected by current user steering; exploratory findings are not implementation authorization. |
| Additional durable database/control plane | Violates S3-only durability requirement. |

The reviewer challenged SlateDB's delayed stale-opener race: directory reservation and engine epoch are different authorities. Rechecking a reservation after opening prevents stale readiness but cannot prevent fencing a legitimate owner. The proposed response is one open attempt per unique reservation/incarnation, terminal retirement of fenced handles, fresh conditional reservations for recovery, and a stable desired owner after faults stop. It promises neither zero disruption nor unconditional liveness in an asynchronous failure model.

The advocate favors serialized per-partition admission through durable acknowledgement because correctness outweighs throughput. Reads cannot observe pending volatile state. This deliberately limits each partition's concurrency while retaining independent multi-node ownership and scaling.

## Falsifiers and stage gate

The initial engine-choice gate has bounded executed evidence: `cargo test --locked -p slatedb-probe -- --nocapture` passed four SlateDB tests for atomic batch/conflict/reopen, write skew, durable-wait/read admission and competing-writer fencing ([output](../evidence/primitive/tests.txt)); `./scripts/probe-local.sh` passed S3-emulator durability/reopen smoke ([output](../evidence/primitive/emulator.txt)). The reviewer independently reran the primitive suite. This does not establish complete engine correctness, process-crash recovery or real-S3 behavior.

Before accepting full durability, extend these tests to process kills and loss of all local state. Before accepting ownership, release two superseded delayed openers sequentially after readiness and prove acknowledged data survives, stale handles retire, and final progress stabilizes after faults stop. Repeat with normal GC/compaction actually observed; keep fence GC `dry_run` locked to the safe pinned setting. Other maintenance remains enabled, but its execution has not yet been established. Verify that a single open cannot repeatedly acquire new epochs after fencing.

Wire conformance must preserve task categories, protobuf payloads, typed errors and mutable output flags; lost responses must recover the original durable outcome. A build or happy-path shard write does not satisfy these conditions. Real-S3 validation remains mandatory and cannot be replaced by an emulator result.

A failed gate triggers diagnosis, an implementation correction and a repeatable regression test. Reconsider the engine within the direct-S3 requirement if evidence shows a necessary primitive cannot be provided. Do not treat engineering difficulty, latency or unfinished coverage as proof of physical impossibility.

## Remaining risks and revisit triggers

Operation volume and query compatibility require substantial implementation. The initial control partition has a single-writer ceiling, durable-outcome retention needs safe limits/reclamation, and ownership convergence needs an explicit failure model and measured bound. Numeric workload targets and proof coverage remain decisions #7/#8. Revisit placement when measured control load prevents the accepted workload from progressing; revisit the language seam only with a concrete correctness-preserving prototype. The [verification matrix](../design/verification-matrix.md) must remain explicit about pending evidence.

## Superseding node-language decision: official Go bindings

On 2026-09-05 Calum explicitly preferred Go. Select Go application nodes embedding the same pinned SlateDB Rust engine through its official UniFFI bindings. The original comparison omitted this same-process option. The engine, placement, complete-operation RPC boundary and S3-only requirements are unchanged; the Rust application-node choice above is historical.

Independent advocate and adversarial reviewer approved the bounded feasibility experiment at `833a76ea309b6ec50e7af818fefb0d227e01b7d3` on `codex/go-bindings-probe`. The clean `python3 scripts/prove.py go-bindings` report passed four named cases: serializable conflicts, durable publication/flush/reopen, typed fencing, and bounded admission retaining a timed-out native wait. Review found and verified fixes for paused admission after quarantine and effective native-build Cargo configuration checks. Integration of this experiment and the Go node port are pending; these results do not establish a running Temporal backend.

Go reduces translation of Temporal domain behavior into another application language. It retains cgo, a pinned generated wrapper/native library ABI and platform-specific library packaging. Generated calls have no context cancellation. Every native operation must run within bounded owner admission; a timeout atomically quarantines the owner and retains active handles until native completion or process termination. A supervisor must recover permanently stuck processes through the ownership protocol. A goroutine timeout or `Destroy` is not native cancellation.

Port the shard server behind the existing operation-level RPC and reuse its typed-error, replay and fencing fixtures against the Go binary. Test stored shard/outcome compatibility with the Rust checkpoint. Preserve Rust engine experiments. Full node lifecycle, supervised recovery, S3 deployment, conformance and all shipping gates remain required. A concrete failing correctness gate triggers a fix and regression; do not override the Go preference merely because the Rust prototype already exists.
