# Requirement verification matrix

Bounded primitive tests have passed; no complete shipping requirement is passed yet. Decision/research closure is not implementation evidence. Partial evidence does not discharge the remaining tests.

| ID | Required behavior / spec | Owner / prerequisite | Code/PR | Acceptance command / evidence | Result |
|---|---|---|---|---|---|
| S3-1 | S3-only durable execution, visibility, ownership; product §Storage | Coordinator / engine decision | Pending | Real-S3 cold-recovery harness pending | UNIMPLEMENTED |
| DUR-1 | Atomic durable acknowledgement; product §Correctness | [Primitive validation](https://github.com/0x63616c/xenon/issues/10) | PR 11 merged | `cargo test --locked -p slatedb-probe` / evidence/primitive | PARTIAL: engine primitives only |
| DUR-2 | No volatile externally actionable reads | [Primitive validation](https://github.com/0x63616c/xenon/issues/10) | Pending | Delayed flush/read interleaving | PARTIAL: engine primitives only |
| OWN-1 | Fencing, competing owners, stale-opener convergence | [Ownership experiment](https://github.com/0x63616c/xenon/issues/12) | Pending | Deterministic pause/open/fence/recovery tests | UNIMPLEMENTED |
| RPC-1 | Full typed request/result/error and retry identity | RPC contract after engine gate | Pending | Wire round trips and real shard RPC | UNIMPLEMENTED |
| EXE-1 | Execution/history/tasks/Continue-As-New transactions | Execution implementation | Pending | Pinned upstream persistence suites | UNIMPLEMENTED |
| META-1 | Matching, fair subqueues, user data, namespaces, cluster, queues, Nexus | Metadata implementation | Pending | Upstream suites plus atomic rollback tests | UNIMPLEMENTED |
| VIS-1 | Typed visibility, list/count/group/pagination, schema, divisions | [Typed query contract](https://github.com/0x63616c/xenon/issues/13) | Pending | Differential and upstream suites | UNIMPLEMENTED |
| SDK-1 | Existing SDK and Omes mixed execution | Acceptance decision | Pending | Pinned Omes scenario manifests | UNIMPLEMENTED |
| UI-1 | Unchanged Temporal UI list/filter/detail/history | Acceptance decision | Pending | Browser exercises with asserted results | UNIMPLEMENTED |
| SCALE-1 | Add Xenon node under load; real partition movement and traffic | Scale-out implementation | Pending | Live workload plus assignment/traffic evidence | UNIMPLEMENTED |
| SCALE-2 | Multiple Temporal instances | Harness implementation | Pending | Multi-instance workflow/failure run | UNIMPLEMENTED |
| FAULT-1 | Commit/ACK/lost response/cache loss/GC recovery | Robustness implementation | Pending | Replayable fault seeds and outcome oracle | UNIMPLEMENTED |
| S3-2 | Repeatable real-S3 run in authorized resources | External environment | Pending | Credential/resource availability not yet assessed | UNVERIFIED PREREQUISITE |
| PERF-1 | Latency, lag, requests, CPU/memory, recovery | Acceptance decision | Pending | Concrete targets and measurement harness pending | UNIMPLEMENTED |
| SHIP-1 | Clean-checkout setup, CI, operations, private integrated release | Packaging implementation | Pending | Independent quickstart plus review and green CI | UNIMPLEMENTED |
