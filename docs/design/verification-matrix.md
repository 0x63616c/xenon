# Requirement verification matrix

Bounded primitive tests have passed; no complete shipping requirement is passed yet. Decision/research closure is not implementation evidence. Partial evidence does not discharge the remaining tests.

| ID | Required behavior / spec | Owner / prerequisite | Code/PR | Acceptance command / evidence | Result |
|---|---|---|---|---|---|
| S3-1 | S3-only durable execution, visibility, ownership; product §Storage | Coordinator / engine decision | Pending | Real-S3 cold-recovery harness pending | UNIMPLEMENTED |
| DUR-1 | Atomic durable acknowledgement; product §Correctness | [Primitive validation](https://github.com/0x63616c/xenon/issues/10) | PR 11 merged | `cargo test --locked -p slatedb-probe` / evidence/primitive | PARTIAL: engine primitives only |
| DUR-2 | No volatile externally actionable reads | [Primitive validation](https://github.com/0x63616c/xenon/issues/10) | PR 11 merged | `python3 scripts/prove.py primitive`; delayed flush/read interleaving | PARTIAL: engine primitives only |
| OWN-1 | Fencing, competing owners, stale-opener convergence | [Ownership experiment](https://github.com/0x63616c/xenon/issues/12) | PR 14 merged | `python3 scripts/prove.py ownership`; deterministic pause/open/fence/recovery | PARTIAL: experiment only; production controller pending |
| RPC-1 | Full typed request/result/error and retry identity | [Shard RPC](https://github.com/0x63616c/xenon/issues/17) | PR 21 in review/CI | `python3 scripts/prove.py shard` on PR branch | PARTIAL: reviewed shard slice; full wire/factory surfaces pending |
| EXE-1 | Execution/history/tasks/Continue-As-New transactions | Execution implementation | Pending | Pinned upstream persistence suites | UNIMPLEMENTED |
| META-1 | Matching, fair subqueues, user data, namespaces, cluster, queues, Nexus | Metadata implementation | Pending | Upstream suites plus atomic rollback tests | UNIMPLEMENTED |
| VIS-1 | Typed visibility, list/count/group/pagination, schema, divisions | [Typed query contract](https://github.com/0x63616c/xenon/issues/13) | PR 16 merged | `go test ./internal/query`; evaluator/differential/upstream suites pending | PARTIAL: typed converter only |
| SDK-1 | Existing SDK and Omes mixed execution | Acceptance decision | Pending | Pinned Omes scenario manifests | UNIMPLEMENTED |
| UI-1 | Unchanged Temporal UI list/filter/detail/history | Acceptance decision | Pending | Browser exercises with asserted results | UNIMPLEMENTED |
| SCALE-1 | Add Xenon node under load; real partition movement and traffic | Scale-out implementation | Pending | Live workload plus assignment/traffic evidence | UNIMPLEMENTED |
| SCALE-2 | Multiple Temporal instances | Harness implementation | Pending | Multi-instance workflow/failure run | UNIMPLEMENTED |
| FAULT-1 | Commit/ACK/lost response/cache loss/GC recovery | [Crash proof](https://github.com/0x63616c/xenon/issues/18) | Reviewed branch; integration pending | `python3 scripts/prove.py crash` on crash branch | PARTIAL: MinIO batch SIGKILL/recovery; application and GC fault gates pending |
| S3-2 | Repeatable real-S3 run in authorized resources | External environment | Pending | Authorized bucket/prefix and AWS profile/role requested; target not supplied | UNVERIFIED PREREQUISITE |
| PERF-1 | Latency, lag, requests, CPU/memory, recovery | Acceptance decision | Pending | Targets committed in acceptance.md; measurement harness pending | UNIMPLEMENTED |
| SHIP-1 | Clean-checkout setup, CI, operations, private integrated release | Packaging implementation | PR 19 merged | `python3 scripts/prove.py primitive` and `ownership`; full quickstart pending | PARTIAL: clean primitive provenance/CI only |

Go application nodes are preferred by Calum. The official binding feasibility experiment is under review; no full node-language migration or Temporal boot is proven. PR/branch evidence above remains partial until integration and the complete row acceptance pass.
