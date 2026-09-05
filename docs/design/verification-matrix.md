# Requirement verification matrix

Native persistence component suites, S3-emulator crash/ownership proofs and fixed history partition routing have passed at the source checkpoints below; no complete end-to-end shipping requirement is passed yet. Decision/research closure is not implementation evidence. Partial evidence does not discharge the remaining tests.

| ID | Required behavior / spec | Owner / prerequisite | Code/PR | Acceptance command / evidence | Result |
|---|---|---|---|---|---|
| S3-1 | S3-only durable execution, visibility, ownership | Coordinator / visibility/runtime | PR55/66; #61 | Component native and MinIO proofs; full runtime/real S3 pending | PARTIAL: execution and ownership components; visibility acceptance pending |
| DUR-1 | Atomic durable acknowledgement | Persistence integration | PR55/66 | `go-runtime-stores` at2535ab1; clean55 commands passed (`20260905T205715Z-go-runtime-stores-2c8effd2`) | PARTIAL: typed native components; end-to-end workload pending |
| DUR-2 | No volatile externally actionable reads | Owner admission | PR59/66 | `owner-manager` at74b13a6; read barrier/fenced replay | PARTIAL: MinIO owner lifecycle, not full workload |
| OWN-1 | Fencing, competing owners, stale-opener convergence | Ownership manager | PR59/66 | Clean MinIO owner-manager proof independently passed, integrated74b13a6 also passed | PARTIAL: explicit activation/movement implemented; live Temporal composition pending |
| RPC-1 | Typed operations, errors and retry identity | Store integration | PR55/66 | Twelve RPC families including visibility; managed coverage and owner-manager proof20d1460 passed | PARTIAL: visibility and final runtime remain |
| EXE-1 | Execution/history/tasks/Continue-As-New transactions | Complete ExecutionStore | PR53/65/66 | Native workflow/history/task suites; missing-shard correctionfb877c1 independently passed six-command proof | PARTIAL: low-level implementation; complete Temporal execution acceptance pending |
| META-1 | Matching/fairness/user data/namespaces/cluster/queues/Nexus | Store integration | PR55/58/66 | Pinned upstream and rollback/replay tests; matching seek regression1b0e013 passed11-command proof | PARTIAL: components implemented; full runtime pending |
| VIS-1 | Typed visibility, list/count/group/pagination, schema, divisions | [Typed query contract](https://github.com/0x63616c/xenon/issues/13) | PR 16 merged | Clean nine-command `go-visibility` proof983222f passed; scalar refinement pending | PARTIAL: five-process upstream suite and component proof; final scalar and runtime acceptance pending |
| SDK-1 | Existing SDK and Omes mixed execution | Acceptance decision | Pending | Pinned Omes scenario manifests | UNIMPLEMENTED |
| UI-1 | Unchanged Temporal UI list/filter/detail/history | Acceptance decision | Pending | Browser exercises with asserted results | UNIMPLEMENTED |
| SCALE-1 | Add node under workload; partition movement and traffic | #57/#62/#63 | PR59/66 | MinIO movement plus four-owner history routing8856074 passed separately | PARTIAL: actual Temporal workload movement remains |
| SCALE-2 | Multiple Temporal instances | Ministack #63 | Active work | Pinned configuration checks; actual two-instance workload pending | UNPROVEN |
| FAULT-1 | Commit/ACK/lost response/cache loss/GC recovery | #18/#57/#64 | PR55/59/66 and maintenance branch | MinIO process kills/replay passed; clean maintenance proofcf57d36 passed (`20260905T203747Z-maintenance-ac7ccb81`) | PARTIAL: combined workload/maintenance acceptance pending |
| S3-2 | Repeatable real-S3 run in authorized resources | External environment | Pending | Authorized bucket/prefix and AWS profile/role requested; target not supplied | UNVERIFIED PREREQUISITE |
| PERF-1 | Latency, lag, requests, CPU/memory, recovery | Acceptance decision | Pending | Targets committed in acceptance.md; measurement harness pending | UNIMPLEMENTED |
| SHIP-1 | Clean-checkout setup, CI, operations, private release | Packaging/runtime | PR55/66, #63 | Declarative component proofs and CI; full quickstart/operations pending | PARTIAL: not shipped |

Go application nodes now use the official pinned SlateDB bindings. Proof checkpoints above are exact historical evidence, not automatic passes for every later commit. CI found a matching pagination deadline regression; native bounded seeks fixed it at1b0e013, with the original upstream deadline unchanged. Current branch CI must still pass before merge.

The clean maintenance proof retained SlateDB's native900-second compaction checkpoints and observed actual WAL/manifest/obsolete-SST deletion, protected snapshot reads, fence retention and recovery of an acknowledged update below the L0 flush threshold. It uses declared accelerated GC settings on pinned MinIO; combined Temporal workload and real-S3 gates remain open.

GitHub hosted CI is currently blocked before job execution: run33991201675/check101373635097 reports an account payment or spending-limit problem. No account settings were changed and CI is not waived. Local gates continue. Twenty immutable Omes fuzz inputs and strict integrity/replay-definition checks are committed under `proof/omes-corpus`; runtime replay is still unexecuted (#69).
