# Requirement verification matrix

Native persistence component suites, S3-emulator crash/ownership proofs and fixed history partition routing have passed at the source checkpoints below; one complete after-AwaitDurable crash-and-cold-restart smoke has passed, while full shipping acceptance remains incomplete. Decision/research closure is not implementation evidence. Partial evidence does not discharge the remaining tests.

| ID | Required behavior / spec | Owner / prerequisite | Code/PR | Acceptance command / evidence | Result |
|---|---|---|---|---|---|
| S3-1 | S3-only durable execution, visibility, ownership | Coordinator / visibility/runtime | PR55/66; #61 | Component native and MinIO proofs; full runtime/real S3 pending | PARTIAL: execution and ownership components; full visibility runtime acceptance pending |
| DUR-1 | Atomic durable acknowledgement | Persistence integration | PR55/66 | `go-runtime-stores` at2535ab1; clean55 commands passed (`20260905T205715Z-go-runtime-stores-2c8effd2`) | PARTIAL: typed native components; end-to-end workload pending |
| DUR-2 | No volatile externally actionable reads | Owner admission | PR59/66 | `owner-manager` at74b13a6; read barrier/fenced replay | PARTIAL: MinIO owner lifecycle, not full workload |
| OWN-1 | Fencing, competing owners, stale-opener convergence | Ownership manager | PR59/66 | Clean MinIO owner-manager proof independently passed, integrated74b13a6 also passed | PARTIAL: explicit activation/movement implemented; live Temporal composition pending |
| RPC-1 | Typed operations, errors and retry identity | Store integration | PR55/66 | Twelve RPC families including visibility; managed coverage and owner-manager proof20d1460 passed | PARTIAL: final integrated runtime acceptance remains |
| EXE-1 | Execution/history/tasks/Continue-As-New transactions | Complete ExecutionStore | PR53/65/66 | Native workflow/history/task suites; missing-shard correctionfb877c1 independently passed six-command proof | PARTIAL: low-level implementation; complete Temporal execution acceptance pending |
| META-1 | Matching/fairness/user data/namespaces/cluster/queues/Nexus | Store integration | PR55/58/66 | Pinned upstream and rollback/replay tests; matching seek regression1b0e013 passed11-command proof | PARTIAL: components implemented; full runtime pending |
| VIS-1 | Typed visibility, list/count/group/pagination, schema, divisions | [Typed query contract](https://github.com/0x63616c/xenon/issues/13) | PR16/66/70 | Final scalar ten-command proofeb62400 and schema cap eleven-command proof39e9315 passed; frozen2,000-record proofba0e495 passed | PARTIAL: typed components and frozen completeness; movement/UI runtime acceptance pending |
| SDK-1 | Existing SDK and Omes mixed execution | Ministack #63 / corpus #69 | PR66 | Clean3076b15 runtime20260905T235225Z-xenon-ministack-e1a79483dbf2 verified SDK retry/child/timers/signal/update/CAN and Omes20 through crash and cold recovery | PARTIAL: smoke passed; mixed and original fuzz attempts failed, corrected fuzz pending |
| UI-1 | Unchanged Temporal UI list/filter/detail/history | Ministack #63 | PR66 | Pinned Playwright list/filter/detail/history before and after cold restart passed in clean3076b15 runtime20260905T235225Z-xenon-ministack-e1a79483dbf2 | PASSED for declared smoke UI coverage; full acceptance remains |
| SCALE-1 | Add node under workload; partition movement and traffic | #57/#62/#63 | PR59/66 | MinIO movement plus four-owner history routing8856074 passed separately | PARTIAL: earlier bounded Omes20 run observed nodeC local matching service; full history/visibility movement remains |
| SCALE-2 | Multiple Temporal instances | Ministack #63 | Active work | Clean3076b15 complete smoke served two instances, recovered after a Temporal process kill, and restarted both against cold S3 state | PARTIAL: full workload remains |
| FAULT-1 | Commit/ACK/lost response/cache loss/GC recovery | #18/#57/#64 | PR55/59/66 and maintenance branch | MinIO process kills/replay passed; clean maintenance proofcf57d36 passed (`20260905T203747Z-maintenance-ac7ccb81`) | PARTIAL: combined workload/maintenance acceptance pending |
| S3-2 | Repeatable real-S3 run in authorized resources | External environment | Pending | Authorized bucket/prefix and AWS profile/role requested; target not supplied | UNVERIFIED PREREQUISITE |
| PERF-1 | Latency, lag, requests, CPU/memory, recovery | Acceptance decision / #71 | PR66/72 | RPC four-command proof7b68234 passed; S3 meter three-command proofcf5aac6 independently passed; host sampler controls passed | PARTIAL: instrument components; complete workload measurements pending |
| SHIP-1 | Clean-checkout setup, CI, operations, private release | Packaging/runtime | PR55/66, #63 | Declarative component proofs and CI; repository layout and local operations guide committed; full verified quickstart pending | PARTIAL: not shipped |

Go application nodes now use the official pinned SlateDB bindings. Proof checkpoints above are exact historical evidence, not automatic passes for every later commit. CI found a matching pagination deadline regression; native bounded seeks fixed it at1b0e013, with the original upstream deadline unchanged. Current branch CI must still pass before merge.

The clean maintenance proof retained SlateDB's native900-second compaction checkpoints and observed actual WAL/manifest/obsolete-SST deletion, protected snapshot reads, fence retention and recovery of an acknowledged update below the L0 flush threshold. It uses declared accelerated GC settings on pinned MinIO; combined Temporal workload and real-S3 gates remain open.

GitHub hosted CI is currently blocked before job execution: run33991201675/check101373635097 reports an account payment or spending-limit problem. No account settings were changed and CI is not waived. Local gates continue. Twenty immutable Omes fuzz inputs and strict integrity/replay-definition checks are committed under `proof/omes-corpus`; the first original input reached its declared deadline after successful readiness; the failed receipt and a separately corrected corpus are tracked in #69.

Combined `make test-go` passed at804898e after a failed disk-exhaustion build; the failure is not reclassified. All22 Python harness regression tests passed at that integration. Full smoke retries preserve their distinct failure reports: browser setup, alias-cache readiness, daemon interruption and an initial workflow deadline have each prevented a complete pass. Component and bounded-stage successes are not combined into an invented whole-run success.

## Integration checkpoint after repository cleanup

At `473df11`, the Rust reference workspace lives under
`test/compatibility/rust/`; the original Cargo lockfile is unchanged. Its ten
workspace tests passed on the cleanup branch, and the clean registered four-command
`primitive` proof passed on the integrated source
(`20260905T231011Z-primitive-779cce4b`). `scripts/check-layout.py` verifies all
registered input paths and the moved workspace. The later640dae9 scenario consolidation is integrated; declarative files now live under `test/scenarios/ministack/` with CLI contracts preserved.

The combined execution-discovery process-cut proof passed nine commands at
`cf8c4ae` (`20260905T230256Z-process-cut-35a240d2`), including real MinIO-backed
execution UPDATE cuts, independent raw state/journal checks, replay and native
candidate rollback. Binding those controls into full Temporal fault workloads
remains open.

The earlier `ae1f663` run passed the SDK crash-recovery sequence, Omes20, node-C
matching work and UI list/filter/detail/history. Its cold SDK histories recovered,
but cold Omes visibility20 timed out. The overall receipt remains FAILED:
`20260905T222810Z-xenon-ministack-cd6027cd9cf8`. Concurrent initial visibility reads
were independently tested and integrated at `20ed571`; the later3076b15 complete cold rerun passed, as recorded below.

The first real fuzz-soak controller attempt
`20260905T230543Z-xenon-ministack-96ea9ccced55` failed during the functional Nexus
readiness workflow. No saved corpus input executed. The endpoint was created;
readiness hit its declared deadline while matching calls also failed. Missing
Nexus HTTP configuration and matching admission behavior are under investigation.
The saved twenty inputs and one-hour/two-round soak contract remain unchanged.

Temporal upgrade impact tooling and a project-local skill are integrated. Fixture
controls passed; path classification is a review inventory, not upgrade
compatibility. No new Temporal version has been adopted or verified.

Duplicate component PRs are being closed only after ancestry/patch accounting
against #66. #55 then #66 retain the merge path; `main` is still `457fad9`, not the
integration branch. CI billing and authorized real-AWS access remain external
gates. The website/docs are now explicitly in user-authorized scope (#78); Xenon
is currently private/proprietary with no open-source release commitment.


### Integrated verification checkpoint: original fuzz corpus incompatibility

At `90327c9`, clean runtime `20260905T233427Z-xenon-ministack-71ee471f700a` passed pinned Node/build/startup and real Nexus readiness on all four history shards. The first original saved fuzz input (`2026090501.proto`) then reached its unchanged 900-second timeout; the soak and runtime receipts remain **FAILED**, with successful cleanup. No corpus round completed. Read-only history diagnosis observed three signals, the upsert and fired timers, but no child/Nexus actions or workflow completion. Pinned Omes generator/Go-worker inspection identified missing signal-acceptance metadata: numbered signals carrying actions and the final return were skipped. This is not an S3 durability failure receipt. Original corpus bytes remain unchanged; a separately versioned correction is pending actual worker controls and runtime verification.

Next integration `51e5e79` adds the reviewed external recorder lifecycle, upgrade-state runner and candidate mixed history oracle. The combined Python harness passes 63 controls. Clean recorder proof `20260905T235019Z-recorder-lifecycle-e7720697` passes all six commands; mixed baseline oracle proof `20260905T234716Z-mixed-oracle-bc31b7c9` passes synthetic controls. Independent source review found that Nexus handler runs must be included separately from the 240 baseline parent/child runs; that correction remains pending, so these controls do not establish mixed runtime acceptance. The upgrade runner has ten passing controls but no actual old/target version pair has been executed. CI billing and authorized real-AWS resources remain external gates.


### Passing real workflow durability cut and cold smoke

Clean `3076b15` runtime `20260905T235225Z-xenon-ministack-e1a79483dbf2/result.json` is **PASSED**, with no cleanup errors. Recorded exact execution UPDATE cut at `after_await`, PID91370/session/incarnation/operation digest, actual SIGKILL at117.919s, and same SDK update acknowledged after fresh owner replacement at121.159s. Subsequent Temporal-instance kill, exact SDK result, two-run CAN history, Omes20, nodeC matching redistribution, UI before/after cold restart and cold visibility all passed. Both before/after cold history SHA256 pairs are identical. This closes the previously failed cold-smoke rerun at this source, not the remaining full acceptance/fuzz/mixed/other-fault/real-AWS gates.


### Current mixed and movement checkpoint

Clean `50d12aa` real mixed runtime `20260906T000500Z-xenon-ministack-7a6ec46a1016/result.json` failed during the frozen workload, before the history audit. Bounded public API diagnostics were retained before teardown. The workload and acceptance deadlines are unchanged; cause investigation remains open. The reviewed Nexus-aware oracle is integrated, but its component controls do not establish workload success.

At `f660fae`, visibility movement is integrated: all page-size1/7/100 cursors pause before one actual history/visibility ownership move and then verify the frozen2,000-record set. Clean registered barrier race proof `20260906T004312Z-visibility-movement-42cd9ef1/result.json` passed. The real movement scenario remains unexecuted at this checkpoint.
