# Issue #119 implementation and evidence checkpoint

Checkpoint: 2026-09-10, integrated source `4840385`.
The [acceptance contract](issue-119-acceptance.md) remains authoritative.
No row below establishes complete acceptance of its criterion.

| Criterion | Integrated work | Remaining acceptance evidence/work |
| --- | --- | --- |
| CLI-01 | Help/config/start contracts, startup banner on stderr, explicit SDK diagnostic loggers, mode validation | Complete instrumented no-effect command matrix, configuration precedence and exit contracts |
| CLI-02 | Three-node dev lifecycle, authority publication barrier, preserved-state guard, scoped teardown, passive inspect, reproducible journey script; concurrent Temporal metadata initialization reconciles equivalent first writes | Clean matching journeys passed at `8aed7a2` and `82408eb`; complete boundary instrumentation and one-candidate aggregate remain |
| GEN-01 | Pinned fresh Omes generator and normalizer, expanded inputs saved before effects, independent workload/fault streams | Required three seeds × 100 cases × two generations passed (600 calls, 100 distinct payloads per seed); integrated gate receipts and complete limits/negative controls remain |
| GEN-02 | Per-case concurrent resident driver, isolated runtime state, three-case component controls, CLI concurrency limits; clean real run completed 3 cases/12 roots and observed actual peak overlap 4 in every case | Observable admission barrier, concurrency-one comparison, explicit child/activity/Nexus bounds and complete graph accounting |
| DST-01 | Production Step runner; 1,000 seeded delivery interleavings, exact replay controls, virtual 24-hour takeover | Broader fault-order coverage, instrumented zero real I/O/sleep/native-open execution, integrated proof |
| FAULT-01 | Coupled safety cuts now include ambiguous publication response, replacement during Open, assignment ABA, displaced-writer fencing and modeled registry-read failure/recovery through production Steps | Remaining reservation/Open/Ready, storage/message/crash/restart matrix and corresponding real contracts |
| ORACLE-01 | Durable replay checkers, typed failures, child-parent inventory consistency, continuation/ancestry and parent-close-policy validation | Input-derived execution graph and expected results, full Nexus census, all required mutants |
| STOP-01 | First-failure latch, phase-bound callbacks, queued member cutoff; internal cancellation/deadline-shaped errors retain their observed failure cause | Complete competing-cause matrix and admission/accounting proof under real asynchronous failure |
| CLEAN-01 | Case-owned process lifecycle, bounded cleanup, refused reuse after uncertain cleanup, exact fixture resource labels | Full remote execution census including late starts, all failure controls, 100-case resource/quota proof |
| REPLAY-01 | Shared saved-envelope checksum and provenance validation; explicit persistent legacy qualification; generator-free saved replay | Integrated passing/failing mode-qualified receipts and complete tamper/compatibility matrix |
| MIN-01 | Bounded fingerprint-preserving reducer, coupled dependency reduction, CLI and negative controls | Required actual fault reduction and three-of-three real controlled-failure reproducer |
| EVID-01 | Versioned envelopes, input/trace hashes, source/tool provenance, cleanup and primary failure records | Complete operation/fault/census accounting, finalized evidence validation and quota behavior |
| DELIVER-01 | Reproducible component scripts and reviewed component tests | Register and execute all five named gates, validate child receipts and requirement mapping from one clean candidate |

## Current execution path

1. Preserve the clean generated-workflow component result at `82408eb` in
   `test/scenarios/agent/evidence/82408eb-generated-concurrent`. Its verifier
   checks 266 referenced outputs and records nine passing journey assertions.
   It remains a component journey (`full_acceptance=false`), not the aggregate gate.
2. Complete the admission barrier, serial comparison and independent expected
   execution graph around the actual concurrent workload/census journey.
3. Extend fault capability and independent oracle coverage through existing
   production seams. Seeded delivery permutations preserve external
   linearization order; they do not cover arbitrary storage outcomes.
4. Assemble the full required gates only from assertions that actually exercise
   their scope. Missing tests, skipped tests and incomplete receipts must fail.

Issue [#131](https://github.com/0x63616c/xenon/issues/131), extracting a reusable
Go-authored DST framework, remains a deferred `backlog`/`idea`. It does not expand
this delivery or excuse any unmet #119 criterion.
