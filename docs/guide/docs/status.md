# Verification status

**Xenon is an experimental backend.** This checkpoint records the passing `3076b15` crash-and-recovery run and the remaining delivery gates. It is not a live dashboard or production certification.

## Passing crash-and-recovery run

Run `20260905T235225Z-xenon-ministack-e1a79483dbf2` finished with `proof_pass: true` and successful cleanup.

| Gate | Observed result |
| --- | --- |
| Two Temporal instances and two initial Xenon nodes | Serving real SDK work |
| Real workflow update cut after AwaitDurable | Exact operation, process and stage recorded; owner killed with SIGKILL |
| Fresh owner replacement | Same SDK update acknowledged after recovery |
| Temporal instance killed with an existing SDK client | Recovered within the declared budget |
| SDK activity retry, child, timers, signal, update and Continue-As-New | Exact result and two-run histories verified |
| Twenty Omes simple workflows | Completed |
| Node C added during Omes work; matching moved | Local successful operations observed on C |
| Temporal UI list, filter, detail and event history | Passed before and after cold restart |
| All local state discarded; S3 retained | Both Temporal instances restarted; recovered histories matched the original hashes |
| Cold visibility | SDK workflow and all twenty Omes records verified |

This is one declared crash scenario, not the full fault/workload matrix. Earlier failed smoke receipts remain failed; the cold visibility deadline observed at `ae1f663` did not recur in this passing run.

## Fuzz and mixed workloads

The original-corpus fuzz attempt at `90327c9` passed real Nexus readiness across all four history shards, then its first saved input reached the unchanged 900-second timeout. No complete corpus round passed. Pinned Omes generator and Go-worker inspection identified missing signal-acceptance metadata: numbered signals containing child/Nexus actions and the final return were skipped. Original input bytes and failed evidence are preserved. A separately versioned correction with explicit provenance and worker regression controls is under development.

The mixed-workload controller and history oracle are implemented but still await live execution. Its 240 baseline parent/child runs are distinct from additional Nexus handler workflows; source-derived checks must account for both. Remaining native cut stages, movement and measurement scenarios are not implied by the passing smoke.

The repository's `docs/design/verification-matrix.md` retains exact component receipts and the integration ledger.

## Component evidence

The repository contains registered proofs for native transactions and fencing, S3 ownership, Rust/Go stored-data compatibility, persistence families, visibility semantics, outcome capacity and process cuts. The integrated candidate also passed the Go race suite and the registered runtime-measurement controls.

A component proof establishes its own declared assertions. It does not establish every Temporal feature, arbitrary workloads, production performance or real AWS behavior. [Local development](./development.md) explains how to rerun a named proof and inspect its receipt.

## The larger acceptance gate

The first-boot smoke is deliberately smaller than the delivery contract. The full committed profile requires:

- 100 simple and 40 throughput scenarios with frozen options.
- 20 saved fuzz inputs in both the no-fault and declared-fault profiles.
- At least ten local successful operations on each of three nodes.
- Movement of history and visibility partitions during work.
- A frozen 2,000-record visibility oracle at page sizes 1, 7 and 100.
- Declared failure schedules, latency/recovery budgets and complete measurement evidence.

The saved corpus and workload helpers are committed. Their existence is not evidence that the full profile has executed successfully.

## External and product boundaries

Real AWS S3 validation still needs an authorized target and external credentials. MinIO success cannot substitute for that gate. Hosted CI remains blocked: checks on integration PRs #55 and #66 report that jobs did not start because of account payments or the spending limit. This was checked against the `90327c9` candidate on September 5, 2026; local checks do not waive required hosted checks. At that checkpoint both PRs were open and `main` remained at `457fad9`. The integrated code and this site therefore describe a delivery candidate, not a release already merged to main.

There is no published performance benchmark, production availability commitment, automatic rebalancer, public source release or available Xenon Cloud service.
