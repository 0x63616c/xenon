# Verification status

**Xenon is an experimental backend, not a production-ready service.** This page records the integrated candidate `ae1f663` and its September 5, 2026 smoke result. It is a checkpoint, not a live dashboard.

## The latest integrated smoke

The run `20260905T222810Z-xenon-ministack-cd6027cd9cf8` finished with `proof_pass: false`.

| Gate | Observed result |
| --- | --- |
| Two Temporal instances serving | Passed |
| SDK activities, retry, child, timers, signal, update and Continue-As-New | Exact result and two-run history checks passed |
| Active Xenon owner killed and replaced | Passed in this scenario |
| Temporal instance killed with an existing SDK client | Recovered within the declared observation budget |
| Twenty Omes simple workflows | Completed before cold restart |
| Node C added and matching moved | Local successful operations observed on C |
| Unchanged UI list, filter, detail and event history | Passed before cold restart |
| Fresh local node directories; cluster metadata recovered through stable ingress | Passed |
| Both cold Temporal instances and exact SDK history recovery | Passed |
| Single SDK workflow visibility after cold restart | Passed |
| Twenty Omes visibility records after cold restart | **Failed: repeated probe deadlines** |
| UI after cold restart | Not reached |
| Scoped cleanup | Completed |

The cold Omes visibility probe was terminated at its 20-second command limit on five attempts before the outer progress deadline failed. The record does not establish missing data or a specific query bug; that cause remains under investigation. Earlier successful stages do not turn the overall result into a pass.

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

Real AWS S3 validation still needs an authorized target and external credentials. MinIO success cannot substitute for that gate. Hosted CI has been blocked by the repository account's billing/spending-limit annotation; local checks do not waive required hosted checks.

There is no published performance benchmark, production availability commitment, automatic rebalancer, public source release or available Xenon Cloud service.
