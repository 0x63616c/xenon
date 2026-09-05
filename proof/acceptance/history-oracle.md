# Omes history audit

Build `go build -o .local/bin/xenon-omes-oracle ./cmd/xenon-omes-oracle`.
After the declared simple workload has drained, run:

```sh
.local/bin/xenon-omes-oracle --omes-run-id xenon-full-simple --exact-runs 100 --require-activity-per-run --output .local/evidence/full-simple-histories
```

The output directory must be new and its parent must exist. The controller owns
source/binary hashes, the workload execution and visibility convergence before this
command. Omes's pinned TaskQueueForRun maps run ID to `omes-<run-id>`; the oracle uses
that exact task queue to enumerate all visible runs. It compares list and count,
rejects duplicate records/cursor cycles, retrieves every history page, verifies
contiguous event IDs and completed/Continue-As-New terminal consistency, and checks
that every successor belongs to the same workflow with no missing runs or cycles.
The simple workload additionally requires a completed activity in each history.

Raw histories, including terminal result payloads, are retained with hashes. This
does not semantically validate arbitrary mixed/fuzz result payloads or prove the
visibility set contains every start the workload attempted. The workload/controller
must independently enumerate those starts and assert expected outcomes. The command
sets full_acceptance=false and does not claim to execute ownership moves or faults.
It requires completed or continued-as-new runs; intentionally failed/canceled/retried
workflow experiments need a separate explicit outcome contract before using it.

API calls have 30-second deadlines; the command has a 900-second bound, 10,000-run
bound, 1,000 pages per listing/history, 100,000 events and 16 MiB protobuf per history,
and 256 MiB total serialized evidence. Failures return nonzero and never write a
successful final report; retain partial histories and the controller's stderr log.
Unit controls exercise missing/duplicate events, terminal mismatches and malformed
run graphs via `go test -race ./cmd/xenon-omes-oracle`. Live execution remains pending.
