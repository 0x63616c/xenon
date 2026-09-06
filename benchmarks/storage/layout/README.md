# Bounded physical database grouping measurement (#116)

Run from a clean checkout:

```sh
python3 benchmarks/storage/layout/run.py
```

Requires the pinned Go 1.27.1/Rust 1.94.0 native build, Docker 29.4.0 and Compose
5.1.2. The runner reuses `scripts/build-go-node.py`, the native contract fixture's
digest-pinned MinIO Compose definition, the production `partitions/slatedb.Engine`
and existing `s3meter.Proxy`. Missing native artifacts are rebuilt. No production
configuration, default partition count or placement policy is changed.

`case.json` fixes 16 logical shards, 16 sequential operations per logical shard,
256-byte state values and an independently persisted outcome per operation. There
are 16 concurrent logical workers in every case. Each operation reads/validates
its shard's previous state, atomically writes next state and its outcome in one
native transaction, commits and waits for **that mutation's** durable receipt.
Logical shard `s` maps to physical writer `s % physical_writers`. No transaction
crosses a shard/database boundary. This is a synthetic atomic state/outcome domain,
not a Temporal workflow/history/matching/visibility compatibility workload.

Physical layouts run as 1, 4, 16 then 16, 4, 1 writers. Every case has a fresh child
process and fresh bucket; cases share the same local MinIO process. This reverses
order once but is only two observations per layout, not statistical qualification.
Each child owns its Go/native writer handles and a localhost metering proxy. The
native build is the existing pinned **debug** library; the measurement binary has
no race instrumentation. Go parallelism is fixed at GOMAXPROCS=4 and the pinned
native runtime uses two threads. Do not interpret its latency as release-build performance.

Metrics and boundaries:

- Baseline RSS samples follow SDK bucket creation and precede native Open. Idle
  samples cover two seconds after all writers open. Loaded samples cover the fixed
  256-operation workload; recovered idle covers two seconds after close/reopen.
  Parent `ps` samples child RSS/CPU time every 100 ms. RSS includes Go, native code,
  allocator retention, mapped library pages and the in-process proxy. It excludes
  the MinIO container, Docker VM, parent and whole Temporal server. Idle median and
  loaded peak deltas subtract that same child's baseline median. Peaks shorter
  than the sampling interval may be missed. CPU is coarse cumulative process time.
  Raw Go heap/sys counters are also emitted and do not measure Rust allocation.
- End-to-end latency spans Begin admission, conditional read, staging, Commit and
  AwaitDurable. Own-write durable latency starts immediately before Commit and
  ends after AwaitDurable; it excludes time queued for the writer. Raw per-operation
  nanoseconds are retained. Percentiles use nearest rank over 256 operations per
  case; p99 is a small-sample tail observation, not an SLO.
- Throughput is acknowledged operations divided by loaded wall time. There is no
  explicit offered-rate cap; every logical worker waits for its own previous
  operation. Grouped writers serialize through the production admission gate.
- Meter snapshots bracket open/idle/load/takeover/reopen/recovered-idle phases.
  Differences include native maintenance/background requests and retries, plus
  authority barriers; request/response bytes are proxy body bytes, excluding HTTP
  headers/TLS and S3 billing. Snapshots retain inflight counts; requests spanning
  boundaries can straddle counts/bytes. This two-second idle window is too short
  to estimate long-run compaction/maintenance costs.
- Takeover opens a replacement native writer while its old handle exists, closes
  the old handle, then reads/verifies all acknowledged states/outcomes for that
  physical database through ReadDurable. Reported total/per-writer recovery times
  include verification; separate per-writer native Open timings exclude it.
  Reopen closes all writers, then opens/verifies them sequentially. These are
  quiescent native recovery times, excluding registry election, routing, controller
  convergence, crashes, network partitions and real remote-S3 latency.

Every case has a 120-second caller budget and a 125-second supervisor guard. RSS
above 1536 MiB stops the child and remaining cases; partial logs/samples and the
resource-bound failure remain evidence. The runner owns child process groups and
its uniquely named Compose project, always kills/reaps an unfinished child and
removes only that project's containers/network/volume. `python3 -m unittest
  discover -s benchmarks/storage/layout -p test_run.py` verifies actual child
  termination at the RSS guard and rejection of unsuccessful child completion. Cleanup failure fails the
run without replacing the primary error. External SIGKILL of the supervisor itself
cannot execute Python cleanup; its recorded project ID identifies recovery scope.

Raw JSONL events, per-process samples and summarized results remain under
`.local/evidence/xenon-layout-*/`. Provenance includes exact source, dirty patch
hash for development runs, source/config hashes, tool/native versions and binary
hashes. Clean runs reject checkout/input changes during measurement. Local test
credentials are fixed in the reused Compose fixture; no AWS credentials or spend.

These bounded 16-logical-shard cases do not establish a database-count default,
100-server capacity, support for 256/1024 physical writers, production resource
limits, real AWS qualification or #116 completion. Placement/control-record and
transaction-domain decisions require their separate evidence and review.
