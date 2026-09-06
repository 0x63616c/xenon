# Native admission latency experiment

This opt-in experiment drives the production native `writer` and explicit
`BeginOperation → Begin → Put → Commit → AwaitDurable → Release` path. Each cell
runs 48 distinct small writes through one writer with 1, 4 or 16 workers; a
bounded durable read verifies all values afterward. Phase samples are seconds.
Timings are observations, not portable pass/fail thresholds. The test fails on
write, durability, verification or close errors and runs with Go's race detector.

The production `Engine.Open` uses builder defaults. At pinned native source
`3fb9e8abab0c9f5833f0c154140ceef009fea02a`, SlateDB settings default WAL flush to
100 ms. Native `Commit` dispatches a worker that waits on the write handle's
`AwaitDurable` before marking the operation transaction finished. An explicit
operation releases writer admission only after this completion and `Release`.
Execution persistence retains that same operation over multiple history/root
transactions. The experiment intentionally uses only one transaction per
operation, excluding RPC, replay encoding, Temporal scheduling and routing.

Reproduce from a clean checkout containing this experiment:

```sh
python3 scripts/build-go-node.py
python3 scripts/native-admission-latency.py \
  --native-dir "$PWD/.local/slatedb-native-target/debug" \
  --output /tmp/xenon-native-latency-new-run
```

Docker must have the exact image in `case.json`; the script starts a separately
named disposable MinIO on port 19026 and removes its container/volumes afterward.
The native fixture creates the bucket. It uses the same native default settings,
MinIO image and local credentials as the agent smoke fixture. No production
settings change. The 10 ms cell is an experimental falsifier: if cadence is the
limiting factor here, reducing only this setting should materially reduce
AwaitDurable and admission wait. This is local MinIO, not AWS latency/cost proof.

## Observed run

Clean Xenon source `6aabaeb` on macOS arm64, Go 1.27.1, two native runtime threads.
The receipt pins the full revision, input hashes and loaded native library hash.
All 288 writes and durable value checks passed; all six race-enabled cells
completed, and container removal returned zero. Saved raw samples and unchanged
receipt are in `evidence/6aabaeb`. All six command log hashes were independently
checked against local evidence before preservation. The first two setup attempts
failed before measurements (duplicate fixture bucket creation; incorrect case
relative path); both were corrected and separately cleaned before this run.

| Flush | Workers | Ops/s | Admission median ms | Begin median ms | Commit median ms | Durable median ms | Total median ms |
| --- | --- | --- | --- | --- | --- | --- | --- |
| default | 1 | 9.80 | 0.00 | 0.15 | 0.37 | 100.89 | 101.32 |
| default | 4 | 9.81 | 305.22 | 0.13 | 0.36 | 101.51 | 406.16 |
| default | 16 | 9.80 | 1527.72 | 0.16 | 0.41 | 102.20 | 1629.89 |
| 10ms | 1 | 81.10 | 0.00 | 0.13 | 0.37 | 11.06 | 11.81 |
| 10ms | 4 | 84.36 | 35.43 | 0.06 | 0.18 | 11.26 | 47.19 |
| 10ms | 16 | 85.69 | 173.89 | 0.14 | 0.41 | 10.89 | 185.75 |

Default throughput stays near one operation per flush. At 16 workers, the queue
alone exceeds one second while native Commit remains below one millisecond.
Reducing only the test flush interval materially reduces both durability and
admission latency. This supports a cadence-plus-serialization bottleneck in this
minimal model; it does not prove the cause of mixed40's actual activity timeout.
A full-workload falsification must preserve the original heartbeat/profile
limits and durability ordering while measuring its actual request queue and
write cadence. No group commit or production configuration decision follows
from this experiment alone; faster flushes can increase S3 requests and cost.
