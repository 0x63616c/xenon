# Real S3-registry control contention (#116)

This bounded research probe uses the production registry/S3 adapter against the
repository's pinned local MinIO image. It does not implement the coordinator,
prove election safety, simulate Xenon, or qualify 100 real instances/AWS S3.

From a clean checkout with Go 1.27.1, Python 3 and Docker Compose:

```sh
python3 benchmarks/storage/contention/run.py /tmp/fresh-contention-evidence
```

The runner builds the probe, starts an isolated Compose project on localhost
port 19316, creates its disposable bucket, runs six cases, saves raw receipts and
provenance, then removes only that project's container/volume. The evidence path
must not already exist. No credentials or cloud resources are required. The
checked-in local-only credentials match existing repository emulator recipes.

## Declared workload

`config.json` fixes 256/1024 fully populated partition records and 1/10/100 owner
contenders. The control shape is copied from placement research `e828fdb`'s
`benchmarks/storage/control_record_test.go`, whose synthetic envelope measurements
were 189,990/758,310 bytes. This probe reports its actual encoded size; ETag,
transition and counter widths can change it. Separate advisory heartbeat traffic
and historical receipt retention are excluded.

Each contender increments its own partition readiness generation three times.
The coordinator increments renewal sequence ten times, sleeping 20 ms after each
successful renewal before its next attempt. Every participant reads the initial
record before a common first-CAS barrier, intentionally inducing a burst. Later
operations run closed-loop, with one in-flight CAS per participant. Conflicts
reread the latest state and retry after a recorded formula's 1–20 ms real-time
stagger. Replacement proposals receive fresh transition IDs because both their
expected version and body changed. Unexpected errors or unknown outcomes cancel
the case immediately; they are never treated as conflicts or guessed successes.

All updates use read/decode/mutate/encode and the adapter's own preflight and CAS
path. The workload preserves unrelated fields and verifies the complete final
control record against an independently constructed expected fixture. Success
counts and all renewals must match. Every attempt records worker, update, attempt,
expected/result versions, transition/digest, outcome and monotonic start/end.
HTTP counts include setup/final verification and adapter preflight/readback.
Body bytes exclude HTTP headers; request bytes are declared Content-Length,
not packet-level accounting. The probe aborts the first failed case and preserves
its events before bounded teardown.

The maximum renewal gap includes the first renewal's wait from case start.
This is measured progress under this workload, not a lease/suspicion recommendation
or a latency SLA. Scheduling is real and nondeterministic; repeated runs retain
exact observations but need not reproduce timing or ordering. Logical CAS outcomes
are checked, not inferred from elapsed time. CPU and traffic include JSON codecs
and the single client-process runtime, so this is not an isolated S3 benchmark.

Limits: tiny update budget, equal owner activity, one MinIO server, no injected
failures, no takeover, no production field authorization, no native databases,
no embedded Temporal. The existing registry contract suite owns lost-response
semantics. Actual cluster control cadence, batching, receipt bounds and renewal
policy still require production-controller testing and coupled DST.

## Executed evidence and decision limit

All three captured runs stopped at their first unexpected outcome and have
`status=failed`. They are retained as failed experiments, not passing gates:

- [Initial baseline](evidence/fb34501-baseline/summary.jsonl), source `fb34501`:
  256 partitions/1 and 10 contenders passed; 100 contenders stopped at an unknown
  outcome. This first version did not expose its nested SDK error.
- [Diagnostic baseline](evidence/fb87d30-diagnostic/summary.jsonl), source `fb87d30`:
  256/1 passed; 256/10 stopped with PUT `http: server closed idle connection`.
- [No-keepalive diagnostic](evidence/a21fa9b-no-keepalive/summary.jsonl), source
  `a21fa9b`: five cases completed; 1024/100 stopped with PUT `EOF` and a later
  envelope that could not prove the historical outcome.

The third run used `no-keepalive.json`, differing only by explicitly disabling
HTTP connection reuse. Reproduce that transport diagnostic with:

```sh
python3 benchmarks/storage/contention/run.py /tmp/fresh-no-keepalive-evidence benchmarks/storage/contention/no-keepalive.json
```

It does not qualify a production transport policy. The default pooled transport
remains the baseline; disabling reuse did not eliminate unknown outcomes.

| Partitions | Contenders | Successful updates / requested | Conflicts | Maximum observed renewal gap | Whole final state |
| --- | ---: | ---: | ---: | ---: | --- |
| 256 | 1 | 13/13 | 5 | 116 ms | verified |
| 256 | 10 | 40/40 | 35 | 267 ms | verified |
| 256 | 100 | 310/310 | 2987 | 2884 ms | verified |
| 1024 | 1 | 13/13 | 5 | 138 ms | verified |
| 1024 | 10 | 40/40 | 79 | 892 ms | verified |
| 1024 | 100 | 50/310 | 1363 | 4098 ms before abort | unverified |

The 256/100 completed case transferred 1,543,802,523 response-body bytes and
submitted 348,778,958 declared request-body bytes in 7.77 seconds. Its 310 logical
updates induced 8119 successful GETs and 1524 PUT/412 responses. Counts include
initialization and final validation. The actual creation envelopes were 189,942
and 758,262 bytes. These observations argue against assuming cheap, frequent
full-record updates or deriving a short suspicion timeout from nominal cadence.
They do not establish that a single bounded control record is impossible.

The probe deliberately has no retained per-actor historical transition receipt.
The adapter can reconcile the latest envelope but cannot determine whether an
unknown older mutation published before another actor replaced that envelope.
The production reservation/readiness protocol must retain a bounded actor-owned
receipt while that actor has a pending attempt, atomically with the affected
fields. Matching transition and intent digest can then establish publication
across unrelated updates; absence alone must not be guessed as nonpublication.
That production protocol and a corresponding ambiguity experiment remain needed.
These results do not qualify it, and must not be used to choose coordinator
renewal/suspicion defaults. Batch owner updates and bound outstanding control CAS
work before repeating with the actual controller.

The original two captures predate runner cleanup/provenance hardening. Both
completed teardown successfully, but do not claim their runner had the later
always-attempt-cleanup or sanitized-environment guarantees. The third capture
records an empty source status, pinned Go environment, unchanged input hashes
and successful scoped cleanup. All evidence includes source/input hashes and
actual tool/image versions. Binaries are omitted; their hashes are retained.
