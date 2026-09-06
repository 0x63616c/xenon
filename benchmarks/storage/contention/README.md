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
