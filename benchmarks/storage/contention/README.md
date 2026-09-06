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
transition and counter widths can change it. The current probe adds 101 bounded last-attempt receipts, one per actor
(including the coordinator). Advisory heartbeat traffic is excluded.

Each contender increments its own partition readiness generation three times.
The coordinator increments renewal sequence ten times, sleeping 20 ms after each
successful renewal before its next attempt. Every participant reads the initial
record before a common first-CAS barrier, intentionally inducing a burst. Later
operations run closed-loop, with one in-flight CAS per participant. Conflicts
reread the latest state and retry after a recorded formula's 1–20 ms real-time
stagger. Replacement proposals receive fresh transition IDs because both their
expected version and body changed. Unexpected errors cancel the case immediately. Ambiguous outcomes are reconciled
using the bounded actor receipt protocol below; they are never blindly classified
as conflicts.

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
no embedded Temporal. Actual cluster control cadence, batching, actor incarnation/retirement, receipt
retention and renewal policy still require production-controller testing and
coupled DST. The probe validates its specific receipt-preservation assumptions.

## Historical envelope-only failures

The three original envelope-only runs stopped at their first unexpected outcome and have
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

These original versions had no retained per-actor historical transition receipt.
The adapter can reconcile the latest envelope but cannot determine whether an
unknown older mutation published before another actor replaced that envelope.
The production reservation/readiness protocol must retain a bounded actor-owned
receipt while that actor has a pending attempt, atomically with the affected
fields. Matching transition and intent digest can then establish publication
across unrelated updates; absence alone must not be guessed as nonpublication.
The following added experiment tests a bounded receipt protocol. The historical
results do not qualify it, and must not be used to choose coordinator
renewal/suspicion defaults. Batch owner updates and bound outstanding control CAS
work before repeating with the actual controller.

The original two captures predate runner cleanup/provenance hardening. Both
completed teardown successfully, but do not claim their runner had the later
always-attempt-cleanup or sanitized-environment guarantees. The third capture
records an empty source status, pinned Go environment, unchanged input hashes
and successful scoped cleanup. All evidence includes source/input hashes and
actual tool/image versions. Binaries are omitted; their hashes are retained.

## Receipt-aware pooled-transport result

[Raw evidence](evidence/78399da-receipts/summary.jsonl) binds clean source `78399da`,
the normal keepalive-enabled profile, 101 bounded receipts, and successful scoped
cleanup. All six cases completed with exact final whole-record state and every
requested renewal/update. Each receipt contains a transition ID and logical
intent digest; the registry envelope independently hashes the complete proposal.
The actual creation envelopes were 207,726 and 776,046 bytes.

| Partitions | Contenders | Proposals | Conflicts, including proven absence | Maximum renewal gap | Duration |
| --- | ---: | ---: | ---: | ---: | ---: |
| 256 | 1 | 14 | 1 | 65 ms | 0.410 s |
| 256 | 10 | 76 | 36 | 242 ms | 0.880 s |
| 256 | 100 | 2889 | 2579 | 3988 ms | 6.391 s |
| 1024 | 1 | 15 | 2 | 144 ms | 0.604 s |
| 1024 | 10 | 103 | 63 | 903 ms | 1.570 s |
| 1024 | 100 | 8029 | 7719 | 18775 ms | 40.225 s |

The 256/100 case resolved 23 unknown outcomes: 22 became proven nonpublications
under the preservation invariant and one completed after an exact-version retry.
The 1024/100 case observed no unknown outcomes in this run. It transferred
14,517,841,601 response-body bytes plus 2,293,592,282 declared request-body bytes.
A logical proposal can contain an exact same-attempt retry, recorded in its
`reconciliation` trace; actual HTTP counters independently report wire attempts.
No stochastic timing repeat or percentile/production-latency claim is made.

Protocol assumptions, enforced by the closed probe's mutation paths:

1. Each actor has one unresolved attempt. It cannot overwrite its own receipt
   until that attempt resolves; every other actor preserves that receipt.
2. Matching transition and intent digest proves publication even after unrelated
   whole-record updates. A digest mismatch or unexpected actor receipt is fatal.
3. Reading the exact original version permits the exact same ID/body/condition
   retry. A later retry conflict alone does not erase earlier ambiguity.
4. A later non-ABA version with the actor's exact original receipt proves this
   attempt did not publish: a committed receipt could not disappear under rules
   1–2, and the original conditional mutation can no longer publish. Only then
   does a fresh proposal/transition rebase onto the latest record.

This negative proof belongs to this protocol, not generic `registry.Store`.
[Real injected cases](evidence/78399da-receipts/receipt-cases.jsonl) force a committed
write with response loss and a noncommitted write with response loss, then publish
an unrelated actor's mutation before registry readback. Both are resolved and
checked against independent expected whole state. A negative control deliberately
drops the first actor's receipt during that intervening mutation; the whole-state
oracle detects the false nonpublication classification. The top-level `passed`
for that negative case means the broken invariant was detected, not accepted.
Focused tests also reject digest mismatch and a second actor-local overwrite.

This is bounded eventual-progress evidence, not a production election/renewal
qualification. The 18.8-second observed renewal gap specifically rules out deriving
a short suspicion period from the nominal 20-ms renewal interval. Reduce concurrent
full-record updates/batch node-owned fields and repeat with the production driver
before selecting cadence. Do not extrapolate 100 goroutines to 100 servers.
