# Failed registry reads through production controllers

This batch addresses the **storage error** portion of FAULT-01 and bounded
healthy recovery through deterministic production code. `storage_read_error` is
supported only on a declared registry `read` effect; applying it to publication
or an unsupported action is rejected before the first execution event.

The adapter model leaves authoritative registry bytes/version unchanged, withholds
all record data and delivers an ordinary storage error through the existing
`cluster.ReadCompleted` or `partitions.ReadCompleted` Step path. No synthetic
NotFound is substituted: an outage cannot trigger bootstrap. Both controllers
must emit no decision from that failed read, then a separately scheduled healthy
poll/read resumes the complete ownership-movement fixture.

`TestCoupledStorageReadFailureRecovers` executes ten seeded delivery schedules for
each controller type, with ten distinct expanded event orders per type. Every
schedule is replayed once with byte-identical trace and must observe exactly one
failed read and one error completion. The normal independent checker proves
final Ready ownership, current-epoch commit and drained obsolete handles/effects.
An independent read observer rejects a deliberate failed-read observation that
changes the authoritative version, using the `registry_read` invariant. Twenty
such controls execute (one per generated schedule).

```sh
go test -race ./internal/simulation \
  -run 'TestCoupled(Storage|CoordinatorMove|LostPublication|UnsupportedFault|LostRead)' \
  -count=1 -v
```

Focused storage/unsupported-mode race tests passed in 3.610s on Go 1.27.1
Darwin/arm64; the earlier storage/golden/lost-response selection passed in 3.625s.
The gate logs actual per-controller schedule/replay/fault/control/order counts.

This proves a modeled registry-read error cut, not a real S3/native outage, write
failure, dropped transport request, wall-clock timeout, or the whole FAULT-01
matrix. Real adapters still require their corresponding contract evidence.
