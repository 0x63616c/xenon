# Service layout migration checkpoint

This bounded #117 batch moves the sole durable replay implementation to
`internal/persistence/replay.go`. All live persistence services and the retiring
native owner's journal call it directly. Stored keys, protobuf bytes, replay
barrier, capacity ordering and transaction durability callbacks are unchanged.

`internal/persistence/outcome_usage.go` now owns both bounded accounting updates
and interpretation of aggregate accounting. The old owner delegates that logic
and keeps its existing native admission, transaction, nonempty barrier and
AwaitDurable path. Compatibility type aliases preserve its JSON/API consumers.
The service package has no dependency on the retiring replay package or native
SlateDB bindings.

The forwarding-only `internal/replay` package has now been removed after
verifying that it had no production callers. Its last two importers,
`internal/simulation/replay_test.go` and `internal/simulation/coordination_test.go`,
call `persistence.RunReplay` and use `persistence.ReplayEffects` directly. The
fixed schedules, independent assertions and expected traces are unchanged.
Active experiment inputs and the native composition runner reference the sole
implementation in `internal/persistence`; historical evidence retains its
original source hashes.

This is not completion of the required repository layout. Remaining concrete
moves include `internal/adapter` and `internal/temporalstore` into
`internal/temporal/adapter`, `internal/temporalruntime` into `internal/temporal`,
agent/storage assembly into `internal/app`, protobuf source/generated bindings
into `api/xenon/v1`, and retirement or relocation of the old native node and
ownership compatibility runtime with its native tests. The node operation-family
files already delegate semantics to persistence; they still bind the historical
native Owner/RPC API and are not safe to delete independently of its callers and
compatibility proofs. No empty destination packages were created.

Validation uses colocated replay/accounting regressions, the full persistence
race suite, existing native owner outcome capacity/legacy and journal/family
collision tests, and `scripts/check-layout.py` manifest checks. The checker still
checks input integrity, not completion of every required package move. No new
real-S3, capacity or complete service-layout acceptance is claimed by this batch.

The shim-removal follow-up runs the replay and coupled lost-response/crash/move
regressions plus independent non-atomic-outcome and stale-owner negative controls
under the race detector:

```sh
go test -race ./internal/persistence ./internal/simulation -run 'Replay|DeterministicCoordinationLostResponseCrashMove|CheckerRejectsNonAtomicOutcome|CheckerRejectsStaleOwnerAcknowledgement' -count=1
python3 scripts/check-layout.py
```

Use the pinned native loader environment when linking simulation's historical
owner dependencies; these focused tests open no native engine or external service.
