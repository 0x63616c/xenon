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

The upstream Temporal embedding boundary now lives in
`internal/temporal/service.go`, with `service_test.go` and the byte-identical
embedded `default.json` beside it. Both production consumers (`internal/app` and
`cmd/xenon` configuration validation) import the new package directly. The old
`internal/temporalruntime` package has no forwarding shim. Runtime construction,
startup, readiness, shutdown and generated settings are unchanged. The adapter/factory migration is recorded below. Agent profile receipts hash every
tracked source and asset, so they automatically include these new paths; no active
manifest enumerated the former runtime paths. Historical receipt hashes are
unchanged. The Temporal upgrade impact inventory now includes this boundary.

The 35 files formerly in `internal/adapter` and three in `internal/temporalstore`
now share `internal/temporal/adapter`, with their original filenames and colocated
tests. Factory constructors call the same adapter constructors directly after
removing the former inter-package qualification; exported APIs and behavior are
unchanged. Production storage, Temporal embedding, probe commands and native
compatibility tests import the merged package directly. There are no forwarding
packages. Relative fixture and fallback-binary paths in the moved tests account
for the extra directory level. Proof runners, expected Go test package names and
active manifests use the new paths; historical evidence is untouched.

This is not completion of the required repository layout. Remaining concrete
moves include agent/storage assembly into `internal/app`, protobuf source/generated bindings
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

The Temporal embedding relocation runs the existing configuration/index-consistency,
app layout, and CLI suites with `go test -race ./internal/temporal ./internal/app
./cmd/xenon -count=1` under the same native loader environment. The embedded
configuration and test bodies are byte-identical after the package-name change;
service code differs only in package and upstream import naming. Manifest checks
and the Temporal upgrade inventory test cover the operational references. This
component validation does not rerun an embedded Temporal cluster.

Adapter/factory validation uses the full `go test -race
./internal/temporal/adapter -count=1` suite with the pinned Go compatibility node
in `XENON_NODE_BINARY` and native loader environment; the declarative experiment
runner supplies those same dependencies. It includes upstream execution-task and
visibility suites and existing lost-response/replay regressions. Consumer suites
cover Temporal configuration, storage, app, CLI and SDK probe. The native
`TestGoOwnerTemporalFactory` regression checks factory-created stores, and the
legacy `cmd/xenon-temporal` still compiles under its `ministack` build tag. Proof
runner and upgrade-inventory tests verify the relocated runner package filters.
No S3 or full-stack acceptance is inferred from these component checks.
