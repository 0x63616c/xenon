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

The temporary `internal/replay` package contains only a forwarding function and
type alias. Its remaining Go importers are exactly
`internal/simulation/replay_test.go` and `internal/simulation/coordination_test.go`.
They are left untouched during concurrent simulation work. Their next mechanical
migration can replace `replay.Run`/`replay.Effects` with
`persistence.RunReplay`/`persistence.ReplayEffects`, update the historical README
and remove the shim. Active experiment input manifests include the relocated
implementation; historical evidence retains its original source hashes. The
native composition runner already hashes all of `internal/persistence`.

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
