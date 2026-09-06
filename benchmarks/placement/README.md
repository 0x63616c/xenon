# Placement and control-size spike (#116)

This is research, not adoption. Candidate modules are pinned in this nested module
and do not enter production `go.mod`. From a clean checkout with Go 1.27.1:

```sh
benchmarks/placement/run.sh /tmp/fresh-placement-evidence
```

The bounded runner records source/input hashes, runs race assertions, repeats the
complete output in a second process and compares bytes, then performs three sets
of 100-iteration Go benchmarks with `GOMAXPROCS=1`. Timings include each adapter's
canonical sorting, fresh library construction and assignment extraction. They are
local planning CPU/allocation measurements, not network/storage or 100-pod support.
No external resources are created; remove only the selected evidence directory
when it is no longer needed. There is no production setting change.

`scenarios.json` fixes slot counts (64/256/1024/4096), membership IDs, ordered churn
steps with joins/departures of 1/10/100, repeated restoration of the same membership,
and three workload-weight profiles. Node IDs are `node-%06d`, slots
`partition-%06d`, beginning at zero. Fresh canonical membership reconstruction is
the pure decision boundary. The bound is partition **count**, not workload weight.
All candidates use SHA-256 truncated to 64 bits where hashing is required.
`buraksezer/consistent` uses 20 replicas/load 1.25 and `GetPartitionOwner(i)` for
fixed slot i; hashing logical slots again through `LocateKey` would test a different
mapping and invalidate the count-load comparison. No populated slot is rehashed
by changing the count. Round robin mirrors `internal/ownership/join.go:rebalance`;
its benchmark adapter is not the production function itself.

Pins: [buraksezer/consistent v1.1.0](https://github.com/buraksezer/consistent/tree/v1.1.0)
and [dgryski/go-rendezvous 9f7001d12a5f](https://github.com/dgryski/go-rendezvous/tree/9f7001d12a5f).
The latter's mutable `Remove` indexes past the slice end; the executable probe
records the panic. The measured fresh-construction path handles departures without
calling that broken API. This does not qualify adopting mutable rendezvous state.

`../storage/control_record_test.go` is an explicitly **synthetic** compact-JSON
shape for the documented coordinator owner/incarnation/generation/renewal,
assignment revision, desired owner, reservation transition/revision/generation and
ready owner/revision/generation, plus stable data prefixes. All per-partition
fields are populated; owners reference 100 synthetic instances. Advisory heartbeat
records are separate; retained historical receipts are excluded. The actual
registry `NewWrite`/`Encode`/`Decode` codec measures envelope/base64 cost. A 1 MiB
record budget is an experiment input, not an adopted production default. No final
control type exists yet, so serialization measurements are conditional on this
shape and the specified seven-digit counter fixture.

Remaining #116 work includes native database grouping, real registry CAS
contention/renewal traffic and embedded Temporal resource cost. Placement alone
cannot choose writer count, movement concurrency or hot-partition policy.
