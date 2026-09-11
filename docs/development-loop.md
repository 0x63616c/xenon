# Development and test loop

Xenon's primary developer workflow is Go-first. Run commands from the repository
root; neither Make nor Python is part of the required interface.

```sh
# Run the current source. Relevant edits rebuild; unchanged runs reuse verified caches.
just xenon

# Fast package tests. This is the ordinary edit loop.
go test ./...

# Fast virtual-time deterministic simulation.
xenon test dst
xenon test dst --seed 42 --cases 1000

# Reproduce and reduce a saved DST failure.
xenon replay failure.json
xenon minimize failure.json

# Explicit real boundaries; Docker is used only here.
xenon test integration
```

Race detection is a separate check:

```sh
go test -race ./...
```

DST JSON receipts include elapsed wall time, seed/case count, OS/architecture,
Go version, CPU count, GOMAXPROCS and source provenance. Wall time is observational;
virtual time still controls the simulation. To retain a receipt, redirect stdout.

The separate performance gate compiles the CLI without race instrumentation,
warms each profile once, then checks the median of three complete command runs:
strictly under 10 seconds for the default run and 30 seconds for 1,000 cases.
Compilation is excluded. Ordinary correctness tests have no speed assertions.

```sh
GOMAXPROCS=2 XENON_DST_PERFORMANCE=1 \
  XENON_DST_PERFORMANCE_RECEIPT="$PWD/.local/evidence/dst-performance.json" \
  go test ./cmd/xenon -run '^TestDSTPerformance$' -count=1 -v -timeout=8m
```

CI runs this as the dedicated `dst-performance` job on Ubuntu 24.04 with Go
1.27.1 and GOMAXPROCS=2. The report retains all measured samples, command arguments,
limits, environment/source receipts and the compiled binary hash, including when
a timing limit fails. The development reference is an Apple M2 Pro running
macOS/arm64, Go 1.27.1 and GOMAXPROCS=2; each receipt records actual runtime values.

## Choose the smallest test

Use ordinary Go tests for local invariants. Use DST for ownership, routing,
retry, time and fault interleavings. DST uses virtual time and must not launch
Docker, MinIO, Temporal or native SlateDB.

The integration command contains only three bounded real-system journeys:

1. SlateDB and MinIO durability, reopen, reconciliation and writer fencing.
2. Three Xenon nodes, ownership movement, owner loss and same-address restart.
3. Temporal compatibility across owner loss and cold restart.

AWS S3 qualification and long soaks are release checks, not part of the normal
development loop.

## Failure workflow

A DST failure prints its seed and artifact path. Replay the unchanged artifact,
then minimize it. The minimizer accepts a reduction only when the invariant
fingerprint remains the same. Convert the smallest reproducer into a permanent
Go regression test.

Real integration failures retain bounded logs and identify failed cleanup. A
timeout never becomes a pass. The runner removes only resources it created.

## Migration status

Older Python proof controllers, manifests and Make targets are historical
migration inputs. Do not extend them. Remove a runner only after its unique
behavior has equivalent Go coverage. Some remain temporarily because native
tool preparation, the third Temporal journey, upgrade compatibility and release
qualification do not yet have Go parity. They are not the recommended developer
entrypoints.

Generated protobuf bindings remain committed. Temporal version changes follow
[the upgrade procedure](temporal-upgrades.md).
