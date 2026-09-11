# Development and test loop

Xenon's primary developer workflow is Go-first. Run commands from the repository
root; neither Make nor Python is part of the required interface.

```sh
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
