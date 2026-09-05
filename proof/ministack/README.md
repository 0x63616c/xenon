# Temporal ministack configuration checkpoint

This is committed runtime input preparation, **not a boot or shipping proof**.
The factory, routed history and visibility implementations must be integrated
before the launcher/controller can run the acceptance scenario in case.json.
Do not turn a configuration validation result into a Temporal pass.

## Exact launcher seam

Build the Xenon repository's pinned Temporal module into a small native Go command:

```go
server, err := temporal.NewServer(
    temporal.WithServerConfigFilePath(configPath),
    temporal.WithCustomDataStoreFactory(temporalstore.AbstractFactory{}),
    temporal.WithCustomVisibilityStoreFactory(temporalstore.VisibilityFactory{}),
    temporal.ForServices(temporal.DefaultServices),
)
```

Use nonblocking `server.Start()`, report startup errors, wait for SIGINT/SIGTERM,
then bound `server.Stop()` with an external process supervisor. Do not use
InterruptOn's blocking startup path as a readiness signal. Actual readiness is a
successful frontend health/namespace API check through the stable proxy. Both
instances use the same cluster configuration and the normal Temporal dynamic
membership implementation; service and membership ports differ. No SQLite,
PostgreSQL, Elasticsearch or embedded dev server is configured.

Execution factory options are address, immutable ordered historyPartitions,
matchingPartition and globalPartition. The four history partitions are fixed before
first boot; adding a Xenon node moves a partition rather than changing modulo/count.
Visibility factory options are address, index and schema_partition; its four locked
visibility partitions are vis-v1-0 through vis-v1-3. The schema uses global. Factories
are being implemented in internal/temporalstore; their compile/runtime gates are
prerequisites, not waived by these files.

## Local topology

The planned controller runs native binaries and disposable runtime directories on
the host. Compose runs pinned MinIO, HAProxy and the unchanged Temporal UI. Native
service listeners accept traffic from the local Docker bridge; Compose's published
ports bind loopback. This harness is for a trusted local development host.

HAProxy exposes one stable storage address and one stable Temporal frontend address.
TCP health checks remove killed ingress processes; existing gRPC streams reset and
clients reconnect through a surviving backend. This must be asserted with the same
SDK/client connection across a fault, not by manually replacing its target address.
The third Xenon backend starts absent, then joins with a fresh incarnation and
explicit conditional topology activation. All ten logical partitions have immutable
data prefixes; their assignment can move between nodes. The controller never infers
ownership from health checks: proxy health is transport availability only.

MinIO is the only durable service. All node/Temporal/worker local directories can be
removed between restarts, while the scoped S3 volume survives the declared recovery
phase. Final cleanup removes only that run's Compose project and resources. Evidence
must record process PIDs/incarnations, assigned partition generations, input/source
and binary hashes, exact fault-trigger events and every workflow result.

## Pins and required execution

`tools/ministack.json` pins Temporal 1.31.2, the existing Go SDK 1.41.1, Omes commit
c6978ba39aa03551ce28974117e8d7ecf983d2b3 with its own worker SDK 1.48.0, UI 2.53.3,
HAProxy 3.2.12 and MinIO by immutable OCI index digests. Omes's separate module keeps
its upstream SDK/tool dependencies; it must not upgrade Xenon's module indirectly.
The Omes command always specifies the existing server address and never enables
--embedded-server. Its fixed iteration count and successful workflow outcomes must
be independently checked; exit0 alone is insufficient.

The acceptance controller will register with scripts/prove.py only when it executes
all required assertions. SDK tests must cover activity, timer, signal, exact result
and durable history; UI tests must exercise unchanged workflow list/filter/detail;
visibility APIs must check list/count/search/pagination. Omes work continues while a
new node receives moved ownership. Kill one Temporal instance while its peers and
the existing SDK connection progress, then restart everything with empty local disks
and verify acknowledged results again. A component failure leaves boot acceptance
failed/incomplete; it does not silently omit the failing workload.

Upstream source references:
- https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/temporal/server_option.go
- https://github.com/temporalio/omes/blob/c6978ba39aa03551ce28974117e8d7ecf983d2b3/docs/running.md
- https://github.com/temporalio/ui/tree/835b349ffa14fbe71af3e26a7f8bc71fe6c29a53

The current single rerun command is `python3 scripts/check-ministack.py`. It uses
the pinned Temporal configuration parser, Compose validation and the pinned HAProxy
binary's configuration checker. Its evidence explicitly sets runtime_executed=false
and proof_pass=false even when configuration is valid. It does not launch the stack.
