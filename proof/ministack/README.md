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


## Runtime candidate

`python3 scripts/ministack-runtime.py` now builds the actual tagged launcher and
SDK oracle, then supervises the declared real runtime stages. The launcher build
requires the real VisibilityFactory and routed DataStoreFactory to be integrated;
a missing implementation fails the run. `go test ./internal/ministack` checks only
the SDK workload construction and cannot satisfy that runtime gate. The SDK oracle
checks retry attempt2, a completed child, fired timers, signal/update acknowledgment,
continue-as-new and exact result/history across both runs. Bootstrap seeds standard
SQL-style search slots through the guarded metadata manager before using the public
AddSearchAttributes API. The UI probe uses pinned Playwright and Chromium to inspect
list/filter/detail pages; Omes uses its own pinned module and retains generated
worker inputs for hashing. None of these candidate stages is claimed passed until
the runtime report says so after all assertions and cleanup.

This profile is the bounded first-boot smoke, not the full delivery acceptance profile in `docs/design/acceptance.md`. Even a runtime pass here does not cover the required 100 simple, 40 throughput and 20 frozen fuzz workloads, ten local operations per node, movement of both history and visibility partitions, or 2,000 frozen visibility rows paged at 1/7/100. Those remain a separate declarative follow-on after boot is proven.

Initial cluster bootstrap starts Temporal A and requires its direct health check before starting B; both must serve before work begins. The pinned upstream cluster-metadata initializer uses a one-shot conditional create and concurrent initialization of an empty cluster can reject the losing process. Existing persistence conditions remain unchanged.

The controller installs official Node v24.19.0/npm11.17.0 from SHA256-pinned platform archives into `.local`, and retains browser binaries there. The previous Node26.8.1 smoke reached SDK/Omes/visibility assertions but failed its300second browser installation deadline during extraction; it is retained as a failed run, not UI evidence.

Prospective delegated deadline correction: smoke `c09a2d7` was recorded FAILED after the generic20s control-command supervisor cut off failover. Surviving Temporal logs showed connection refusals to killed history18234/matching18235 until that cutoff. The smoke now uses the existing locked120s recovery budget from `docs/design/acceptance.md` as one monotonic deadline from the Temporal kill through control and live verification; it does not reset per command. Ordinary probes retain20s, native/per-RPC ceilings and full acceptance budgets are unchanged, and the old run remains failed.

Bootstrap also pre-registers pinned Omes defaults `OmesExecutionID`/`KS_Keyword` (Keyword) and `KS_Int` (Int), then successfully compiles an alias-filtered Count through both direct frontends before work begins. Omes remains unchanged. Its own immediate AddSearchAttributes→Start sequence can race normal namespace-cache propagation: smoke `c52d639` recorded that exact failure after successful SDK failover recovery.

## Optional measurements

`python3 scripts/ministack-runtime.py --measurements` applies the committed
`measurements.json` instrumentation configuration without changing smoke workload
counts or fault schedules. The local meter starts after MinIO readiness and spans
cold node restarts; directory/native clients share its endpoint. Long-lived
launched host process incarnations are sampled; short CLI probes and container
resources are outside those samples. Each Temporal process gets a unique trace.
After successful server.Stop, it drains rpctrace.Close and confirms closure in
its log. The controller requires that confirmation, zero exit status and a
successful trace footer before declaring its trace complete.

Raw traces and samples, hashes, separate invocation/Execute-attempt histograms,
status counts and final S3 counters are retained under the runtime evidence.
P99 is unavailable below100samples per family/kind, rather than inferred from a
small population. The recorded scope is entire process lifetimes, not selected
steady or fault windows. Default execution does not enable instrumentation.

The mandatory SIGKILL necessarily prevents that process's trace from completing.
This opt-in reports `functional_result` separately and fails `measurement_complete`
and the overall proof when requested data are incomplete. A passing functional
smoke must not be reported as a full measured acceptance pass. Phase-scoped trace
rotation or an external synchronous recorder is still required to establish
complete steady/fault windows. This limitation is preserved, not bypassed by
silently removing killed-process data.

`python3 scripts/prove.py runtime-measurements` validates the committed parser,
completeness failures, histogram separation, host resource controls and supervisor
controls. It does not execute or claim the measured ministack/full acceptance.
