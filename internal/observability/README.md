# Passive agent diagnostics

`New()` allocates a 1024-event buffer. Attach `Metrics.Events` to the router before
serving, run `Metrics.Run(ctx)` under the agent lifecycle, and expose
`Metrics.Handler(ready, version)` through the agent's protected diagnostics
listener. Do not close the channel while any router can still emit. Cancelling
the collector may discard buffered events. Collector/exporter failure must not
change request admission or persistence decisions.

| Endpoint | Contract |
| --- | --- |
| `/metrics` | Prometheus text 0.0.4, fixed series, process-local counters reset on restart |
| `/readyz` | 200 when the supplied readiness callback returns true; otherwise 503 |
| `/version` | JSON from the supplied build-version callback; 500 if it cannot be encoded |

`xenon_routing_observed_events_total` counts observed completed routing attempts.
Labels are `kind` (`resolve`, `local`, `forward`, `other`), zero-based `attempt`
(`0`, `1`, `other`), and `code` (the 17 standard gRPC status names or `other`).
Unexpected categories collapse to `other`. There are always 216 series, including
zero values. There are no workflow, operation, partition, node or payload labels.

- `resolve` records directory lookup completion, including refresh on attempt 1.
- `local` records local dispatch completion.
- `forward` records outbound invocation completion.

A rising `forward` / `Unavailable` count can help investigate unavailable owners;
`resolve` failures point toward directory lookup trouble. These are diagnostic
signals, not an assertion of the root cause. Validation before lookup produces no
event. One request can produce multiple events, and successful dispatch does not
independently prove a durable commit.

Router sends are nonblocking. Full buffers drop events before collection without
a drop counter, so this metric can undercount. It is unsuitable for billing,
exact request totals, durability assertions or progress guarantees. A slow HTTP
scrape performs no I/O on the routing goroutine. Simulation may omit this collector
or capture a sufficiently buffered exact event trace separately. No clock,
randomness or scheduling decision is introduced into coordination by this package.

This initial surface does not claim latency, ownership, tracing or engine metrics.

Repeatable package check (using the repository-pinned Go toolchain):

```sh
go test -race ./internal/observability
```
