# Observability boundaries

Instrumentation must describe decisions without becoming admission authority or
altering durable outcomes. Current seams support optional capture; a configured
metrics exporter, tracing backend and stable production metric catalog are still
outstanding wiring. Do not infer those capabilities from the interfaces below.

## Routing events

`routing.Router.Events` is an optional caller-owned send-only channel configured
before serving. `routing.Event` has three bounded fields:

| Field | Meaning | Values |
| --- | --- | --- |
| `Kind` | Completed routing stage | `resolve`, `local`, `forward` |
| `Attempt` | Zero-based outer routing attempt | 0 or 1 |
| `Code` | Resulting gRPC status | gRPC status-code enum |

No operation identity, partition ID, address, workflow ID, payload or digest is
included. Events are emitted with a nonblocking channel send and may be dropped
when the channel is full or no receiver is ready. They are diagnostic samples,
not exact billing counts or proof of acknowledged writes. A single operation can
emit several stage events because it resolves, forwards and retries.

Configure the channel before starting requests and keep it open until all serving
work has ended. Closing it while the router may send is unsafe. A nil channel
turns capture off. A simulator may use a sufficiently buffered channel and drain
it after controlled steps; deterministic delivery still depends on the simulator
controlling production decision scheduling, not on this channel alone.

A future collector can aggregate counters by kind, attempt and status, with units
of observed stage completions. Those are proposed metrics, not current exported
metric names. Export network calls belong in a separate consumer and must never
run under a partition gate. Exporter failure must not change routing outcomes.
Dropped-event accounting and duration histograms require additional explicit
implementation before anyone treats this stream as a complete performance record.

## Ownership outcome diagnostics

`ownership.Manager.OutcomesHandler` serves JSON at `GET /outcomes`. The legacy
managed-node command enables its listener with `XENON_METRICS_LISTEN`. The unified
agent does not currently wire this listener into its configuration. This endpoint
is an aggregate diagnostic read, not a Prometheus endpoint or readiness grant.

The handler admits at most four concurrent requests and bounds its request context
to ten seconds. It reads actual outcome accounting through the owner, so a scrape
can involve native storage work and a durability barrier. It is not the passive
routing-event seam. Failed reads return unavailable diagnostics; do not use them
to elect an owner or authorize writes.

Reports include per-partition local dispatch attempts, RPC results and successful
operations, plus durable outcome entries and encoded outcome bytes where readable.
Counts of observed local dispatch are volatile process counters. Durable journal
usage survives recovery but is not a count of unique application workflows.
Encoded bytes exclude native storage overhead; legacy unaccounted entries and
partial errors are represented explicitly. Do not equate this diagnostic with
S3 cost, physical byte usage or durable acknowledgment totals.

Keep diagnostics on the trusted administrative network. Unlike bounded routing
events, the detailed report includes node, incarnation, address and partition
metadata; do not convert arbitrary identifiers into metric labels.

## Remaining integration

Temporal's persistence factory currently receives but discards its metrics handler.
Routing capture is optional and is not yet connected to an agent exporter. Native
and Temporal logs likewise are not a unified telemetry contract. Future wiring
must document metric names, units, bounded labels, process-reset semantics and
loss behavior, plus scrape/export configuration and diagnosis examples.

Latency, ownership transitions, durable commits, retry recovery and readiness need
specific implementation and tests before inclusion in that catalog. Logical time
inside deterministic tests must not silently become wall-clock duration metrics.
Keep exporter goroutines and wall-clock collection outside the controlled
coordination boundary. Existing SDK/Temporal/native real-stack measurements remain
separate evidence from the simulated decision trace.
