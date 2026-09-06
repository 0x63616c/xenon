# Routing effects

`internal/routing.Router` keeps the production bounded routing/retry decisions in
one interceptor. `Invoke` optionally replaces only an outbound RPC effect; nil
uses the existing gRPC client pool. The resolver and local dispatcher remain the
existing seams. Configure all fields before serving and keep them immutable.

An injected transport receives the original protobuf request, method, destination
and outgoing context. It must preserve the request, honor cancellation/deadlines,
fill the response, and return the original typed gRPC error. A simulator can model
message loss, delayed completion and stale routes here. It owns injected transport
cleanup. `Router.Close` owns only the default client pool. Pool eviction selects
the lexically smallest address, avoiding dependence on Go map iteration.

The router requires a deadline but creates no clocks/timers. A deterministic
caller can supply a logical context; a production context retains ordinary gRPC
deadline behavior. Concurrency and storage effects are not made deterministic by
this transport seam alone.

## Passive events

Optional `Events` is a caller-owned channel receiving these categories:

| Kind | Meaning |
| --- | --- |
| `resolve` | A directory lookup returned, including an explicit refresh on attempt 1 |
| `local` | Local dispatch returned |
| `forward` | An outbound transport invocation returned |

Every event includes a zero-based attempt (0 or 1) and resulting gRPC status code.
There are no payloads, operation/workflow IDs, addresses, partition labels or
wall-clock reads. Events describe individual attempts, not durable commits or
unique customer operations. Validation rejection before lookup emits no event.

Delivery is nonblocking and drops events when no channel capacity is available.
Consumers may export counters labelled by kind/attempt/code, but those counters
are diagnostic and can undercount. Do not use them for billing or correctness
assertions. Keep the channel open until requests finish; the router never closes
it. Exporters run outside request execution. For a bounded deterministic scenario,
provide sufficient buffer and assert the complete event trace after execution.

## Repeatable routing checks

```sh
go test -race ./internal/routing
```

`TestInjectedTransportLostResponseRefresh` uses a fixed logical deadline and a
fixed two-attempt schedule: response loss at the prior address, directory refresh,
and success at the replacement. It checks unchanged request identity/digest,
deadline, metadata hop count and exact routing events. Additional tests check typed
errors, nonblocking saturated observers, deterministic eviction and existing real
gRPC forwarding behavior.

These are routing tests, **not durable retry or owner-crash simulation**. Their
response double does not prove journal recovery. Ownership and the production
journal's durable replay barrier require their separate simulation and real-stack
proofs.
