# Fresh service runtime bootstrap boundary

Delegated agent decision, 2026-09-06: the coordinator accepted this bounded design
under Calum's autonomous delivery authorization after independent advocate and
adversarial review. This is not a claim of personal approval or populated cutover.

The agent configuration can explicitly provide `service_storage` format 2. It
requires canonical cluster/node IDs, the full ordered immutable physical layout,
all controller/membership timings and resource limits. Names, order, paths, counts
and IDs are never inferred. Timing values are positive Go duration strings.
Human cluster name and HistoryShards retain the existing manifest checks; layout
and wire versions remain pinned to 1. The layout digest is derived from configured
layout, never adopted from storage. Paths must lie beneath exactly
`<prefix>/data/`, in addition to layout's canonical/nonoverlap constraints.
`service_storage: null`, partial and invalid configurations fail rather than
selecting legacy behavior.

`storage.PrepareServiceStorage` consumes the validated configuration, S3 client,
injected identity source and caller context (further bounded by registry_timeout).
It creates a fresh process incarnation, prevalidates initial control size, then
reads the *existing legacy manifest key* `<prefix>/metadata/cluster.json`.
The new canonical format2 marker retains cluster-name/history-count/layout/wire
fields and adds canonical ClusterID, LayoutDigest and a unique initialization
transition. It is never overwritten by this API.

An existing format1 marker always refuses new preparation. An existing matching
format2 marker requires an existing valid, matching cluster/layout control record
at `<prefix>/metadata/registry/cluster/control`. Missing or corrupt authority is
an explicit offline recovery case even when bootstrap flags remain enabled.
No Prepared object exposes a reusable FreshNamespace permission; its controller
configuration always disables controller bootstrap.

Only an absent manifest, explicit `bootstrap` plus `fresh_namespace`, and an empty
bounded S3 prefix listing permit a new marker claim. The list uses MaxKeys=1 and
checks the entire `<prefix>/` namespace, including pre-manifest topology and data.
Listing is an occupancy check, not a lock. Conditional create at the same manifest
key arbitrates concurrent startup. Hidden manifest mutation retries are disabled.
Exact readback of this call's unique initialization recovers a lost response;
a different claimant with identical configuration is an ordinary join, not a
transfer of initialization permission. Errors are not interpreted as absence.

The winning call creates and confirms the initial control before returning an
opaque Prepared capsule. Initial coordinator generation and assignment revision
are 1; every explicitly configured physical slot initially names this actual
bootstrap process, with generation 0, no reservation and not ready. This is an
initial-authority policy, not a fabricated singleton membership view. Native
opening still requires ordinary confirmed reservations. Subsequent placement
requires actual coordinator-bound membership discovery. All paths and ordering
remain exactly configured.

A crash after marker creation but before confirmed initial control can leave an
incomplete namespace. Restart fails closed; there is no new durable initialization
protocol or automatic repair in this batch. This deliberate limitation also
prevents recreating a deleted live control record.

## Legacy exclusion and remaining cutover

The existing application calls `ownership.EnsureCluster` before listener creation,
NewManager and Join. It accepts only format1 and conditionally creates that same
manifest key. Therefore a manifest-honoring old process cannot start after the
new marker; an already-running manifest-honoring old process has an existing old
marker that blocks fresh preparation. The real S3 test races both protocols and
requires exactly one winner. Another test races identical new claimants and
requires exactly one initial control publication.

This does not exclude pre-manifest binaries, manual marker deletion, unsupported
out-of-band object mutations or principals bypassing the application protocol.
Production namespace policy must forbid those actions. A populated legacy prefix
requires explicit offline stop/exclusion, preserved metadata and exact path/order
migration evidence; this API neither performs nor silently infers that migration.
There is no dual manager or alternate open of the same paths.

## Next app composition boundary

The legacy `storage.Runtime.Start` rejects service_storage before any storage or
listener side effects. New runtime assembly is deliberately not claimed ready.
The next app change consumes one Prepared capsule to instantiate registry-backed
cluster and membership services, one partition service per exact physical slot,
and the new persistence dispatch/router for every required family. Only that new
runtime may consume this configuration; it must never fall back to Manager.

The host supplies monotonic elapsed ticks, independently schedules heartbeats and
bounded coordinator scans, and feeds only matching ready MembershipViews to the
cluster service. RPC resolution uses the configured logical-name-to-physical-ID
mapping and fresh validated ready control. Local admission captures the exact
Writer request/token and reports failure to that token. Shutdown stops admission,
then drains cluster and partition effects or reports process-exit-required.

A successful single global metadata read does not guarantee Temporal startup will
survive a later ownership gap. The host/adapter retry policy must preserve the
original deadline and cancellation while retrying bounded typed transient
admission errors; permanent errors must still fail. This remains part of actual
runtime activation, not a promise made by preparation.

## Repeatable evidence

Run `go test -race ./internal/app` with the pinned native
library environment, or run the full bootstrap proof:

```
XENON_DIRECTORY_PROJECT=xenon-directory-<12-hex-run-id> python3 scripts/directory-proof.py bootstrap
```

The bootstrap profile builds the pinned native dependency/toolchain required to
link the storage package, checks pinned Docker/Compose versions, runs the committed
MinIO schedule, and verifies scoped container/volume cleanup. It does not open a
native database or qualify real AWS, full executable activation or fleet capacity.
