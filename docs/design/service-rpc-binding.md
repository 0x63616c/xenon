# Service runtime RPC binding

The coordinator and independent systems reviewer accepted this binding as a
delegated agent decision on 2026-09-06. It composes existing cluster, partition,
persistence and routing services; it does not use the legacy ownership manager
or native node Owner.

`routing.NewServer(ServerConfig)` returns the registered gRPC server and its
outbound Router. The host supplies a static full-layout map of partition drivers,
registry/control configuration, explicit cluster/layout pins, local owner,
outcome capacity and clock. The host owns polling, stop/drain, serving and closing
the router. Constructor copies the layout/map; it does not infer a layout from
loaded storage or open a writer.

Every resolution reads and decodes authoritative control, validates the expected
layout digest and cluster ID, and maps the external logical partition through the
explicit layout. Only a ready owner is routable. The resolved hint binds cluster,
layout digest, physical partition, Node, incarnation, assignment revision,
reservation and generation. Path is already bound by the layout digest.

Both local and forwarded operations retain that exact hint. Forwarding carries one
bounded, canonical, single-valued metadata field alongside the existing hop count.
A forwarded receiver never replaces the incoming hint with a fresh resolution.
Missing/malformed metadata fails closed. The origin makes at most three attempts;
the forwarded receiver makes one attempt and never forwards recursively.

Local dispatch calls `partitions.Service.Writer` once to atomically capture writer,
open attempt and effect token. A private request clone changes only the partition
envelope from logical name to explicit canonical ID for the persistence service.
The external request, command digest, operation ID, payloads, pagination and
deadline remain unchanged. Visibility receives the pinned layout and separately
validates its document's logical partition.

The persistence authority callback runs under Begin/BeginOperation admission. It
rereads pinned control and compares the full hint, path and current readiness. A
compare-only driver reborrow checks that the original attempt/token is still
current and the driver is not stopping. The failure callback always captures the
original driver and effect token; a replacement token never receives an older
writer's failure.

A refusal before application execution is a typed stale-owner result. Native
fencing, retirement or unknown outcomes after family execution starts are
conservatively typed unknown outcomes: an execution history child may already be
durable. The raw error reaches `ObserveFailure` before wire conversion. An origin
retains the first unknown outcome across later failed attempts, including resolver
failure or cancellation; only a successful durable response resolves it.

`go test -race ./internal/routing` runs the committed portable qualification:
all twelve real service constructors/journals through actual partition drivers,
unchanged envelopes and deadlines, full forwarding hints, same-address newer
reservation rejection, layout/cluster pin rejection, and retirement after a
durable execution child. Writer and registry effects in these routing tests are
controlled fixtures. Native lifecycle/family tests remain separate; this is not
a claim of full application, multi-node or real-S3 acceptance.
