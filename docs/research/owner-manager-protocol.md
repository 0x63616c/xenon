# Conditional owner manager protocol (proposed)

This is the bounded integration of issues #6, #24, #49 and #46. It retains the
Go node, S3-only durable metadata/data and disposable local state. It is not a
clock lease or autonomous failure detector. Delegated review is required before
implementing the admission seam described below.

## Topology intention

An administrator publishes one bounded, versioned JSON S3 object using create-only
or exact-ETag compare-and-swap. It contains a monotonically increasing revision,
a random transition UUID, member IDs/addresses and an explicit partition-to-member
assignment with immutable data prefixes. The assignment is deterministic because
it is explicit; no election, heartbeat expiry or inferred dead member changes it.
A node gets a fresh random process incarnation on every launch. Membership and
assignment changes are durable only after conditional publication or exact-record
readback reconciliation. Unknown outcomes fail closed. No directory or data object
is deleted during a move. Metadata prefixes must be disjoint from all data prefixes,
and different partitions must have disjoint data prefixes.

## One local acquisition attempt

For each assigned partition, the manager reads fresh topology and directory,
reserves a fresh directory generation/transition for this incarnation, and invokes
SlateDB Build exactly once for that reservation. An attempt record is private and
one-shot; it is not reconstructed from an old opening record after restart.
The data prefix comes from the bound directory, never an RPC argument. After Build,
the manager rechecks fresh desired topology and conditionally publishes READY using
the exact reservation. Only that successful handle/generation pair can be installed
as the partition owner. Failed/superseded readiness retires the opened handle without
serving application work. An opening timeout retains resources until completion;
never Destroy a handle concurrently with native work.

A desired owner whose handle is fenced retires it and makes a fresh reservation
only after reading the current topology again. Reconciliation has bounded backoff
and at most one active opening attempt per partition per process. Previously
scheduled stale attempts may finish and fence a newer writer, but cannot publish
READY or serve work. They do not retry an obsolete assignment. Finite delayed
contenders therefore cause bounded failed service/recovery cycles, not unsafe
acknowledgments. This is not a liveness claim under infinitely many stale contenders.

## Shared admission and publication

Every persistence family already enters Owner.Run. The manager must install an
immutable authority identity and callback there, not only at the router. Under that
same per-partition gate, before invoking the operation, read fresh directory and
require READY plus exact partition, prefix, node, incarnation, generation and
transition. A failure quarantines the handle and returns Unavailable. No local cache
or routing decision grants authority.

Run captures the complete operation result without returning it. Before publishing
any successful result (including journal replay and logical-error results), it writes
a unique nonempty value to one reserved overwritten barrier key using a real native
transaction and waits for its durability. An empty transaction or flush is not a
fencing check. The gate covers authority check, operation, barrier and publication.
A fence/uncertain native error quarantines the owner; a timeout preserves in-flight
resources and rejects subsequent admissions. All persistence families use this path.
The barrier does not replace each operation's atomic durable journal.

A transfer can race an admitted operation. The old operation may linearize before
the newer SlateDB fence even if its response arrives later; the new engine recovers
its acknowledged writes. If the fence wins, the old durable barrier must fail and
no captured result is published. Directory READY by itself does not establish this
ordering. A paused stale opener can fence the current owner after READY, which is
why even reads/replays need a nonempty durable barrier.

## Any-node ingress

The same Go binary registers every available persistence RPC and uses the reviewed
router. Resolve reads fresh directory and returns only READY destinations. Local
dispatch selects the bound partition Owner; it never bypasses Owner.Run. Multiple
partitions share the process but have separate gates and native handles. Unknown
partitions, opening entries, retired handles, stale routes and forwarding loops fail
within the caller deadline. Router retries preserve the original operation ID,
digest and protobuf envelope. An administrator may move an assignment while requests
continue through either node. No data copying is required.

## Committed acceptance proof

A declared MinIO scenario launches identical binaries with fresh incarnations,
records pinned inputs/tool/binary hashes, and asserts:

1. Two nodes own multiple explicitly assigned partitions; either ingress reaches
   the correct owner with opaque bytes and operation replay intact.
2. Add a member and conditionally move one partition; acknowledged writes and
   outcomes recover through the new owner, while another partition progresses.
3. Pause B after reservation before Build; C supersedes B, opens READY and writes
   durably. Resume B: it may fence C but cannot publish READY or serve; C reserves
   afresh and recovers acknowledged state. Repeat with two finite delayed contenders.
4. Capture an old read before a new fence; its nonempty barrier fails, and no stale
   result is published. A fresh owner's read/barrier succeeds.
5. Kill an owner process; explicit reassignment/restart uses a fresh incarnation and
   reservation and recovers durable outcomes. No reuse of an old opening attempt.
6. Conditional topology races, lost response reconciliation, bounded cancellation,
   cleanup and clean-checkout provenance are automated assertions.

Real AWS behavior, automatic failure detection, global listing fanout, complete
Temporal boot and Omes remain separate gates. The test must not imply them.
