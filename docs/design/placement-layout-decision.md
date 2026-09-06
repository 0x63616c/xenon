# Placement and storage layout: delegated implementation decision

2026-09-06, partial resolution of [#116](https://github.com/0x63616c/xenon/issues/116).
The coordinating agent adjudicated independent user-priority and adversarial
systems assessments against the committed experiments below. These are delegated
decisions, not claims of additional personal approval by Calum. They permit the
first production service slice; they do not close the capacity qualification.

Use `buraksezer/consistent` v1.1.0, reconstructed from canonically sorted stable
NodeIDs, SHA-256's first eight bytes in big-endian order, 20 replicas and load
factor 1.25. Incarnations fence ownership but do not determine placement. Persist
the planner version/settings and immutable ordered physical partition identities
with the cluster layout. Changing the count or ordering on populated state is a
migration, never a configuration-only rehash. The [placement comparison](../../benchmarks/placement/README.md)
measures 276 fixed membership snapshots. At 1024 slots/100 members, adding member
101 moves 1.76% under bounded hashing versus 90.23% under round robin. Bounded
hashing's count concentration was 1.27 maximum/mean versus rendezvous's 2.05;
rounding means 1.25 is not a strict relative bound at every count. Its roughly
5.8-ms plan cost is small relative to native recovery. None of these algorithms
balances a hot partition's workload.

Preserve one physical SlateDB for each existing indivisible persistence partition
in the first service slice. Preserve existing matching, global, visibility and
history transaction domains and data prefixes. Do not consolidate independent
domains to save control-record bytes. The [native grouping experiment](../../benchmarks/storage/layout/RESULTS.md)
compares sixteen synthetic logical domains across one, four and sixteen writers.
Own-write durability stays around 100 ms while sharing fewer serialized gates
raises queueing; throughput was approximately 9.83, 39.4 and 111–158 operations/s.
The six cases recovered all acknowledged outcomes after quiescent takeover and
reopen. Their small state sets and two-second idle windows do not measure mature
compaction, production recovery, or whole-server resource cost.

Start one physical-database rebalance move at a time. Persist the active move in
the same authoritative control record as its assignment so coordinator replacement
does not forget the budget. Retarget that same move when its target is no longer
eligible; do not start a second move while the first is pending. Initial database
startup is distinct from voluntary rebalancing. Native calls retain their pending
effect/handle until completion, even after timeout or supersession. A completed
current ready publication releases the move budget. Delayed obsolete opens must
eventually retire, and the current owner must recover after finite faults stop.

Retain one bounded control record for coordinator authority, assignments,
reservations and readiness. Exact whole-record CAS is the safety mechanism.
The [real MinIO contention experiment](../../benchmarks/storage/contention/README.md)
preserves failed connection/ambiguous-write baselines and demonstrates bounded
receipt-based reconciliation. At 1024 synthetic partitions and 100 contenders it
completed all updates, but had an 18.775-second maximum renewal gap and transferred
roughly 14.5 GB of response bodies. This rules out deriving a short suspicion
timeout from its nominal 20-ms renewal interval. Coalesce compatible node-owned
changes, retain one unresolved attempt per actor and qualify the actual controller
before selecting polling/renewal/suspicion timings. Retained-receipt negative proof
requires all writers to preserve other actors' receipts; it is not a guarantee of
the generic registry interface. A superseded ownership reservation must retire,
without inventing a historical-success claim.

Still open in #116: actual proposed physical-count footprint and populated recovery,
embedded Temporal footprint, sustained maintenance, actual control-schema size and
driver contention, chosen runtime cadence, and approximately 100-server capacity.
No 256/1024-writer default follows from these measurements. Zero-owner instances
must be proved ready through real routing/Temporal behavior. Fixed physical layout
has a finite writer ceiling; increasing it requires an explicit split/migration
protocol preserving fenced transactions and replay. Real AWS and NFS/SMB remain
unqualified as recorded in the specification.

The research runners use real time for native/backend measurement. Production
coordination decisions and deterministic simulations retain injected elapsed time.
Review identified and corrected native workload first-failure cancellation and
supervisor cleanup races; historical receipts remain bound to their original
source revisions, and corrected-run receipts must not be inferred from them.
