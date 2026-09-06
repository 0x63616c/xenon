# Agent bootstrap and joining

The agent calls `ownership.Join` once during startup, before starting ownership
reconciliation. An explicitly bootstrapping agent may create missing topology;
ordinary joins fail when topology is absent. All agents use the same data prefix
and fixed partition layout: global, matching, history-0 through history-3, and
vis-v1-0 through vis-v1-3. Existing IDs and data prefixes must match exactly.
Changing this layout requires a separate migration, not adding more replicas.

Joining records the process incarnation and contact address in S3 topology, then
assigns sorted partitions round-robin across sorted member IDs. Counts differ by
at most one. This deliberately simple policy can move more partitions than an
incremental placement algorithm, and more than ten members leaves some without
owned storage. It never changes durable partition identities. The existing
ownership manager must reserve, open, fence and publish readiness before serving;
a successful join alone grants no writer authority or readiness.

Publication uses the existing conditional S3 topology operation, including its
unknown-outcome readback. A join retries at most eight times within the caller's
context, rereading topology between attempts. An observed registration of this
exact identity completes the attempt. A concurrent replacement of the initial
same-node predecessor aborts rather than stealing registration back.

A fresh process may replace its node ID's previous incarnation once. Runtime code
must never call Join repeatedly from its ownership reconciliation loop. A new
stateless Join invocation cannot tell an intentional restart from stale reentry;
node IDs must be controlled by deployment configuration. Concurrent duplicate node
IDs are an operator error, not a supported replica configuration.

Each process advances a counter in the conditional S3 topology. Surviving agents
observe peer counter changes and may remove one unchanged peer after 15 seconds
measured on their own monotonic clock. That interval triggers suspicion only; it
never grants writer authority. The topology CAS, owner directory, per-operation
authority checks, and SlateDB fencing remain the safety boundary. A concurrently
restarted incarnation changes the member record and cannot be removed by a stale
CAS. A removed process treats lost membership as permanent and exits.

The production loop passes time explicitly into the membership decision so the
same decision can run under deterministic scheduling. Unit proofs cover live
heartbeats, lost publication responses, stale incarnation replacement,
deterministic rebalance, and competing evictors. The extended real-stack eviction
scenario remains a separate gate.
