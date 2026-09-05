# Actual owner process cuts

`python3 scripts/prove.py process-cut` builds the pinned native Go owner, runs
identity/lifetime controls, and starts isolated pinned MinIO on loopback19010.
The component controller starts the actual Go node on19011 with metadata-only
control on19012. It acknowledges a baseline shard, arms the exact target UPDATE,
observes a real barrier, verifies PID/incarnation and sends actual OS SIGKILL.
Each replacement uses a fresh local working directory and the same S3 prefix.

Before retry, a fresh native handle independently reads raw shard, outcome and
outcome-count keys. Pre-Await permits either wholly old or wholly new state:
Commit returning does not imply the background WAL flush has not already run.
After successful AwaitDurable and before final owner publication, all new state
and the exact result must be recoverable. Exact-ID retry must succeed without
reapplying the range-conditional mutation; final raw count must remain two.
The controller verifies the process exited because of SIGKILL and that no RPC
success was delivered. These assertions are stronger than returning an injected
Unavailable result from a fake store.

Instrumentation is disabled unless XENON_PROCESS_CUT_PLAN names a plan. Plan
schema1 selects only shard UPDATE or execution UPDATE by exact operation ID,
partition, family, mutation kind and command digest. Its local control accepts
one ARM containing the plan session and fresh boot incarnation. A copied plan
on a restarted process remains unarmed; old arm identities are rejected. No HTTP
operation can alter the selector, resume a pause, write data or request a kill.
Only metadata is reported; the external controller owns process termination.

The native worker holds the owner gate while paused. Commit/Await barriers also
retain the durability handle. A pause timeout is bounded at five seconds and
quarantines the owner before any gate release. A pre-Await timeout still drains
AwaitDurable before destroying its handle; an indefinitely blocked native wait
retains the worker/gate under the existing outer timeout. The final-publication
barrier follows the managed authority barrier and precedes publishing the owner
result. Native waits have completed there, so no pending handle is invented.

This first proof uses controlled direct shard RPC IDs. Real Temporal adapters
generate IDs internally; discovering and arming the exact live mutation before
dispatch remains a separate runtime hook requirement. Execution UPDATE selection
is wired but not a workflow crash-proof claim. Directory response loss, delayed
owners, full workloads, maintenance composition and real-S3 execution remain
separate acceptance gates. Current smoke defaults are unchanged.
