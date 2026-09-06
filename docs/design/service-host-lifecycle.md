# Service host lifecycle

The application now requires explicit `service_storage` format2 configuration and
validates the existing Temporal logical domains before provisioning. The public
CLI checks the same requirement. There is no legacy Manager fallback. Legacy
storage code remains available to historical compatibility probes; the main app
instantiates only `storage.ServiceRuntime`.

The host consumes one validated Prepared capsule, constructs one cluster service,
one membership driver and a static map containing one partition service per
explicit physical layout slot, and installs the real shared RPC router. Remote
partitions stay idle. Routing owns logical resolution, exact authority checks,
writer borrowing and token-bound failure reporting; the host does not duplicate
those decisions. One listener serves every persistence family.

Control and partition Poll calls run independently of membership I/O. Both use
process-local monotonic elapsed ticks. Every registry effect has a configured
context timeout; native effects retain completion ownership under cancellation.
A separate serialized loop publishes this process's heartbeat, verifies index
admission, and performs bounded peer discovery only while its observed incarnation
is coordinator. Partial/error views are unready and tenure remains structural.
No wall clock enters controller decisions. RPC semantic time uses its explicit
injected host clock.

Ready requires current successful membership registration and a real routed global
ListClusterMetadata operation with its durable admission barrier. It waits only
for typed transient failures within the supplied deadline and preserves permanent
errors. A zero-owner forwarding instance can become ready through that same route.
A single successful probe cannot guarantee that Temporal initialization will avoid
a later movement gap; bounded initialization retry policy and full-stack proof
remain separate gates. Passive diagnostics perform no storage/native operations
and do not substitute for Ready.

Stop cancels the host loops, closes RPC admission and outbound connections, marks
all drivers stopped, and waits for host goroutines and every cluster/partition
effect to drain. An outstanding native effect is retained and a spent stop budget
returns `agent.ErrProcessExitRequired`; it never authorizes freeing a live handle.
Repeated Stop after completed teardown succeeds, including with a canceled budget.
The existing agent lifecycle keeps storage alive until embedded Temporal has
stopped. Failed setup cannot begin partition effects before the full RPC assembly
and listener exist.

Focused race tests cover missing configuration, the required logical mapping,
registry budget/unknown-outcome preservation and a delayed native-open boundary
that survives the first Stop budget and drains after completion. They do not
constitute executable multi-node or native S3 failover acceptance. The coordinating
agent runs that next using the committed fresh agent scenario.

### Startup ownership gaps (delegated correction)

The composed run `agent-20260906T114456-274d67` at `ced487c` failed when
Temporal's ringpop startup called `UpsertClusterMembership` after C's successful
Temporal frontend health check (not the app `/readyz` probe). Global routing returned `no ready partition owner`. The saved
pre-join control was ready on B; no post-failure control survived, so the exact
assignment versus writer reactivation cause is unproven. Both can legitimately
make admission temporarily unavailable. A readiness probe cannot promise that
later initialization calls avoid movement.

Cluster operations now retry transport Unavailable within their original caller
or 30-second invocation deadline, retaining the same operation ID, command digest
and request. Backoff starts at 20ms and caps at 250ms. This spends the existing
bounded operation budget; the router's one-hop and three-owner-attempt bounds
remain unchanged. Permanent errors stop immediately. An explicit UNKNOWN_OUTCOME
retains its first wire details across subsequent failure/cancellation, while a
plain admission failure does not override a permanent terminal status. A valid
ClusterResult resolves ambiguity because ClusterService returns it only after
replay and AwaitDurable, including persisted logical errors.

`go test -race ./internal/temporal/adapter -run TestCluster -count=1` exercises startup
membership recovery past the former three-attempt ceiling, unchanged request and
deadline, caller cancellation, terminal status precedence, and unknown outcome
preservation through Temporal's serviceerror conversion. This is a focused
regression; the composed scenario must be rerun before claiming startup proof.

### Health deadline and retained completion

The clean `495819c` run `agent-20260906T122650-8e58ea` reached C's healthy
state and observed C serve matching generation 3. After the scheduled SIGKILL of
B, A exited with `agent health: context deadline exceeded` and
`process exit required`. The lifecycle used the same five-second context both to
cancel Ready and to decide its call had not completed. A normal cancellation
unwind can lose that scheduling race; the error alone does not establish a hung
native effect.

Health expiry now revokes readiness while retaining the single pending probe.
Only observed completion permits another probe. No work overlaps and no
cancellation is treated as proof of completed resource use. A permanently stuck
probe leaves the process unready; external shutdown while it remains pending
still requires process exit without component teardown. Startup and shutdown
bounds, native driver drain rules, and the health observation deadline remain
unchanged. This is a delegated lifecycle correction, not an extension of the
readiness timeout or a claim that an expired probe succeeded.

The focused agent race suite includes a gated cancellation-unwind regression and
the existing shutdown-with-pending-probe safety regression. The real composed
fault scenario remains the acceptance boundary for failover progress.

### Cold readiness uses the caller's budget

The clean `7df6453` run `agent-20260906T134326-ad453f` passed join, assigned
service, B's crash/eviction/restart, 20 Omes workflows and UI/SDK checks. During
all-cold restart A then returned `storage readiness: context deadline exceeded`.
Its captured log creation and final-write timestamps span about 30.30 seconds,
matching the adapter's 30-second child invocation budget, not the 120-second
startup budget. B and C entered Temporal initialization shortly afterward, before
harness cleanup canceled them. No cold control snapshot survived, so this does
not establish which election, membership or native-open phase occupied that time.

Readiness now recognizes completed child context deadlines and Temporal's typed
Unavailable/DeadlineExceeded errors. It retries those observations within the
unchanged parent budget; raw gRPC status conversion did not recognize these
adapter return types. Caller cancellation, explicit permanent errors and invalid
configuration remain terminal. It does not acknowledge an unknown operation or
assume membership eligibility: success still requires a new successful routed
probe. A regression expires an actual child context while the parent remains
live, then confirms successful observation without changing its deadline; other
controls exhaust the parent budget and reject permanent-error retries.
