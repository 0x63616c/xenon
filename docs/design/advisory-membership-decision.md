# Advisory discovery and heartbeats

Delegated agent decision, 2026-09-06. The coordinating agent accepted this design
under Calum's autonomous delivery authorization after a user-priority proposal
and independent adversarial review. It is not a claim of personal design approval.

Use the existing conditional registry unchanged: a bounded registration index at
`<configured-prefix>/index`, and one heartbeat at
`<configured-prefix>/<node-ID>/<incarnation-ID>`. Both payloads bind the explicit
cluster ID and immutable layout digest. Each process must generate a fresh
incarnation at startup and retain it for that process lifetime. Addresses belong
to that incarnation and cannot change while its sequence advances. The index is
advisory and grants no reservation, coordinator or native writer authority.

This serves stable paths, portable S3/filesystem deployment and one Go process
without adding a consensus protocol. Filesystem registry filenames hash logical
keys, so native prefix discovery would otherwise require a different encoding or
scanning unrelated files. Index CAS occurs on registration and pruning; heartbeats
never rewrite the authority record or entire member set.

`Membership.Heartbeat(ctx, tick)` advances this incarnation and verifies its index
entry on every call, rejoining after mistaken removal. Full admission fails with
`ErrMembershipFull`; heartbeat publication alone does not mean registered.
`Discover(ctx, tick, coordinator)` is called only by the active coordinator and
reads at most the configured number of peer heartbeats per call. The host
serializes these calls and interleaves heartbeat work between scan batches. Every
operation has the host's context budget; no controller uses a wall clock or starts
its own timer. Scans spanning a complete suspicion interval are discarded.

`MembershipView` makes readiness and coordinator incarnation/generation explicit.
`cluster.Service.Poll` requires that view; there is no raw-member compatibility
bypass. Absent, partial, failed, stale-tenure and unready views suppress placement,
including active-move retargeting and ambiguous assignment retries. Election and
renewal remain independent. Ordinary renewal does not reset discovery. A new
coordinator starts with new local observations; it never treats itself alone as
the fleet while discovering peers.

First successful reads establish baselines, not eligibility. Only increasing
heartbeat sequences establish progress. A complete scan must classify every
registered member as progressing or locally unchanged for the configured failure
interval before becoming ready. Missing/error reads are uncertainty and fail the
scan; they do not prune a peer. Regressing sequence or changed identity/address/pin
fails validation. Sequence overflow requires a new process incarnation. Two
progressing incarnations with the same node ID are both excluded; there is no
liveness promise until that configuration error ends or one stops progressing.
No remote timestamps or filesystem/S3 modification times are used.

Pruning rechecks the suspect heartbeat and conditionally publishes the original
index version with that exact entry removed. A concurrent fresh heartbeat can
still be mistakenly suspected; it conveys no authority and the live process
re-registers. Index conflicts discard suspicion for the selected entry; the next
scan must establish new evidence, preventing removal/re-registration ABA from
rebasing an old prune. Unknown writes retain exact bytes, transition and condition
for original-version retry. A newer coherent record permits fresh validated work
while retaining historical ambiguity in `LastUnknown`; current registration does
not prove a historical write succeeded.

All limits and intervals are explicit configuration, with no inferred fleet size
or timeout defaults. Normal follower work is O(1): with the present S3 registry,
a successful heartbeat plus verification uses three GETs and one PUT (including
publication preflight), plus independently scheduled control polling. Each complete
coordinator scan uses one index GET plus N heartbeat GETs, and pruning adds a
heartbeat check plus conditional publication/preflight. Takeover incurs baseline
and progress scans. These are operation counts, not latency or capacity results.
The approximately 100-node target remains unqualified; healthy latency relative
to configured intervals is required for liveness.

The *active index* is bounded by entry and encoded-byte limits. Historical
per-incarnation heartbeat objects remain in storage because registry has no delete
contract. Offline orphan cleanup is a separate gate; this design does not imply
bounded lifetime object count. Discovery implementation and component tests do not
establish app wiring, full executable failover, real AWS or fleet-scale acceptance.

Reproduce filesystem/fault tests with `go test -race ./internal/cluster`.
The existing pinned MinIO registry fixture now includes `advisory_membership`:
`XENON_DIRECTORY_PROJECT=xenon-directory-<12-hex-run-id> python3 scripts/directory-proof.py registry`.
The runner sets up and tears down only its named project. Credentials in that
fixture are emulator-only; real AWS credentials remain external.
