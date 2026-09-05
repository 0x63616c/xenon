# Local proof operations

Xenon is an experimental persistence backend. Component proofs have run; the full
acceptance profile remains open. This guide describes implemented mechanisms and
local test commands, not a production deployment certification.

## Start and rerun

Use a clean committed checkout and the pinned prerequisites listed in README.
`make test` runs Rust and Go checks, building the native dependency before Go tests.
`make proof CASE=owner-manager` runs the declared ownership scenario with scoped
MinIO setup and teardown. Other registered cases are listed by
`python3 scripts/prove.py --help`.

`python3 scripts/check-ministack.py` validates configuration only. It deliberately
never produces a runtime proof pass. `python3 scripts/ministack-runtime.py` runs the
candidate stack: two Temporal processes, two initial Xenon nodes, a third added
node, and pinned MinIO, HAProxy and Temporal UI containers. Its smoke result is
separate from `proof/acceptance/full-profile.json`; passing smoke cannot satisfy
the larger workload, fault and measurement gates. See `proof/ministack/README.md`
for pins, ports and assertions. Run one ministack at a time because its published
ports are fixed. Avoid competing heavy builds during latency-sensitive runs.

## Failure and recovery boundaries

A TCP-ready node is an ingress, not proof of partition ownership. Managed nodes
announce a fresh incarnation on every process launch. Explicit topology publication
activates that exact incarnation; reusing a node name does not activate a replacement.
The node manager reserves ownership, opens the existing S3 prefix, rechecks authority
and publishes READY. Every admitted operation also checks authority and performs a
durable fencing barrier. The local owner-manager proof exercises replacement and
finite delayed contenders; automatic failure detection and rebalancing are not
implemented.

Moving a partition changes its assignment, preserving its logical ID and S3 prefix.
Do not change the history partition list or its order as a scale-out action: the
adapter uses that fixed list for placement. Do not delete ownership metadata, WAL
fences or data prefixes to resolve a stale route. A process with uncertain native
work quarantines its handle; destroying that handle while FFI work remains active
is unsafe. Process replacement and explicit activation are the tested recovery path.

An RPC timeout may follow a durable commit. Preserve the operation ID and input
when retrying; never invent a new ID to turn an unknown outcome into a new write.
Durable journals retain exact results. These journals have a per-partition cap:
`XENON_MAX_OUTCOMES` defaults to 10000, while smoke declares 200000. Reads and logical
failures also consume new entries. Existing results can replay at capacity; new IDs
fail. Safe outcome reclamation is not implemented, so this is a finite proof setup.
See `proof/outcomes/README.md` for accounting and the optional `/outcomes` endpoint.

## Preserve and diagnose evidence

Each declared runner writes a result and raw logs below `.local/evidence/`, with
source and input hashes. Keep the whole run directory when investigating a failure.
A dirty checkout, changed input, missing assertion or failed cleanup cannot be
relabelled as a pass. A later successful rerun receives its own report. Preserve
failed reports and record subsequent infrastructure recovery separately.

If Docker disappears, first verify the daemon and the recorded Compose project.
When it returns, inspect and clean only that run's project using its recorded
configuration. Do not use global volume prune or delete another run's resources.
If compilation reports no space left on device, inspect free space and inactive
compiler caches. Source files, native artifacts referenced by reports, saved corpus
inputs and evidence are not disposable compiler caches.

RPC trace and S3 meter outputs have explicit scopes. Adapter invocation timings
include helper retries but are not whole public API timings; a killed process can
leave an incomplete trace. The local S3 meter counts observed HTTP attempts and body
bytes, not AWS billing or physical wire bytes. Host resource samples can miss brief
peaks and do not include container resource use. Their individual proofs do not
substitute for measurements from the complete acceptance run.

## External shipping gates

MinIO is local emulator evidence only. Real S3 requires an explicitly authorized
bucket, prefix and credential scope; keep secrets outside committed configuration.
GitHub Actions must run successfully under normal branch protections before merge.
A billing or spending-limit rejection is a blocked gate, not permission to waive CI.
The repository stays private until Calum explicitly authorizes public release.
