# Container-owned local development lifecycle

Implementation plan for the CLI-02 portion of #119; not passing evidence.

`xenon dev up --fixture FILE --state DIRECTORY --timeout 5m` validates an
explicit, versioned fixture before Docker effects. `xenon dev down --state
DIRECTORY --timeout 1m [--ephemeral]` operates only on the run persisted there.
JSON results distinguish running, stopped and cleanup-unverified states.
Help and validation never build or provision. Image builds are explicit setup.

A fixture names immutable local image IDs/digests, explicit loopback host ports,
and exactly three format2 node configurations. It uses the existing pinned
MinIO image and the unified-agent image build, extended with existing SDK probe
and ingress programs. MinIO holds the containers' shared network namespace;
three separate agent containers retain the committed localhost address model.
The ingress and compatible worker run in separate containers in that namespace.
Only the namespace holder publishes host ports. No host PID is a cleanup handle.

Before effects, persist a cryptographically random run identity, canonical
fixture hash and normalized configurations. Hold an exclusive advisory lock
through lifecycle operations. Container names and labels bind run, role and
fixture hash. Capture Docker's immutable container/network identifiers on
creation. Recovery enumerates only the exact run labels, then verifies the
recorded role/name/image before accepting a resource created in a crash window.
A conflicting/unverifiable resource is a cleanup blocker, never a deletion target.

The named object volume carries the same ownership labels and persists across
ordinary down/up. Reusing state requires the identical fixture and preserved
volume; a missing previously initialized volume fails rather than silently
creating an empty cluster. Up preserves cluster ID, prefix and partition layout.
Fresh namespace setup is only for the initial run; preserved state rejoins it.
The saved inspect configuration targets the same explicitly published MinIO port.

Down first persists explicit ephemeral authorization when requested. Under one
absolute deadline it stops/removes verified containers in reverse dependency
order, checks their absence, and removes the verified network if present.
Default down preserves the object volume; ephemeral down removes it only after
all dependents are absent and its identity/labels still match. Evidence, config
and state remain. Repeated down verifies the same census and is safe. A sentinel
with a different run identity survives. Failure, timeout or unavailable Docker
leaves cleanup unverified with pending IDs, never reports success.

Tests must cover validation before effects, malformed/changed state, label/image
mismatch, crash between create and record, immutable-ID replacement, cancellation,
one cleanup budget, repeated down, preserved reuse and explicit ephemeral intent.
A real CLI journey additionally builds pinned images from a clean source revision,
uses isolated host ports, proves readiness, runs SDK and Nexus work, compares
inspect to independently read authority, preserves/reuses data and verifies that
sentinel resources survive both ordinary and ephemeral teardown.

## Executable setup and current limits

From a clean committed checkout, run `python3 scripts/build-dev-fixture.py
--evidence /absolute/new/build-evidence`. This explicitly builds the
`dev-runtime` target, records its immutable image ID and emits `fixture.json`.
The ordinary Dockerfile target remains the production image. The additional
local target reuses the existing SDK probe, ingress and pinned Omes Go worker;
it does not add a new Nexus implementation. The generated fixture reserves
loopback ports 30006, 30233, 30243, 30935, 30250, 31250 and 32250.

Run `xenon dev up --fixture /absolute/build-evidence/fixture.json --state
/absolute/persistent-state --timeout 5m`. The public schema contains only the
immutable fixture image and seven host ports; the three-node topology is fixed.
The application waits for three readiness endpoints, registers the existing
namespace/schema and observes workflow pollers on the SDK and Omes queues.
It emits one schema-1 JSON terminal object with status `ready` or a nonzero
failure. This readiness does not itself execute the required SDK/Nexus journey.

Inspect with `AWS_DEFAULT_REGION=us-east-1 AWS_ACCESS_KEY_ID=xenon-local
AWS_SECRET_ACCESS_KEY=xenon-local-test-only AWS_ENDPOINT=http://127.0.0.1:30006
xenon inspect --config /absolute/persistent-state/inspect.json`. Use the selected
S3 port if the fixture differs from the generated default.

Run `xenon dev down --state /absolute/persistent-state --timeout 1m` to remove
owned process containers while retaining object data. Up with the same fixture
and state reuses that data and prefix. Add `--ephemeral` only to retire the
fixture and remove its verified object volume. Retired state cannot be reused.
Configs and ownership state remain. An incomplete up intentionally retains its
recorded resources for explicit bounded down and diagnosis.
