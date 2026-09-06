# Development and verification loop

Run commands from the repository root. `make help` lists the entrypoints.

| Stage | Command | Evidence boundary |
| --- | --- | --- |
| Build | `make build` | Compiles Go and pinned SlateDB native dependency |
| Harness controls | `make test-harness` | Tests supervision, manifests and failure accounting; does not run Temporal |
| Go tests | `make test` | Package tests with the race detector; no complete runtime claim |
| Targeted component | `make proof CASE=owner-manager` | Executes that committed manifest and produces an immutable run receipt |
| Real smoke | `make smoke` | Temporal SDK, Omes20, UI, owner replacement, scale-out and cold recovery |
| Saved fuzz soak | `make fuzz-soak` | Actual Temporal/MinIO, declared minimum duration and corpus rounds; no injected faults |
| Local checks | `make check` | Layout, Python controls, Go race tests and Rust reference tests |

`make check` does not replace smoke, fuzz, full fault acceptance, or real-S3
verification. The full acceptance contract remains in
`proof/acceptance/full-profile.json`; its runtime controller is incomplete. An
input validator passing is not an acceptance execution.

## Work on one failure at a time

Begin with the smallest meaningful test that exercises the changed contract.
Use existing Go package tests for transaction conditions and typed errors; use
native/MinIO tests for durability, recovery and writer fencing. Use the real
ministack when the failure involves Temporal behavior across components. Retain
the original failed receipt, implement a regression, then rerun the affected
scenario from a clean commit. Never edit a failed receipt to become successful.

Do not run multiple heavyweight stacks on the same host. Reuse verified build
caches and pinned container images, but give each run its own namespace of process
identities, evidence, local directories and scoped object-store state. Do not reuse
an application-data prefix as a build cache. `make check` serializes its prerequisites;
component package tests may still use their existing internal concurrency.

## Add a scenario

Keep exact tools/topology, workload inputs, fault triggers and assertions committed.
The ministack is consolidated under `test/scenarios/ministack/`, including its
configuration, pins, UI fixtures and related component manifests. Other component
proofs still use `experiments/` and `proof/` until their own bounded migrations. Update input manifests when
moving files and run `make check-layout`. The Rust compatibility harness already
lives under `test/compatibility/rust/`. Historical evidence retains its original
paths and commit identities.

Use observed barriers for faults rather than relying on a sleep to hit a race.
A seeded schedule makes inputs replayable; it does not make OS scheduling or S3
request timing deterministic. Preserve actual trigger identity, operation ID,
source/input/binary hashes and independent state assertions. A timeout is a
failure or unknown outcome, never a synthetic success.

## Upgrades

Use [the Temporal upgrade runbook](temporal-upgrades.md) and the project-local
`xenon-temporal-upgrade` skill. Compare explicitly selected source revisions, map
changes to the adapter and scenario contracts, then separately prove fresh-install
behavior and continuation of state written by the previous version.

Failed ministack runs now capture bounded public workflow list, describe and first-page history snapshots before teardown. The committed `test/scenarios/ministack/diagnostics.json` declares limits. Raw responses, hashes, errors and truncation are retained under `failure-diagnostics/`; these best-effort snapshots never establish history completeness or turn a failed run into a pass.
