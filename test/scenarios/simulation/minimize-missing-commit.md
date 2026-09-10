# Coupled component reduction fixture

`minimize-missing-commit.json` derives from `coordinator-move.json`: insert 22
repeated initial `writer-old` polls while its first read is already pending, and
remove the final successful commit. The production partition Step emits no new
effect for the repeated polls. The final checker fails the typed
`progress/missing_live_commit` invariant. This is an intentionally incomplete
schedule, not evidence of a production engine bug.

The original contains 105 actions and the three existing named negative-control
cut labels. With no negative-control mode selected, these labels do not activate
a mutation. Their count is metadata, **not three injected physical faults**.
The fixture proves actual production-Step reduction and replay, not full MIN-01
fault coverage or real-stack reduction acceptance.

With the repository's pinned Go/native link environment, run:

```sh
go test -race ./internal/simulation ./cmd/xenon \
  -run 'TestMinimizeCoupledArtifactAndDependencyRepair|TestMinimizeCLIActualCoupledFailure' -count=1
```

The tests assert the original fingerprint; at least 20 removed actions; unchanged
original bytes; no dangling consumers of a removed effect creator; identical
fingerprints on three exact best-artifact replays; tamper rejection; and CLI
budget exhaustion preserving a replayable verified best. The complete reducer
also requires three candidate-specific identical traces before each acceptance.
No globally minimal result is claimed. The test has a two-minute reduction bound,
256 replay attempts, 2,048 proposals and three-second per-attempt budgets.

For manual use with a built CLI:

```sh
xenon test simulation --scenario test/scenarios/simulation/minimize-missing-commit.json --evidence /tmp/xenon-original
# Expected exit 1: the saved intended failure.
xenon minimize --artifact /tmp/xenon-original/case-00000000000000000000/scenario.json --evidence /tmp/xenon-reduced --duration 2m
xenon replay --artifact /tmp/xenon-reduced/best-scenario.json --evidence /tmp/xenon-replay
# Expected replay exit 1: the same intended failure.
```

Use new paths on each run; evidence is never overwritten. Development builds need
`--development`. Minimization exit 0 means the bounded proposal space was exhausted
with a reproducible failure retained; 2 means incomplete due to budget; 130 means
cancellation; other errors return 1. `best-scenario.json` exists only after the
original reproduces three times. No generator is needed to replay that file.

Only complete coupled traces are currently reducible. Candidates remove one
chosen action plus transitive effect/application/handle dependents. Their effect
IDs and final assertions are preserved; production replay rejects any invalid
remaining dependencies. The resident Omes artifact kind is explicitly unsupported.

Replay integrity: new expanded artifacts use schema 2. An envelope checksum
covers saved configuration, generator identity, provenance, expanded inputs and
replay origin, in addition to the scenario checksum. Replay and minimization
require the saved source revision and tool-version map to match the current
runner before evidence creation or driver calls. Checksums detect accidental
edits; they do not authenticate an author capable of recomputing checksums.

Old schema-1 artifacts are refused by default. `xenon replay
--allow-legacy-artifact ...` explicitly permits their scenario-only checksum,
labels the result `legacy-unverified-artifact-replay`, and still requires matching
source/tool provenance. This mode cannot supply exact-revision acceptance proof.
Minimization requires schema 2. `--development` permits matching unknown/dirty
build identities and retains development qualification; it does not bypass a
provenance mismatch. No cross-version compatibility override is implemented.
