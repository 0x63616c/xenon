# Short local CLI journey (CLI-02)

Build the immutable local fixture with `scripts/build-dev-fixture.py` from a clean
candidate checkout. Build the host `xenon` CLI using the pinned native setup. Then:

```sh
python3 scripts/dev-journey.py \
  --cli /absolute/path/to/xenon \
  --native-library /absolute/path/to/libslatedb_uniffi.dylib \
  --build-receipt /absolute/path/to/image-evidence/build.json \
  --fixture /absolute/path/to/image-evidence/fixture.json \
  --evidence /absolute/path/to/new-journey-evidence
```

The script requires the seven fixture ports to be free. It creates a separate
stopped sentinel container, invokes CLI `dev up`, executes the SDK durable
workflow (retry, child, signal, update, Continue-As-New and complete history
assertions), and executes four real synchronous Nexus echo operations through
the named endpoint, one per declared Temporal history shard. It pauses only its
three storage writer containers while comparing CLI inspection against a
separately downloaded S3 authority envelope, then unpauses them. It verifies
idempotent CLI teardown, zero owned containers, preservation of the exact object
volume, survival of the sentinel, and unavailable inspection without invented
control state. Finally it removes its sentinel by exact container ID.

Every subprocess has an explicit timeout. CLI teardown runs in `finally`; errors
retain a failed receipt and pending cleanup details. No broad Docker prune or
volume deletion is performed. Evidence includes command arguments, outputs,
checksums, source/image identity, and SDK histories. The object volume is retained
for inspection even on failure. Explicit later ephemeral teardown is a separate
action; do not automatically discard it.

`--discovery` permits an older immutable image and dirty source for investigation,
but is always recorded and cannot certify exact-revision acceptance. A successful
journey is component evidence, not the complete #119 gate: side-effect
instrumentation, independent source/binary provenance and aggregate receipt
validation remain required. The script intentionally never sets
`acceptance_pass=true` by itself.

Run the independent authority assertion controls without Docker:

```sh
python3 -m unittest discover -s scripts -p test_dev_journey.py
```

Non-discovery runs execute `xenon version` before provisioning and reject stale or
modified CLI builds, unknown native identity, wrong native library checksums,
dependency replacements or mismatched Go/Temporal/SlateDB pins. The loader search
path is bound to the explicitly supplied library directory. Native identity remains
a build attestation, not independent dynamic-loader introspection.

## Generated concurrent component

Add `--search-bundle /absolute/prepared/generator-bundle --history-oracle
/absolute/xenon-omes-oracle` to execute three fresh generated cases with four
root inputs and concurrency four. Build the oracle from this checkout with
`go build -o /absolute/xenon-omes-oracle ./cmd/xenon-omes-oracle` using the same
pinned native environment. The generator bundle comes from the existing pinned
`prepare-workflow-generator.py` setup. The selected worker's checksum is checked;
its process group is recorded and drained before fixture teardown. No previously
running fixture is adopted.

`workflow-observations.json` counts initial roots, child runs, continued runs and
Nexus handler runs from the saved, checksum-verified histories and measures
actual interval overlap. This is component evidence: it has no admission barrier,
no concurrency-one comparison, and no independent complete expected execution
graph census yet. It cannot satisfy full GEN-02/CLEAN-01 by itself. Search errors
stop immediately, retain all artifacts, and trigger owned fixture teardown.
