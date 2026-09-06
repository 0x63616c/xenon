# Fresh seeded workflow-input preparation

This component prepares genuinely new Omes kitchen-sink inputs. It does not run
workflows or explore live fault schedules. Existing finite-corpus `search` keeps
its meaning; continuous real-workflow search still requires a supervised runtime
driver and independent outcome checks.

The pinned upstream Omes generator is `kitchen-sink-gen generate --explicit-seed
UINT64 --generator-config-override FILE --nexus-endpoint xenon-fuzz`. Its source
and Cargo lock match the historical corpus manifest. The registered config keeps
local activities disabled and bounds action-set sizes. Arbitrary configurations
and capability combinations are rejected rather than silently ignored.

The upstream seeded RNG determines actions, but protobuf map order need not give
identical raw bytes. The accepted signal-contract normalizer deterministically
serializes the expanded input and verifies action-tree equivalence, changing only
signal metadata. A separate bounded inspector rejects malformed/unsupported
normalization, more than 2048 action objects, depth over 64, or payload over 1 MiB.
A seed that cannot satisfy this contract fails preparation before workflow
execution; it is not silently skipped or replaced with another seed.

Build tools and the explicit compatible Go worker in a new local bundle:

```sh
python3 scripts/prepare-workflow-generator.py \
  --source /path/to/clean/pinned/omes \
  --output /tmp/xenon-workflow-generator
```

The input checkout must be Omes `c6978ba39aa03551ce28974117e8d7ecf983d2b3`.
The builder reuses `prepare-corrected-omes.py`, its pinned API/protoc/toolchain and
reviewed additive worker overlay. Rust uses the pinned locked dependency graph.
It archives upstream source, builds a separate generator, and compiles the
unchanged normalizer plus inspector in a separate copy; it never edits historical
corpora or the prepared worker source. Generator/config/normalizer/worker binary
hashes and source compatibility identities are checked before and after each
input. This is local build attestation, not a cryptographic trusted release.

```sh
xenon generate workflow --bundle /tmp/xenon-workflow-generator \
  --workload-seed 42 --fault-seed 99 --max-cases 3 --evidence /tmp/xenon-new-inputs
xenon replay --artifact /tmp/xenon-new-inputs/case-00000000000000000000/scenario.json \
  --evidence /tmp/xenon-input-replay
```

Generation uses the shared Runner's independent per-case workload and fault RNG
identities, one in-flight case, exact expanded Scenario artifact, first failure
and bounded cleanup. The workload seed is actually consumed by Omes. Fault seed
metadata says explicitly that no fault exploration is implemented; changing it
does not alter workload bytes. The CLI's `workflow-input-preparation-only` mode
means completed inputs, **not successful workflows**. Replay validates stored
input hashes/bounds and reproduces the preparation trace without invoking the
current generator or normalizer. It does not re-execute or semantically revalidate
a Temporal workflow. The real runtime must still validate its supported features.

Controls are ordinary `internal/simulation`/CLI tests; fake leaf tools are marked
as contract controls. Actual pinned generation is opt-in and opens no servers:

```sh
XENON_WORKFLOW_GENERATOR_BUNDLE=/tmp/xenon-workflow-generator \
  go test -race -count=1 -run TestActualPreparedWorkflowGenerator -v ./internal/simulation
```

Run with the repository's pinned Go/native link environment. This checks fresh
seeds and deterministic normalized outputs from the actual Rust/Go tools and
records generator identity, not real-stack correctness or continuous exploration.

## Resident execution adapter (execution qualification requires an actual run)

`xenon test workflow` uses the same expanded Scenario/Search envelope and a
separate `omes-resident-workflow-v1` driver. It runs the exact normalized protobuf
through the pinned corrected Omes command and compatible worker, then invokes the
independent history oracle. The default `generate workflow` and generic artifact
driver cannot silently substitute preparation for this execution.

The caller must own an externally supervised, exclusive fixture with the normal
schema and namespace ready, plus the `xenon-fuzz` endpoint targeting a compatible
resident worker at `omes-xenon-ministack-fuzz`. This driver does not start agents,
provision S3, create endpoints, or claim cold recovery. The fixture file pins these
bindings, with `run_id` and `fixture_sha256` empty (the latter is computed over the
exact file bytes):

```json
{"version":1,"address":"127.0.0.1:17233","namespace":"xenon-ministack","run_id":"","nexus_endpoint":"xenon-fuzz","nexus_task_queue":"omes-xenon-ministack-fuzz","fixture_sha256":""}
```

Use a clean CLI build and a prepared corrected worker `build.json` produced by
`prepare-corrected-omes.py` (also included in the generator bundle's `worker/`).
Build `./cmd/xenon-omes-oracle` from the same reviewed source and supply its exact
SHA256. Example after the external supervisor has made the fixture ready:

```sh
xenon test workflow --bundle "$GENERATOR_BUNDLE" \
  --runtime-build "$GENERATOR_BUNDLE/worker/build.json" \
  --resident-fixture "$FIXTURE_JSON" --history-oracle "$HISTORY_ORACLE" \
  --history-oracle-sha256 "$ORACLE_SHA256" --workload-seed 42 --max-cases 1 \
  --evidence "$NEW_SCENARIO_DIRECTORY" --runtime-evidence "$NEW_RUNTIME_DIRECTORY"
```

Replay uses `xenon replay --artifact CASE/scenario.json --evidence NEW` with the
same five runtime flags. It does not load a generator or normalize bytes again.
The saved topology and case run ID must match. Recreate/reset the externally owned
fixture before replay; an occupied case queue is rejected rather than reusing old
results. Empty visibility is an observation under this explicit exclusive/fresh
fixture contract, not distributed exclusion against another writer.

Before launch the shared envelope and raw input are retained. Tool manifests,
prepared source/worker files and binaries are hashed before/after execution.
Omes terminal iteration errors are latched before bounded process-group shutdown;
logs remain on disk. The independent oracle checks list/count agreement,
contiguous complete terminal histories and successor relationships, and preserves
result bytes. Omes supplies its own semantic result checks. This is not a complete
expected child-execution census or independently derived semantic oracle.

Successful cleanup verifies no visible running case executions after the history
check. On failure it makes bounded scoped termination requests and records
unfinished-history diagnostic pages, but reports remote cleanup **unverified**:
visibility absence alone cannot prove that delayed executions do not exist.
The external supervisor must retire/reset that fixture. No failed case permits
continuous reuse. This bounded adapter has no fault scheduling, throughput/100-node
qualification, cold recovery, or full Temporal/Nexus acceptance claim. Fake-driver
and child-process tests verify the adapter contract; they are not runtime evidence.
