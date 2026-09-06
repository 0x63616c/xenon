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
