# Clean real generated-workflow component checkpoint

Candidate: `82408eb068d46aa7da386277dd16cc1befc0de8f`. The freshly built image and
host CLI identify the same clean revision. Nine journey assertions passed. The
search completed three cases containing twelve initial roots. Actual execution
interval overlap peaked at four in each case; the visibility-derived histories
included 69 child runs and two continued runs. The four separate named-endpoint
Nexus echo checks passed; this generated inventory contained zero Nexus handler
runs, which is not evidence of full Nexus graph coverage.

`result.json` contains the exact image, build/tool/native identities, fixture and
input hashes, selected commands, case receipts, counts and cleanup enumeration.
`outputs.sha256.json` indexes all 266 preserved source output files. No raw runtime
logs, object data or container state are embedded in this 68 KB checkpoint. Hashes
were verified against the original evidence directory before committing it:

```sh
python3 test/scenarios/agent/evidence/82408eb-generated-concurrent/verify.py
```

Supply `--evidence` and `--build` if the preserved directories have moved. The
verifier fails when an artifact is missing or altered; it verifies integrity and
recorded component assertions, not the complete #119 specification.

Reproduction from a clean checkout of the candidate (use new output directories):

```sh
python3 scripts/build-go-node.py
python3 scripts/build-dev-fixture.py --evidence "$PWD/.local/image-82408eb"
git clone --no-checkout https://github.com/temporalio/omes .local/omes-82408eb
git -C .local/omes-82408eb checkout --detach c6978ba39aa03551ce28974117e8d7ecf983d2b3
python3 scripts/prepare-workflow-generator.py \
  --source "$PWD/.local/omes-82408eb" --output "$PWD/.local/generator-82408eb"
CGO_LDFLAGS="-L$PWD/.local/slatedb-native-target/debug" \
  go build -o .local/bin/xenon-omes-oracle ./cmd/xenon-omes-oracle
python3 scripts/dev-journey.py \
  --cli "$PWD/.local/bin/xenon" \
  --native-library "$PWD/.local/slatedb-native-target/debug/libslatedb_uniffi.dylib" \
  --build-receipt "$PWD/.local/image-82408eb/build.json" \
  --fixture "$PWD/.local/image-82408eb/fixture.json" \
  --evidence "$PWD/.local/journey-82408eb" \
  --search-bundle "$PWD/.local/generator-82408eb" \
  --history-oracle "$PWD/.local/bin/xenon-omes-oracle"
```

The recorded host is macOS; use `.so` for the native library on Linux. Builds use
committed pins. The fixture uses disposable local process/container state and
MinIO, not AWS. Teardown is in the journey's `finally` path. Repeated `dev down`
reported verified cleanup, the independently enumerated owned container set was
empty, and the recorded Nexus worker process group was drained. The exact object
volume was preserved. The unrelated sentinel survived fixture teardown and was
then removed by its explicit owner.

This is **not full GEN-02, ORACLE-01, CLEAN-01 or #119 acceptance**. Remaining work
includes the observable four-root admission barrier, concurrency-one comparison,
independent fan-out bounds, full expected child/Continue-As-New/Nexus execution
graph and semantic result oracle, scoped execution/process census, fault coverage,
and aggregate clean-checkout gates. The component search command still explicitly
uses `--development`; actual clean source metadata does not erase that qualification.
No speed/capacity or full release claim follows from this run.
