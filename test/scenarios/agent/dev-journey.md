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
actual interval overlap. Without the optional admission relay below this is
observational overlap only. The independent complete expected execution graph
census and child/activity/Nexus fan-out limits remain unqualified. It cannot satisfy full GEN-02/CLEAN-01 by itself. Search errors
stop immediately, retain all artifacts, and trigger owned fixture teardown.


### Controlled root admission

Build the fixture-only relay from the same candidate checkout:

```sh
go build -o /absolute/xenon-admission-proxy ./cmd/xenon-admission-proxy
```

Add `--admission-proxy /absolute/xenon-admission-proxy --workflow-concurrency 4`
to the generated journey above. The relay forwards normal Temporal unary RPCs
but holds generated root workflow-task polls. It admits exactly one start per
member queue, rejects eager-execution requests before forwarding them, queries
all four exact run IDs through Temporal Describe, and
persists their Running observations before releasing any workflow task. Before
admitting another window it verifies each preceding root's latest run is terminal,
so Continue-As-New cannot hide an unfinished chain. Ambiguous start results,
duplicate starts, unknown terminal states, failed observations or failed receipt
writes invalidate the fixture. This is a test tool, not Xenon request routing.
It intentionally fails ambiguous RPC retries rather than claiming fault recovery.

Repeat with `--workflow-concurrency 1` in a **fresh owned fixture and evidence
directory** to compare the same generated inputs. Do not reuse the previous
fixture's durable volume or namespace for this comparison: workflow run IDs are
seed-derived. The existing dev journey refuses to adopt an initialized fixture.
Each run checks twelve distinct admitted root/run IDs against the saved initial
root history census, three cases of four member queues, complete released
windows, and exact actual root execution interval peaks (four or one). The relay
binary hash, launch command, admission observations and verified process cleanup
are retained. A single invocation proves only its selected concurrency setting;
retain both receipts for the comparison.

This supplies controlled-admission tooling and focused seam tests. It does not
claim a real controlled-admission run has passed, full expected graph/semantic
oracle coverage, fan-out policy enforcement, or full GEN-02/#119 acceptance.

```sh
go test -race ./cmd/xenon-admission-proxy
python3 -m unittest discover -s scripts -p 'test_*journey.py'
```


The first controlled `ff8acce` run exposed a fixture gap: generated Omes cases
can use signal-with-start and update-with-start (`ExecuteMultiOperation`). The
latter may wait for update execution before returning. The relay now forwards
these original atomic RPCs exactly once and observes their freshly created run
IDs independently before releasing polls; it does not split an update-with-start
into separate operations. Eager composite starts and preexisting root IDs are
rejected before forwarding. Component controls keep the composite response
pending while proving the barrier observation completes.

Failed run evidence is retained at
`/Users/calum/Documents/ChatGPT/xenon-generated-ff8acce-concurrency4-attested`.
It completed SDK/Nexus setup but failed the first generated case; this is not
controlled-admission acceptance. Its exact owned-container census is empty and
both helper process groups were drained. Both real concurrency controls must be
rerun from an integrated candidate containing the composite-start fix.

The next `b4536f1` controlled run observed four Running roots but then rejected a
legitimate generated signal-with-start targeting another admitted root. The
relay now distinguishes follow-ups by both workflow ID and member queue. It
preserves the original composite RPC and verifies that its response reused the
admitted run or a Describe-confirmed Continue-As-New chain. A response reporting
a newly started root, missing identity or unrelated chain invalidates the run;
request policies are not silently rewritten. Replacement detection is after the
original RPC and is not a claim that its side effects were prevented.

That failed run's logs, four-root admission receipt and independent empty cleanup
census remain at
`/Users/calum/Documents/ChatGPT/xenon-generated-b4536f1-concurrency4`.
Both complete real concurrency controls remain outstanding.
