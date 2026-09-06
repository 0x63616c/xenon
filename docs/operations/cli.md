# Xenon CLI foundation

`xenon` is the single CLI entry point. This foundation delivers the existing
server commands, help/completion, finite coupled-simulation test/search/replay,
and opt-in real smoke/ten-minute profile entries.
Cluster inspection, development orchestration, generated workflow search and
minimization remain tracked in #119 and are not advertised as implemented.

```sh
xenon --help
xenon version
xenon check-config --config deploy/agent.example.json
xenon start --config /path/to/agent.json
xenon completion bash > xenon.bash
xenon completion zsh > _xenon
xenon completion fish > xenon.fish
xenon completion powershell > xenon.ps1
```

The packaged example includes the explicit format 2 service layout for a fresh
namespace. Copy it and configure your bucket, prefix, immutable layout paths and
node identity before starting; it is not an existing-state migration recipe. Completion output is generated on stdout; install
it according to `xenon completion <shell> --help`. Commands never prompt.

## Configuration and output contract

`--config FILE` is required for `start` and `check-config`. The explicit JSON file
is the only source of agent configuration: there is no automatic environment
merging, home-directory search or inferred layout. Existing `agent.Load`, exact
service-layout validation and Temporal configuration validation remain in use.
External AWS credential/environment handling remains owned by the runtime and is
not consulted by help, completion, version or configuration validation.

- `version` emits the existing single build-metadata JSON object on stdout.
- `check-config` emits exactly `XENON_CONFIG_VALID` followed by a newline.
- `start` emits the same build JSON before calling the foreground application.
  Subsequent runtime logging retains its existing behavior; the entire server
  log stream is not a single JSON result.
- Help and completion go to stdout. CLI errors go once to stderr without an
  automatic usage dump. Neither errors nor help echo configuration contents.
- Exit 0 means command success; exit 1 means invalid arguments, configuration,
  output or runtime failure. Existing foreground shutdown semantics remain:
  SIGINT/SIGTERM cancels the runtime context, bounded application cleanup finishes,
  and a clean shutdown returns 0. A failed or incomplete shutdown returns 1.
  Cancellation before command execution returns 1 without starting the runtime.
  A clean server shutdown is not a workload/search pass. The simulation commands
  below use explicit canceled and budget results.

## Implementation decision and verification

Delegated implementation choice under the approved CLI plan: use
[Cobra v1.10.2](https://github.com/spf13/cobra/releases/tag/v1.10.2) for command
parsing, standard help and the
[upstream shell completion generators](https://cobra.dev/docs/how-to-guides/shell-completion/).
The module and checksums are pinned; pflag was already in the dependency graph.
No Viper, styling library or TUI was added.

Commands stay in `cmd/xenon`. `newCommand` constructs a fresh command tree with an
injected foreground function; `execute` supplies context, arguments, stdin,
stdout and stderr. New commands can later bind typed harness APIs without
changing process-global streams, installing signal handlers in tests or starting
backends during construction. Only `mainExit` installs OS signal handling.

Run `go test -race -count=1 ./cmd/xenon` using the repository's pinned Go/native
link environment. Tests exercise all four completion generators, help, preserved
machine outputs, strict command/config failures, output failure before startup,
and caller-context cancellation with preserved shutdown errors. A forbidden
stdin and startup callback prove lightweight commands neither prompt nor enter
the backend. The executable still links its existing native runtime dependency;
this is not a claim of a separate native-free binary or full CLI delivery.


## Saved simulation test, finite search and replay

These journeys use the shared `internal/simulation` Runner and production-Step
coupled driver. They do not start Temporal, SDK workers, SlateDB databases or
containers. They execute the saved ordered coordinator/partition effects with
modeled registry/native behavior and check final recovery/progress.

```sh
xenon test simulation \
  --scenario test/scenarios/simulation/coordinator-move.json \
  --evidence /tmp/xenon-simulation-test
xenon search \
  --scenario test/scenarios/simulation/coordinator-move.json \
  --max-cases 1 --duration 1m --evidence /tmp/xenon-simulation-search
xenon replay \
  --artifact /tmp/xenon-simulation-test/case-00000000000000000000/scenario.json \
  --evidence /tmp/xenon-simulation-replay
```

Every evidence directory must be new and its parent must already exist. Search
accepts repeated `--scenario FILE` flags and explores that **finite saved corpus**;
zero `--max-cases` means all supplied files. It does not randomly generate new
workflows or loop the corpus to manufacture coverage. `--workload-seed` and
`--fault-seed` are recorded independently but unused by this fixed corpus.

The supported profile has at most 256 ordered events, depth 1, 1 MiB expanded
payload and 8 MiB trace per case. It allows one in-flight case, a one-minute
default total budget (`--duration` overrides it), and one-second settle/cleanup
budgets. These values are saved in each artifact. Replay uses the artifact's
saved bytes, event ordering and budgets, independently of the current generator;
its new evidence records the current executing build.

By default these commands require a known clean 40-hex source build revision.
`--development` explicitly permits missing or dirty build identity and labels the
JSON result `development`. Evidence records build revision/dirty state/toolchain,
modeled native/no-image boundary, generator source hash, expanded bytes and hashes,
original scenario file hashes, trace, first failure and cleanup outcomes. This is
component evidence, not full Temporal/Nexus or real-S3 qualification.

Each invocation that reaches the runner writes exactly one stdout JSON object:
`schema`, `mode`, `qualification` and `result` (including `completed`, `stop_reason`
and absolute `evidence_path`). Diagnostics remain on stderr. CLI argument/file/
provenance failures before runner entry use stderr without a result object.
Test/search/replay share these exit codes:

| Exit | Meaning |
| --- | --- |
| 0 | Requested cases completed and recovery/cleanup checks passed |
| 1 | Invalid input, invariant/recovery/output failure, or failed cleanup |
| 2 | Declared exploration budget ended; unfinished cases are not passes |
| 130 | Caller cancellation; evidence and bounded cleanup were retained |

A budget/cancellation result never reports an incomplete case as completed.
Already completed cases are only the explored prefix. If cleanup cannot finish,
the error reports process-exit-required; no next case is launched. The original
artifact is never overwritten by replay. There is no `minimize` command yet.

For an executable check, build the reviewed source using the repository's pinned
Go/native environment, run the three commands above, and compare their saved
`trace.jsonl` files. Unit contracts also execute these exact command paths against
real saved production-Step bytes, checking finite limits, bad-effect failure,
replay, canceled/budget exit codes, JSON streams, and backend-free help.

The executable smoke harness makes these assertions automatically, including
byte-identical traces and actual nonzero failure/budget exits:

```sh
python3 scripts/cli-simulation-proof.py --binary .local/xenon \
  --source "$(git rev-parse HEAD)" --evidence /tmp/xenon-cli-proof
```

Build `.local/xenon` from that clean revision first. Retain the resulting
`receipt.json` and case artifacts. The harness removes AWS credentials from child
environments and accepts the repository's existing native loader environment;
linking the binary does not execute a native storage scenario.

## Real local agent profiles

The single CLI also exposes the reviewed Python implementation helpers:

```sh
xenon test smoke --repository /absolute/path/to/xenon --evidence /tmp/xenon-smoke
xenon test local-release-ten-minute --repository /absolute/path/to/xenon \
  --evidence /tmp/xenon-ten-minute
```

Both paths are explicit; the evidence directory must be new and its parent must
exist. The checkout must contain the pinned scenario scripts, config and sources.
Python 3, Git, Go, Rust/rustup/Cargo, a C compiler, AWS CLI, Docker Compose and the
already-pulled pinned local images are required. The helper validates source and
build pins and refuses occupied scenario ports. `--development` explicitly
permits a dirty checkout and retains development qualification. Help, version and
config validation do not inspect these tools or initialize backends.

The helper remains responsible for the actual workload and all owned agents,
workers and Compose resources. See [the ten-minute contract](../../test/scenarios/agent/ten-minute.md)
for separate setup/workload/drain/recovery budgets, exact saved corpus, faults and
remaining oracle gaps. Exposing the command is not proof that either runtime
profile has passed. The CLI records its own build independently from the explicit
checkout used by the helper; the two source identities are not silently equated.

Each invocation returns one stdout JSON object with schema, profile, status,
evidence path, helper receipt and whether cleanup was verified. Raw helper output
is retained in `helper.log`; command diagnostics/errors use stderr. `cli-request.json`
and `cli-result.json` preserve the invocation and result; helper evidence lives in
`run/`. Existing evidence is never overwritten. Exit 0 requires a passing helper
receipt matching the requested profile. Failure is 1, an outer deadline is 2,
and cancellation is 130. An already-recorded helper failure remains a failure if
cancellation arrives afterward. Canceled/budget runs are never passes.

On cancellation the CLI signals the helper group and allows 150 seconds for its
bounded diagnostics and cleanup. It then kills and reaps the helper, with a final
five-second reap bound. Forced termination reports cleanup unverified: a wedged
helper may not have retired its separately supervised child groups or Compose
project. Retain the evidence for scoped recovery; this is never reported as a
clean shutdown. The outer ceilings are 20 minutes for smoke and three hours for
the full profile; they do not extend any shorter helper workload budget.

Tests execute the typed command binding and real child supervision with fake
helpers: clean JSON, explicit paths, preserved receipts/logs, cancellation,
forced termination, backend-free help and missing input/tool failures. These
controls open no agent ports, storage engines or containers.
