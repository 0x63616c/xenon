# Xenon CLI foundation

`xenon` is the single CLI entry point. This foundation delivers the existing
server commands plus help and completion. Cluster inspection, development
orchestration, tests, search, replay and minimization are still tracked in #119;
they are not advertised as implemented commands.

```sh
xenon --help
xenon version
xenon check-config --config test/scenarios/agent/a.json
xenon start --config /path/to/agent.json
xenon completion bash > xenon.bash
xenon completion zsh > _xenon
xenon completion fish > xenon.fish
xenon completion powershell > xenon.ps1
```

The example scenario configuration is a local proof fixture, not a production
configuration recommendation. Completion output is generated on stdout; install
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
  A clean server shutdown is not a workload/search pass; those commands are not
  delivered by this slice.

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
