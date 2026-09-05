# Maintaining a Temporal version upgrade

Xenon currently pins Temporal in `go.mod` and records its source contracts in `docs/research`. An upgrade begins with explicit old and proposed new Temporal revisions. Do not substitute a moving “latest” ref or silently change a pin. The project skill is [.agents/skills/xenon-temporal-upgrade/SKILL.md](../.agents/skills/xenon-temporal-upgrade/SKILL.md).

Generate a source inventory from a clean local Temporal Git checkout containing both revisions:

```sh
python3 scripts/temporal_upgrade.py --source /absolute/path/to/temporal --old OLD_COMMIT --new NEW_COMMIT --output .local/evidence/temporal-upgrade-impact
python3 -m unittest discover -s scripts -p test_temporal_upgrade.py
```

The output directory must be new and outside the Temporal checkout. The script resolves both refs to immutable commits, rejects tracked or untracked source changes, rechecks cleanliness and refs, saves the binary-capable patch and its hash, and records Xenon/script provenance. It never checks out code, edits dependencies, fetches a target, or declares compatibility. Rename guessing is disabled so both removed and added paths remain visible. Review every unmapped path; path classification cannot detect unchanged interface signatures whose runtime behavior changed elsewhere.

Keep version-sensitive adaptation concentrated in these existing seams:

| Temporal change | Xenon review boundary | Evidence to refresh |
| --- | --- | --- |
| Persistence interfaces, internal requests, manager behavior | `internal/adapter`, `internal/temporalstore`, typed RPC contracts | Interface composition, typed errors, callbacks, transaction guards and upstream suites |
| Persistence protobufs and opaque encodings | `internal/adapter/execution_codec.go`, `proto/xenon/v1`, `internal/node` | Oneofs, unknown fields, enums, UUID/time/byte round trips and stored outcome decoding |
| SQL schema or persistence SQL behavior | `internal/node`, `docs/research` | Intended conditions, ordering, conflict/version semantics, documented SQL deviations; SQL is an oracle, never Xenon durable storage |
| Visibility/search attributes/query conversion | `internal/query`, `internal/visibility`, visibility adapter | Raw values versus generated comparisons, nulls, aliases, pagination, CHASM, PostgreSQL oracle fixtures |
| Server configuration, launch wiring and dependencies | `internal/temporalstore`, `cmd`, `tools`, `test/scenarios/ministack` | Both factories, native binding pin, multiple Temporal processes, actual SDK/UI/Omes runtime |

Update the seam inventory alongside new boundaries instead of scattering version conditionals through handlers. A removed or added method needs an explicit behavior and complete transaction contract; do not satisfy compilation with placeholder success. Changing generated RPC schemas must preserve durable outcome decoding or supply a reviewed migration policy.

For an authorized implementation, use an isolated Xenon branch, update the explicit target pin, inspect the full diff and resolve affected methods. Then run the existing reproducible gates appropriate to the change:

```sh
make test-go
python3 scripts/prove.py go-runtime-stores
python3 scripts/prove.py go-visibility
python3 scripts/prove.py go-visibility-frozen
python3 scripts/ministack-runtime.py
```

These commands have their own pinned native/emulator/environment prerequisites; read each manifest and the ministack runbook before execution. A source inventory or test command printed in a report is not a test receipt. Record exact commits, configuration/input hashes, failures and cleanup. Refresh Omes/fault/measurement gates from `docs/design/acceptance.md`; the smoke alone is not full acceptance.

Report two independent outcomes:

* **Fresh install:** run the target build against a new isolated S3 prefix, exercise all required stores and actual workflows, query complete visibility, and execute declared recovery tests. A passing component suite does not imply this outcome passed.
* **Existing-state upgrade:** first create a committed, reproducible fixture using the old build: workflow histories and active/completed executions, queue/task cursors, namespaces/search schema, visibility/tombstones, operation journals and ownership metadata. Stop producers and old owners according to the selected upgrade policy. Cold-start the target against those same S3 objects and disposable local disks; verify decoding, continued workflows, replay identity, pagination policy and no loss of acknowledged writes. Record pre/post object/input manifests. Do not silently replace this prefix with an empty one. Rollback and mixed-version operation require their own evidence and explicit supported policy.

This first tooling slice supplies the source-impact inventory and orchestration guidance only. It does not yet provide an old-build/target-build migration fixture runner. Both compatibility outcomes remain **NOT_TESTED** until separately executed; a fresh-install pass cannot promote existing-state upgrade. No Temporal version upgrade is performed by this tooling change.
