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

The source inventory does not execute an upgrade. The bounded workflow runner below adds orchestration, while both compatibility outcomes remain **NOT_TESTED** until separately executed; a fresh-install pass cannot promote existing-state upgrade. No Temporal version upgrade is performed by this tooling change.

## Existing-state workflow component runner

`scripts/upgrade_state.py` now supplies a bounded executable scenario. **No actual old/target version pair has been executed by this change.** It creates a completed two-run DurableWorkflow and a second workflow paused before control using the old Temporal/owner binaries. It stops old producers and owners cleanly, records the object inventory, starts the target with fresh process directories against the same MinIO objects, compares the old completed result and canonical histories, and continues the original active RunID through update, signal and Continue-As-New. It checks visibility after continuation. This is one workflow component, not full upgrade acceptance: queue/cursor fixtures, tombstones, journal replay identities, mixed versions, rollback and a separate fresh-install run remain **NOT_TESTED**.

Prepare two explicit **local** clean Xenon builds and their clean Temporal source checkouts. Each Xenon checkout must contain `.local/bin/{xenon-go-node,xenon-temporal,xenon-topology,xenon-sdk-probe}`, `.local/go-node-build.json`, and its pinned native source/library. Build the launcher with the `ministack` tag using the existing build workflow. The bundle command verifies Git cleanliness and revision, each Go binary's embedded entrypoint/VCS metadata, the launcher's Temporal module version against the explicit source tag/commit, and native library/node/lockfile hashes against the build manifest. It revalidates all bindings before and after execution. This is local reproducibility evidence, not signed supply-chain attestation. Local module replacements are deliberately rejected in this first component.

```sh
python3 scripts/upgrade_state.py bundle --xenon-source /absolute/old-xenon \
  --temporal-source /absolute/old-temporal --temporal-commit OLD_COMMIT \
  --temporal-version OLD_RELEASE_TAG --output /absolute/old-bundle.json
python3 scripts/upgrade_state.py bundle --xenon-source /absolute/target-xenon \
  --temporal-source /absolute/target-temporal --temporal-commit TARGET_COMMIT \
  --temporal-version TARGET_RELEASE_TAG --output /absolute/target-bundle.json
python3 scripts/upgrade_state.py run --plan /absolute/upgrade-plan.json \
  --evidence /absolute/new-evidence-directory
```

The exact plan is `{"schema":1,"old":"/absolute/old-bundle.json","target":"/absolute/target-bundle.json","rehearsal":false}`. Missing artifacts or target metadata fail; no target is chosen, downloaded or built. Identical Temporal commits, release versions or launcher bytes require explicit `rehearsal:true`, which can only produce a rehearsal receipt with compatibility **NOT_TESTED**. Evidence directories must be new. Run from a clean controller checkout, with Go for build-info inspection, Python, AWS CLI, Docker Compose, and the already-present digest-pinned MinIO/HAProxy images from `test/scenarios/ministack/config/compose.json`. The runner uses `--pull never` and the existing local ministack ports, rejecting occupied ports before launch; do not run it alongside the ministack. Only the unique Compose project's volumes are removed during cleanup. Neither AWS credentials nor real AWS endpoints are accepted from the environment.

The old SDK probe remains fixed across both phases. Existing scenario configuration and launcher CLI compatibility are prerequisites; an incompatible target must receive an explicit reviewed scenario update. All commands/log hashes, source and bundle bindings, configuration hashes, object inventories, history files and failure/cleanup status remain in the receipt. A failed decode, continuation, preflight or cleanup cannot produce a component pass. The controller has a 1,800-second outer limit; workflow/probe and process-stop bounds remain enforced. Forced controller SIGKILL is outside this component's cleanup guarantee.

Repeat the lightweight orchestration/provenance/failure controls with:

```sh
python3 -m unittest discover -s scripts -p test_upgrade_state.py -v
```

Those fixtures simulate the phase boundary and exercise real Git cleanliness and command-failure receipts; they do not execute a Temporal upgrade. A future real `component-passed` receipt only promotes the explicitly identified workflow component for that exact pair.
