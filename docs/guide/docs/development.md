# Local development

Use a private checkout you are authorized to access. The commands below describe the unified-agent runtime in this branch; check [verification status](./status.md) before treating any proof as shipping-complete. Start with a clean committed revision when producing proof evidence. Generated builds, emulator state and reports belong under ignored `.local/` paths.

## Prerequisites

The repository pins Go **1.27.1**, Rust **1.94.0** and SlateDB **0.16.0**. Native builds require the platform C/C++ toolchain, Python 3 and Git. S3-emulator proofs require Docker with Compose. Do not replace pinned versions silently when diagnosing a failure.

The full ministack additionally checks AWS CLI **2.36.39**. Its controller prepares Node **24.19.0**, npm **11.17.0**, the pinned Playwright browser, Temporal UI and Omes. Those downloads require network access. Its disk preflight requires at least **5 GiB free**; a cold compiler cache can require more.

The examples below are repository-root commands implemented in the integrated source. `make help` lists the current Go-first development entrypoints. They are test entrypoints, not a claim that every machine or shipping gate has passed.

## Run a component proof

```sh
python3 scripts/prove.py go-runtime-stores
```

Other useful registered cases are `go-history-routing`, `owner-manager`, `go-visibility`, `go-visibility-frozen` and `runtime-measurements`. Each name selects a committed JSON manifest. Ministack-related manifests live in `test/scenarios/ministack/manifests/`; unmigrated components remain in `experiments/`. Use the manifest to inspect its exact commands, inputs, timeout and expected tests before running it.

The runner checks the source state, pinned inputs, tool versions and declared assertions. It writes a receipt and command logs under `.local/evidence/`. Inspect `result.json`, including `proof_pass`, the source revision and any cleanup failures. A dirty development run cannot produce a clean proof pass.

## Run the ordinary test suites

```sh
make test-go
make test-rust
```

`test-go` builds the pinned native library and node, sets the required native loader paths, and runs `go test -race ./...`. `test-rust` runs the locked Rust workspace tests. Prefer these targets to manually inventing CGo linker settings.

Generated protobuf bindings are checked in. Regenerate them through the pinned helper when changing a contract:

```sh
make generate
```

Review the generated diff along with the protocol change. Do not edit generated bindings by hand.

## Run the real ministack smoke

```sh
python3 scripts/ministack-runtime.py
```

This command launches the older split-process proof topology: scoped MinIO, stable HAProxy ingress, two Temporal instances and Go Xenon storage nodes. It remains a regression harness while the unified-agent scenario catches up. It bootstraps the actual namespace and search-attribute aliases, drives the SDK sequence and 20 Omes workflows, adds a node, moves matching, kills selected processes, checks the real UI, and attempts a cold restart.

The local ports include S3 `19006`, Temporal ingress `17233`, Xenon ingress `17935` and UI `18080`. The checked-in Compose and Temporal configurations live under `test/scenarios/ministack/config/`; workload and tool inputs live under `test/scenarios/ministack/` and `test/scenarios/ministack/pins.json`.

The controller tears down its own containers and emulator volume after saving evidence. Treat it as a test environment, not a long-lived development database. Do not run concurrent heavy builds when measuring a runtime scenario.

A container recipe now exists for the unified runtime via `Dockerfile.unified-agent`; on this checkout, clean-host container validation is still an open delivery gate.

## Additional runtime modes

Choose one workload/fault mode per run:

```sh
python3 scripts/ministack-runtime.py --omes-mixed
python3 scripts/ministack-runtime.py --process-cut-stage after_await
```

The mixed mode consumes the frozen 40-iteration throughput command. The process-cut mode watches the actual workflow execution UPDATE, binds its operation identity and digest, and kills the selected native owner at the declared stage. Other stages are `commit_before_await` and `before_result_publication`. These are separate candidate scenarios, not completed full-acceptance evidence. Read `test/scenarios/ministack/README.md` and `scenario.json` for their exact scope and command contracts.

## Saved Omes inputs

`proof/acceptance/full-profile.json` declares the larger workload. The corpus under `proof/omes-corpus/` contains the actual saved fuzz bytes; reproducing a generator seed is not a replacement for replaying those bytes.

```sh
python3 scripts/validate_acceptance.py
python3 scripts/omes-corpus.py verify
```

These validate committed profile/corpus integrity. They do **not** start Temporal or prove live replay. `scripts/omes_workloads.py` runs the declared stages against an already available stack, with explicit source, binary, profile and evidence-directory arguments. Read `proof/acceptance/workload-runner.md` before using it. It neither creates the stack nor injects faults itself.

## Work on this site

```sh
npm ci --prefix website
npm run dev --prefix website
npm run build --prefix website
```

VitePress renders the curated Markdown in `docs/guide/`. The local preview binds loopback port `4178`; the production output is `website/dist/`. Only that generated directory is eligible for a site deployment. Runtime logs, source files and `.local/` are excluded.

For the full development loop, use the [testing guide](./testing.md) and `docs/development-loop.md` in the checkout. Version changes follow the [Temporal upgrade guide](./upgrades.md).
