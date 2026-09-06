# Testing and evidence

Different tests answer different questions. A parser control, a native engine test and a real Temporal recovery run cannot be combined into an invented whole-system pass.

## Choose the smallest relevant gate

| Stage | Repository-root command | What it establishes |
| --- | --- | --- |
| Build | `make build` | Go node and pinned native dependency compile. |
| Harness controls | `make test-harness` | Supervision, manifests and failure accounting behave as asserted. |
| Go packages | `make test` | Package tests run with the race detector. |
| Registered component | `make proof CASE=owner-manager` | That manifest's exact engine/emulator assertions execute. |
| Real smoke | `make smoke` | The declared SDK, Omes20, UI, movement and cold-recovery scenario executes. |
| Frozen mixed workload | `python3 scripts/ministack-runtime.py --omes-mixed` | Runs the declared throughput component against the real stack. |
| Exact workflow cut | `python3 scripts/ministack-runtime.py --process-cut-stage after_await` | Attempts the declared native cut during an actual workflow UPDATE and verifies recovery. |
| Saved fuzz soak | `make fuzz-soak` | The declared actual-stack corpus soak runs without injected faults. |
| Combined local checks | `make check` | Layout, harness, Go race and Rust reference checks run in sequence. |

These commands are implemented entrypoints. They are not a statement that smoke, soak or full acceptance currently pass. Read [verification status](./status.md) for the observed results and [local development](./development.md) for prerequisites.

## Preserve the complete contract

Component manifests in `experiments/` and `test/scenarios/*/manifests/` bind exact inputs, command arrays, expected test events and timeouts. The ministack owns its case, configuration, pins and UI fixtures under `test/scenarios/ministack/`; other workloads and the frozen corpus remain in `proof/`. The native reference workspace lives under `test/compatibility/rust/`; old receipts keep their original paths and revisions.

A new behavior needs a repeatable test that would fail if that behavior regressed. For transaction conditions, use the pinned persistence contract and real handler. For durability or fencing, exercise the native engine and object store. For cross-service Temporal behavior, use the real stack.

Use observed fault barriers where a particular interleaving matters. A seed fixes inputs, not operating-system scheduling or S3 latency. Record the actual operation, trigger, source, configuration, binaries and state assertions.

## Read a receipt

A clean proof records its source revision, input and tool hashes, executed commands, assertions and cleanup result. Reports live under `.local/evidence/`. `proof_pass: true` applies only to that declared proof scope.

Dirty development results remain development results. A timeout is a failure or unknown outcome. A missing trace footer is incomplete telemetry. A failed cleanup cannot be ignored because an earlier assertion succeeded.

The smoke's twenty simple Omes workflows are not the full acceptance workload. The larger profile requires 100 simple, 40 throughput and twenty saved fuzz inputs in both fault profiles, plus movement, visibility and measurement gates. Profile validation is not runtime execution.

## Work efficiently without hiding failures

Do not run multiple heavyweight stacks on one host while measuring them. Reuse verified compiler caches and pinned images, while giving each run isolated identities, local directories and scoped object-store state.

When a test fails, retain the original receipt. Fix the demonstrated defect, commit the correction, and rerun the affected gate. Do not raise a deadline after observing a failure merely to obtain a pass.

The detailed repository procedure lives in `docs/development-loop.md`. Changes to the Temporal pin additionally require the [upgrade procedure](./upgrades.md).
