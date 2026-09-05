# Repeatable primitive experiments

These manifests define bounded **actual SlateDB engine tests against an in-memory object store**. They do not demonstrate the full Xenon/Temporal system or real S3 semantics. Fault ordering and workload inputs live in the named committed Rust tests; manifests declare the expected test names, assertions, backend, exact configuration inputs and outer deadlines.

From a clean checkout of the commit under review, with Git, Python 3.9+ and rustup installed:

```sh
python3 scripts/prove.py primitive
python3 scripts/prove.py ownership
```

`rust-toolchain.toml` selects the compiler. `Cargo.lock` and exact direct dependency versions select the engine and libraries; the runner uses `cargo test --locked`. First execution may download the pinned compiler and dependencies. Ownership requires the committed ownership module; a missing module or zero matched tests fails, never skips.

Each command writes `.local/evidence/<UTC>-<experiment>-<id>/result.json`, the resolved manifest and raw command logs. Results include commit SHA, clean/dirty status, SHA-256 hashes of declared source/config inputs and logs, tool versions, actual Python interpreter, OS/architecture, commands, durations, timeouts, expected tests, assertions and scope limitations. Output test names and exact passing/nonignored test counts must match. Nonzero exit, timeout, missing input or changed checkout fails. Timeout terminates the command's process group. No shell commands or arbitrary executables are accepted from manifests.

The default rejects dirty checkouts. `--allow-dirty` is explicitly for development and marks the result `development-passed` with `proof_pass=false`, never a proof pass. Clean verified runs alone set `proof_pass=true`. To reproduce a report, check out its `commit`, confirm the input hashes match, and rerun the named manifest. Logs stay local and ignored by Git; publish reviewed reports with a ticket or PR when needed. The runner does not copy or print environment variables and removes inherited AWS credentials from subprocess environments.

Existing `deploy/compose.yaml` and `scripts/probe-local.sh` provide a separate pinned S3-emulator graceful-reopen smoke. It is not a manifest-backed ownership proof and is not required by these commands. Full declarative Temporal/SDK/UI/Omes topology and external-S3 validation remain later delivery gates.

Runner self-checks:

```sh
python3 -m unittest discover -s scripts -p 'test_prove.py'
```

This is host-native reproduction, not a hermetic container build. OS/kernel/architecture and verbose compiler identity are recorded; matching commit and input hashes alone does not promise byte-identical artifacts across hosts. The runner rejects ambient Cargo `config`/`config.toml` files in the checkout, its ancestors and `CARGO_HOME`, recording only their hashes. Use an environment without those overrides. Inherited compiler flags and wrappers are removed; native system linker/toolchain differences remain a declared boundary.
