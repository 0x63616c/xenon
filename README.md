# Xenon

Experimental S3-backed persistence for Temporal, with Go storage nodes embedding SlateDB through its official Go bindings.

Temporal retains its workflow engine, public frontend, SDKs and UI. Its persistence adapters send complete operations to one Xenon service endpoint. Any node can receive a request and forward it to the current owner of the logical storage partition. S3 stores both application data and ownership metadata; local caches are disposable.

**The persistence components and ownership controller are implemented and under integration. Full Temporal/visibility/Omes acceptance is not yet complete.** This is not a production-ready backend. The repository remains private; public release requires separate authorization.

## Current implementation

- Complete low-level execution store composition, history and task operations; namespaces, cluster metadata, legacy/fair matching, queues and Nexus endpoints.
- Atomic state/condition/outcome transactions, typed errors, durable replay, bounded admission and quarantine after uncertain native operations.
- A conditional S3 ownership directory and explicit topology activation, including node movement, crashed-process replacement and delayed-opener fencing.
- Fixed history partition placement with bounded global listing. Adding a node moves existing partitions; changing their count or order requires a separate data migration.
- Temporal execution and visibility factories, typed search attributes, bounded query fan-out and pagination. The multi-instance runtime controller is implemented; its full acceptance gate remains open.

See the [verification matrix](docs/design/verification-matrix.md) for evidence and remaining gates, and [Wayfinder](https://github.com/0x63616c/xenon/issues/1) for current work. Component test success does not establish end-to-end compatibility.

## Repeatable component proofs

Use Go 1.27.1, Rust 1.94.0 through rustup, Python 3, Git, and the platform C/C++ toolchain required by the native library. Docker with Compose is required for S3-emulator proofs. Native sources, bindings, protobuf tooling and containers are pinned in the repository.

From a clean committed checkout:

```sh
python3 scripts/prove.py go-runtime-stores
python3 scripts/prove.py go-history-routing
python3 scripts/prove.py owner-manager
python3 scripts/prove.py go-visibility
python3 scripts/prove.py go-visibility-frozen
```

The runner builds the pinned native library and node, checks exact test events, and writes source/tool/input/binary hashes and results under `.local/evidence/`. A dirty development run can never produce `proof_pass=true`. CI retains proof artifacts and also checks generated bindings, Rust checks, the Go race detector, vet, process-crash recovery and Rust/Go stored-data compatibility.

`owner-manager` creates a scoped, loopback-only MinIO environment on port 19004 with local test credentials, then removes its own containers and volume. It does not write to AWS. Real S3 remains an unverified shipping gate until an authorized bucket/prefix is supplied.

## Repository layout

```text
test/compatibility/rust/  Rust reference harness and its Cargo workspace
cmd/          Go storage node, topology administration and runtime entrypoints
internal/     Temporal adapters, node operations, ownership, routing and queries
proto/, gen/  Typed RPC contracts and generated bindings
proof/        Committed workloads, fault schedules and fixtures
experiments/  Declarative proof manifests and assertions
scripts/      Pinned builds, proof runners and scoped cleanup
deploy/       Local orchestration definitions
docs/         Contracts, decisions, requirements and evidence guidance
```

Keep implementation packages under `internal/`; add a `cmd/` entrypoint only for a runnable tool. Unit tests live beside their Go packages. Cross-process proofs belong in `proof/` with a registered manifest in `experiments/`; generated reports stay under ignored `.local/evidence/`. This is one Go module with a pinned native Rust dependency, not a collection of independently versioned services.

See the [local operations guide](docs/operations.md) for reruns, recovery boundaries and evidence handling.

The Rust harness under `test/compatibility/rust/` remains a reference for native engine behavior and stored-data compatibility. Application node code is Go. `make build` builds the Go server and its pinned SlateDB native dependency; `make test` runs Go race tests. `make check` includes the Rust reference suite. Rust remains a native build dependency, with its toolchain pinned at the root.

Start with the [technical design](docs/design/technical.md), [history placement contract](docs/research/history-partition-contract.md), [ownership protocol](docs/research/owner-manager-protocol.md), and [autonomous handoff](docs/handoff-autonomous.md). The full Temporal quickstart will be published with its verified runtime proof, rather than presented as working before that gate passes.

## License

Xenon is currently private and proprietary. No public reuse license is granted; see [LICENSE](LICENSE). Third-party components retain their own licenses. A future licensing decision remains open.
