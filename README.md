<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/brand/banner-counter-dark.svg">
  <img src="assets/brand/banner-counter.svg" alt="Xenon — Temporal persistence. Built on object storage. In development." width="100%">
</picture>

<p align="center">
  <a href="https://0x63616c.github.io/xenon/">Website</a> ·
  <a href="docs/design/technical.md">Technical design</a> ·
  <a href="docs/design/verification-matrix.md">Verification status</a> ·
  <a href="https://github.com/0x63616c/xenon/issues/1">Roadmap</a> ·
  <a href="docs/brand.md">Brand assets</a>
</p>

# Xenon

> ⚠️ **UNDER CONSTRUCTION** ⚠️
> Xenon is experimental software in active development. It is not production-ready.

Xenon is an experiment in running Temporal persistence on object storage. It keeps Temporal's workflow engine, SDKs, and UI, while exploring S3 as the sole durable application-storage layer.

The project is building toward an end-to-end proof with existing Temporal SDKs, Omes, visibility, dynamic storage-node scale-out, and crash recovery from disposable local disks. The current runtime is one Go `xenon` agent: it embeds Temporal Server, exposes Temporal's compatible API, and owns the routed SlateDB-backed persistence service behind that API.

## Status

Xenon has implemented the combined agent, persistence components, and ownership controller, with full end-to-end acceptance still in progress. A clean three-agent component run passed active ownership movement, whole-agent crash/restart, and recovery from S3 after all local state was discarded. Passing component tests do not make Xenon a supported Temporal backend.

See the [verification matrix](docs/design/verification-matrix.md) for the evidence ledger and open gates. The [Wayfinder map](https://github.com/0x63616c/xenon/issues/1) tracks the delivery work.

## What Xenon is testing

- Durable application records and ownership metadata in S3.
- A stable Xenon endpoint that forwards work to the current partition owner.
- Node movement and recovery without relying on local disks.
- Compatibility with Temporal execution, visibility, SDKs, and UI.

## Run Xenon

[`just`](https://just.systems/) is the developer entrypoint. This runs the Xenon
CLI from the latest source:

```sh
just xenon
```

The recipe caches the executable at `.local/bin/xenon` and rebuilds it only
when Go source or module files change. Arguments are forwarded to the CLI, so
the same entrypoint runs its tools:

```sh
just xenon test dst --seed 42 --cases 1000
just xenon replay failure.json
```

## Run the component proofs

Requirements: `just`, Go 1.27.1, Rust 1.94.0 through `rustup`, Python 3, Git,
platform C/C++ build tools, and Docker Compose for the legacy S3-emulator
proofs below.

```sh
python3 scripts/prove.py go-runtime-stores
python3 scripts/prove.py owner-manager
python3 scripts/prove.py go-visibility
python3 scripts/agent-smoke.py
```

These proofs use pinned local dependencies and write evidence under `.local/evidence/`. They do not validate real AWS S3 or establish full Temporal compatibility.

## Learn more

- [Architecture](docs/design/technical.md)
- [Operations and recovery](docs/operations.md)
- [Verification status](docs/design/verification-matrix.md)
- [Development handoff](docs/handoff-autonomous.md)

## License

Xenon is available under the [MIT License](LICENSE), matching Temporal's license.
