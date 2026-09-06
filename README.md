<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/brand/banner-dark.svg">
  <img src="assets/brand/banner.svg" alt="Xenon — Temporal persistence. Built on object storage. In development." width="100%">
</picture>

<p align="center">
  <a href="docs/design/technical.md">Technical design</a> ·
  <a href="docs/design/verification-matrix.md">Verification status</a> ·
  <a href="https://github.com/0x63616c/xenon/issues/1">Roadmap</a> ·
  <a href="docs/brand.md">Brand assets</a>
</p>

# Xenon

> ⚠️ **UNDER CONSTRUCTION** ⚠️
> Xenon is experimental software in active development. It is not production-ready.

Xenon is an experiment in running Temporal persistence on object storage. It keeps Temporal's workflow engine, SDKs, and UI, while exploring S3 as the sole durable application-storage layer.

The project is building toward an end-to-end proof with existing Temporal SDKs, Omes, visibility, dynamic storage-node scale-out, and crash recovery from disposable local disks. The current design uses a gRPC persistence adapter, partitioned Go storage nodes, and SlateDB backed directly by S3.

## Status

Xenon has implemented persistence components and an ownership controller, with integration and end-to-end acceptance still in progress. Passing component tests and a bounded smoke scenario are useful evidence, but they do not make Xenon a supported Temporal backend.

See the [verification matrix](docs/design/verification-matrix.md) for the evidence ledger and open gates. The [Wayfinder map](https://github.com/0x63616c/xenon/issues/1) tracks the delivery work.

## What Xenon is testing

- Durable application records and ownership metadata in S3.
- A stable Xenon endpoint that forwards work to the current partition owner.
- Node movement and recovery without relying on local disks.
- Compatibility with Temporal execution, visibility, SDKs, and UI.

## Run the component proofs

Requirements: Go 1.27.1, Rust 1.94.0 through `rustup`, Python 3, Git, platform C/C++ build tools, and Docker Compose for S3-emulator proofs.

```sh
python3 scripts/prove.py go-runtime-stores
python3 scripts/prove.py owner-manager
python3 scripts/prove.py go-visibility
```

These proofs use pinned local dependencies and write evidence under `.local/evidence/`. They do not validate real AWS S3 or establish full Temporal compatibility.

## Learn more

- [Architecture](docs/design/technical.md)
- [Operations and recovery](docs/operations.md)
- [Verification status](docs/design/verification-matrix.md)
- [Development handoff](docs/handoff-autonomous.md)

## License

Xenon is available under the [MIT License](LICENSE), matching Temporal's license.
