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

## Develop and test

[`just`](https://just.systems/) runs the Xenon CLI from the latest source. It
caches the executable at `.local/bin/xenon` and rebuilds it only when executable
source inputs change:

```sh
just xenon
```

The ordinary loop is Go-first and does not start Docker or Temporal:

```sh
go test ./...
just xenon test dst
just xenon test dst --seed 42 --cases 1000
```

Replay and minimize a saved DST failure with `just xenon replay failure.json`
and `just xenon minimize failure.json`. Real native and process boundaries run
separately through `just xenon test integration`; only that command requires
Docker and MinIO.
See the [development loop](docs/development-loop.md) for the testing pyramid and
current migration status.

The source tree pins Go 1.27.1, Rust 1.94.0 and SlateDB 0.16.0. Building the
native binding requires Rust and the platform C/C++ toolchain. Historical Python
proof runners and Make targets remain during migration where unique coverage has
not yet moved to Go; they are not required developer entrypoints.

## Learn more

- [Architecture](docs/design/technical.md)
- [Operations and recovery](docs/operations.md)
- [Verification status](docs/design/verification-matrix.md)
- [Development handoff](docs/handoff-autonomous.md)

## License

Xenon is available under the [MIT License](LICENSE), matching Temporal's license.
