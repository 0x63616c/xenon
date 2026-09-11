# Local development

Xenon uses a conventional Go testing pyramid. The ordinary edit loop is:

```sh
just xenon
go test ./...
xenon test dst
```

`just xenon` invokes a dependency-free Go bootstrap. It fingerprints the current
working-tree source, verifies the pinned SlateDB source and Go binding, builds a
missing or stale native library, embeds the native commit and artifact hash in
Xenon's build metadata, and reuses valid native and binary caches. Relevant
source edits invalidate the executable cache automatically.

Run a larger deterministic search explicitly:

```sh
xenon test dst --seed 42 --cases 1000
```

A failure reports its seed and artifact. Reproduce and reduce it with:

```sh
xenon replay failure.json
xenon minimize failure.json
```

These commands use virtual time and do not launch Docker, MinIO, Temporal or a
native SlateDB database. Use `xenon test integration` only when changing a real
native, process or Temporal boundary. It runs three bounded journeys and owns its
temporary resources.

The repository pins Go **1.27.1**, Rust **1.94.0** and SlateDB **0.16.0**.
Building the native binding requires Rust and the platform C/C++ toolchain. The
explicit integration tier additionally requires Docker. AWS S3 qualification and
long soaks belong to release work.

Historical Python proof controllers and Make targets remain only where unique
coverage has not yet migrated. Do not use them as the standard development
interface or add new coverage to them. See the repository
[development loop](https://github.com/0x63616c/xenon/blob/main/docs/development-loop.md) and
[verification status](./status.md).

## Website

```sh
npm ci --prefix website
npm run dev --prefix website
npm run build --prefix website
```

The local preview binds loopback port `4178`; production output is
`website/dist/`.
