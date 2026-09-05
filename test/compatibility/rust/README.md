# Native reference harness

These Rust crates preserve the original SlateDB experiments and cross-language
stored-data compatibility tests. They are not the application server: that is the
Go executable under `cmd/xenon-go-node`.

From the repository root, `make test-rust` runs this workspace. Registered
experiments continue through `python3 scripts/prove.py NAME`, including `primitive`,
`ownership`, `shard`, `crash`, and `go-shard-compat`. Build artifacts remain under
ignored root `target/`, so the supervised crash and compatibility runners find the
same binaries. Cargo.lock is preserved byte-for-byte by the relocation.

SlateDB's Go native library is separately built from pinned upstream source by
`scripts/build-go-node.py`; moving this reference workspace does not change that
library or remove the Rust toolchain requirement. Historical evidence paths are
left unchanged because they identify the original checkout that produced them.
