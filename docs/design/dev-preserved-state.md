# Local dev fixture preserved-state safety

`dev up` records successful initialization only after all nodes and workers meet
readiness and the manifest/control are readable. The saved initialization token
identifies that cluster incarnation independently of the Docker volume identity.

A later `up` starts MinIO first, then reads the configured manifest and control
and checks the saved initialization token before starting any agent. Missing or
invalid authority, a replaced token, access errors, and an unavailable endpoint
fail closed within the command deadline. This path never creates a bucket.
Agent configuration is rewritten with bootstrap and fresh-namespace permission
disabled, including when reusing previously created containers. The fixture hash
continues to represent the original declared profile.

An unsuccessful first startup remains resumable. The initialization marker is
not set merely because containers were created, nor because only some nodes are
ready. Default teardown retains both object data and the initialization marker;
explicit ephemeral teardown retires the state directory as before.

This is a local fixture safety contract, not proof against external concurrent
storage deletion, full data integrity, or a complete CLI-02 acceptance result.
Component checks cover missing bucket/manifest/control, replacement identity,
read-only reuse, unsuccessful first startup, and bootstrap removal on reuse.

Run the component checks with the pinned SlateDB native library built from
`tools/slatedb-native.json`, with its directory supplied to `CGO_LDFLAGS=-L...`
and `DYLD_LIBRARY_PATH` on macOS:

```sh
go test -race ./internal/app ./cmd/xenon -count=1
```

The local runtime image remains a separate clean-source build:

```sh
python3 scripts/build-dev-fixture.py --evidence /absolute/new/evidence-directory
```

That command builds the pinned `dev-runtime` Docker target and saves its immutable
image ID and generated fixture; it does not start a cluster. Real CLI-02 proof
still requires the declared up/workflow/Nexus/inspect/down journey.

New fixture lifecycle IDs use the `dev_` prefix and the shared 128-bit base62
identity generator. Existing 32-character hexadecimal lifecycle IDs remain
readable unchanged so their recorded Docker ownership and teardown still work.
