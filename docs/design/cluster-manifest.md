# Immutable cluster manifest

Before registering an agent or opening a storage partition, call
`ownership.EnsureCluster` with the configuration's cluster name and history shard
count. The manifest is stored at `<topology-prefix>/cluster.json` in customer S3.
It contains exactly:

- `format: 1`: this manifest schema.
- `cluster`: embedded Temporal cluster name.
- `history_shards`: the fixed Temporal history shard count.
- `layout_version: 1`: the current fixed logical storage-partition layout.
- `wire_version: 1`: the current persistence protocol identity.

These are contract identities, not minimum/maximum supported Xenon releases.
They do not prove rolling upgrade or stored-engine-format compatibility.

A missing manifest requires explicit bootstrap. Creation uses S3
`If-None-Match: *`; a competing bootstrap cannot replace the winner. Every create
is followed by a bounded readback. A lost response succeeds only if the exact
manifest is observed before the operation deadline; unresolved outcomes return
`ErrUnknown`. Identical existing configuration can join without bootstrap.
Mismatches return `ErrConflict`; malformed, unknown-field, oversized or unsupported
manifests are rejected without rewriting them. There is no automatic migration.

This gate must precede `Join`; it prevents a misconfigured agent from changing
ownership first. Cluster manifests must remain immutable for the lifetime of the
prefix. Removing or editing one manually can defeat this protection. An older
prefix without a manifest requires a separately reviewed adoption/migration that
checks its existing Temporal metadata before enabling this gate; bootstrap is
only appropriate for a new prefix, not a way to infer legacy settings.

## Native-independent verification

```sh
go test -race internal/ownership/topology.go internal/ownership/manifest.go internal/ownership/manifest_test.go
```

This explicit file set tests S3 conditional operations without linking the
ownership package's native engine runtime. Tests cover matching and competing
concurrent bootstraps, cluster/shard mismatch, missing bootstrap permission,
corrupt/unknown data, lost responses and canceled readback. The in-memory S3 double
models conditional creation; real-S3 conditional semantics remain a separate gate.
