# Cluster metadata and membership proof

Run from a clean checkout with the pinned Go/Rust/C toolchain:

```sh
python3 scripts/prove.py go-cluster
```

The manifest builds the Go server against the exact official SlateDB native source, reruns the shard and namespace gates, then checks cluster recovery, membership and real-gRPC manager behavior. Native resources and input hashes are recorded in the shared machine report. A dirty run is development-only, never proof. The fixed clock, workload counts and byte limits are in `case.json`.

Cluster metadata uses expected-version CAS in the same transaction as its durable outcome. All three operation families share request IDs, a durable capacity counter and the same single-owner gate. Field 4 extends the existing outcome oneof without renumbering shard field 2 or namespace field 3. Replay performs a nonempty durable barrier and cannot extend a membership lease. A timeout or unknown native error quarantines the shared owner.

Required low-level semantics follow Temporal `19a774302c613da9adc4436ab14278ccdca8e0a5`: strict expiry/heartbeat filtering, inclusive session boundary, host-filter precedence over cursor, normalized IP equality, server-time heartbeat and expiry, idempotent deletes and pruning strictly before now. Metadata pages use versioned opaque name cursors; a non-nil empty token is invalid. Membership cursors remain 16-byte UUIDs. Both page types respect the complete 3 MiB protobuf-result budget and return continuation after the last emitted row. Version overflow returns ResourceExhausted instead of wrapping.

The tests cover one winner among concurrent CAS attempts, replay after reopening and intervening mutation, cross-family ID rejection, stale-writer fencing, 100-member keyset pagination, time/filter boundaries, no lease extension on replay, strict prune boundary, and prune's intentionally ignored count field. Real gRPC exercises a dropped completed save response, retries, large-page delivery, typed errors and the pinned upstream manager's immutable-field and membership validation behavior. They are focused fixtures modeled on upstream cases, not execution of the entire upstream persistence suite.

This is an in-memory object-store experiment with a real native engine and gRPC. It does not establish S3 crash recovery for cluster operations, service forwarding, multi-node ownership movement, or Temporal boot. Those remain required shipping gates. The process server is configured for S3 through the same native URL resolver as the shard owner; no durable local disk or second application database is introduced.
