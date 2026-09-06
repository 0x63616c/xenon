# Identity and registry foundation (#109)

This slice implements new typed identities and registry contracts only. Existing
production callers still use `directory`/`ownership`; their formats and behavior
are unchanged. No S3/filesystem implementation, backend durability, production
wiring, or migration acceptance is claimed by the unit tests.

## New contract

New IDs use prefixes `clu`, `nod`, `inc`, `prt`, `op`, `trn` and 22 base62 digits
in `0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz` order. The
fixed width includes leading zeroes; decoded values above 128 bits are invalid.
`identity.Generator` reads exactly 16 bytes with `io.ReadFull`, propagates entropy
failure, and can receive a recorded reader. Constructors validate injected sources.
Each named type has `Validate`; callers must validate casts at ingress. No new
randomness affects placement or fault schedules. Existing IDs are not accepted by
this new codec and must continue through their existing versioned readers.

`registry.Write.Body` contains the exact canonical application payload bytes.
`NewWrite` copies them and hashes domain-separated, length-framed key, expected
condition/version, and payload. Empty expected version means create-only; nonempty
means exact replacement. `ValidateCreate`/`ValidateReplace` enforce this distinction
before dispatch; replacement with an empty version is invalid. Versions remain
opaque, including non-UTF8 bytes. The registry does not normalize application JSON.

`Encode` stores a format-1 publication envelope containing the key, original
condition, transition, digest, and payload. Expected-version and payload bytes use
base64; the digest uses lowercase hex. `Record.Body` is that envelope, not the
payload. `Decode` validates the key, nonempty backend version, identity, digest and
exact canonical encoding, rejecting duplicate fields and other JSON aliases.
Empty payloads use one representation. See the executable `ExampleEncode`.
Backends must bound reads, enforce namespace containment, copy request/response
bytes and qualify their durability before implementing the Store contract.

`Reconcile` reports publication only when transition and digest match. Reuse of
the same transition with another digest is invalid. An unchanged original version
(or absence for create-only) permits retry of the same write, without establishing
historical nonpublication. A later transition, failed read or corrupt read retains
`UnknownOutcome`; a subsequent SDK conflict cannot erase earlier ambiguity.
Classify outer `UnknownOutcome` before inspecting wrapped causes, which may include
`Conflict` or cancellation. Retained application receipts, when needed, belong in
the authoritative record; this is not a history journal. Backends must reject or
reconcile reused transitions and ensure accepted changes never reuse versions,
including identical-payload replacements. These are pending adapter contracts,
not guarantees provided by an interface alone.

## Existing identifier and persisted-reference inventory

Inventory baseline: `0fd64b1`. Sources below are unchanged by this slice except for
the additional legacy-read test. The table groups producers by semantic identity;
repeated operation families share the same generation and persistence path.

| Identity/reference | Current producer and reader | Required compatibility |
| --- | --- | --- |
| Cluster and node names | `internal/agent/config.go` accepts configured strings; `internal/ownership/topology.go` stores node keys/assignment references; adapter shard store carries configured cluster name | Preserve existing names and upstream Temporal cluster metadata. Allocate a separate typed resource identity only with an explicit mapping; never silently reinterpret user configuration. |
| Process incarnation | `internal/ownership/manager.go` uses `uuid.NewString`; directory/topology validators use `uuid.Parse` | Keep UUID format-1 readers. New process IDs may use `inc` only after authority cutover excludes old participants. |
| Ownership and topology transition | `internal/directory/directory.go` and `internal/ownership/topology.go` produce UUID strings; topology has an injected source | Preserve existing transition references/receipts. Fresh `trn` values belong to the new envelope/controller version after cutover. |
| Logical partitions and database paths | configured names in topology/agent storage startup; `internal/adapter/history_partitions.go` routes by ordered partition list; directory uses hex(partition)+`.json` and stores exact `DataPrefix` | Preserve names such as `history-0`, list ordering, physical prefixes and pagination references. Never derive new paths from a newly generated `prt` ID or rehash a populated partition set. |
| Persistence operation identity | `internal/adapter/{shard,execution,executiontasks,history,historytasks,matching,metadata,cluster,nexus,queue,queuev2,visibility}.go` generates UUID once per request; `internal/node/shard.go` accepts `[a-zA-Z0-9-]{1,128}` | Existing wire validator rejects `_`, so production cannot emit `op_` yet. Add explicit protocol/read compatibility before changing producers; retain operation IDs across retries. |
| Replay and history suboperations | `internal/replay/replay.go` and `internal/node/journal.go` store `v1/outcome/<id>`; `internal/node/execution.go` derives `<id>-h-<index>` | Preserve old outcome keys and derived suboperation identity. Future typed operations need a reviewed deterministic parent/child mapping; do not allocate a fresh child ID on replay. |
| Read barrier nonce | `internal/node/shard.go`, `internal/node/outcome_usage.go` write UUID bytes/string to fixed barrier keys | This is an internal changing value, not a resource reference. Keep its durable-barrier semantics; no ID migration is needed merely to conform to prefixes. |
| Evidence invocation and fault controller IDs | `internal/rpctrace/{trace,observer}.go` generates UUIDs; `internal/processcut/cut.go` validates UUID session and generates incarnation | Preserve saved traces/cut schedules. Version evidence schemas before changing these IDs; do not infer runtime resource IDs from diagnostic strings. |
| Harness run/container IDs | UUID-derived labels in `scripts/{prove,crash-proof,agent-smoke,ministack-runtime,prove-go-bindings,check-ministack,omes-signal-proof,visibility-oracle}.py`, `scripts/upgrade_state.py`, `scripts/recorder_lifecycle.py` | Preserve recorded resource handles and cleanup ownership. These run IDs need their own explicit vocabulary/version decision; the six runtime types must not be overloaded. |
| SDK probe workload IDs | `cmd/xenon-sdk-probe/nexus_readiness.go` generates a UUID nonce used in test workload names | Treat as user-visible Temporal test inputs and retain in saved replay; not a Xenon runtime identity migration. |
| Temporal/user identities | UUID parsing in adapter metadata/history/nexus/execution and visibility; workflow IDs, run/namespace/tree/branch IDs, Temporal history node IDs, task queue names and tokens | Preserve upstream/user encoding verbatim. Numeric history NodeID is not Xenon NodeID. No prefix rewrite. |
| Frozen Rust references | `test/compatibility/rust/crates/slatedb-probe/src/ownership.rs` uses UUID transition/barrier values; Rust node validates old operation alphabet | Preserve compatibility fixtures and their IDs. Qualify any future wire-format expansion against these references; do not rewrite evidence inputs. |
| Non-resource values | SHA-256 digests, ETags, generations, revision/renewal counters, range IDs, incarnation-local effect sequence numbers | Keep their domain-specific representation; these are not candidates for prefixed resource IDs. |

## Compatibility fixture and migration sequence

`internal/directory/legacy_identity_test.go` feeds saved format-1 JSON through the
existing production `Read` path with a read-only S3 double. It asserts UUID
transition/incarnation, `node-a`, `history-0`, exact data prefix, generation, and
hex-encoded object path are unchanged and forbids writes. This is codec/read
compatibility evidence, not a backend durability test or an executed migration.

Before production adopts the new types/envelope, implement the step-3 versioned
offline authority cutover from the delivery plan. Stop and exclude old participants
with an enforceable access/launch boundary, preserve existing paths/partition
references, persist migration identity/phase, and reconcile each ambiguous CAS.
Resume or halt safely after crashes. Restarted old managers must be refused or
isolated before the new authority can publish. Do not claim mixed-version rolling
coordination. Decode legacy format explicitly rather than casting arbitrary old
strings to new typed IDs. Keep populated-state, interrupted/resumed migration,
legacy replay, old pagination, and unchanged acknowledged data tests as required
acceptance gates before old readers are retired (step 7 completes remaining IDs).

## Reproduction and remaining gates

From a clean checkout, with the Go toolchain pinned in `go.mod`:

```sh
GOENV=off GOWORK=off GOFLAGS=-mod=readonly GOTOOLCHAIN=go1.27.1 go test -race -count=1 ./internal/identity ./internal/registry ./internal/directory
```

Inputs, negative controls and expected results are committed as deterministic Go
fixtures; no service, random fixture generation, credential, or native library is
needed. The command exits nonzero on assertion/race failure and creates no owned
external resources requiring teardown. Record the exact commit, Go version and
command result in the linked issue at integration. The isolated implementation
and independent review are distinct from an integrated-head rerun.

Next slices implement the real S3 adapter using existing conditional SDK code,
shared backend contracts, filesystem stable-lock/fsync/read recovery, and clock
helpers at actual service consumers. Legacy `directory.publish` can currently
classify an unrelated later record as conflict after ambiguous publication; do not
copy this into the new adapter. S3/filesystem durability, no-ABA, aggregate retries,
NFS/SMB qualification and persisted migration remain unproven here.
