# Cluster metadata operation contract

Issue: [#27](https://github.com/0x63616c/xenon/issues/27), child of the Wayfinder map. This checkpoint implements only protobuf and the Go `ClusterMetadataStore` adapter. Mocked-wire tests do not establish storage correctness, Temporal boot, S3 durability or owner recovery.

Pinned Temporal v1.31.2 commit `19a774302c613da9adc4436ab14278ccdca8e0a5` defines the complete [low-level interface](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/persistence_interface.go), [membership fields](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/data_interfaces.go), [SQL store behavior](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/sql/cluster_metadata.go), and [PostgreSQL conditions](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/sql/sqlplugin/postgresql/cluster_metadata.go).

## Required handler semantics

| Method | Contract |
| --- | --- |
| ListClusterMetadata | Cluster-name ordered live pagination; token opaque to adapter. Invalid token is Internal. Bound complete encoded response to 3 MiB; byte truncation continues after last emitted record. Preserve nil versus non-nil empty request token. |
| GetClusterMetadata | Missing record is NotFound; preserve blob bytes, numeric encoding and signed 64-bit version. |
| SaveClusterMetadata | One control-partition transaction checks expected version (missing means 0), inserts stored version 1 or updates to expected+1. Mismatch returns false plus Unavailable; success true. Never wrap version overflow. Blob and outcome journal commit together. |
| DeleteClusterMetadata | Missing record succeeds idempotently. |
| GetClusterMembers | Filter expiry strictly after server time; heartbeat strictly after now-minus-positive-duration; session start >= supplied nonzero instant; role filtered unless All. Host equality overrides page cursor; optional filter presence matters even for empty bytes. Host IDs ordered, cursor last emitted ID. Nonempty cursor must be 16 bytes; malformed token is Internal. PageSize <=0 means no SQL count limit, but byte budget still requires continuation. |
| UpsertClusterMembership | Replace role/address/port/session for host UUID; server sets heartbeat to now and expiry to now+duration. Preserve uint16 port, IPv4/IPv6, signed nanosecond duration. No client-generated heartbeat. |
| PruneClusterMembership | Delete expired records strictly before server now. Pinned SQL ignores MaxRecordsPruned; field transported to avoid accidental wire omission. |

The [upstream manager](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/cluster_metadata_store.go) retains immutable-field comparison (false,nil) and rejection of deleting the current cluster. Do not duplicate those checks in the low-level adapter. The wire retains `applied=false` distinctly, while SQL CAS failure is false+Unavailable.

The owner selects time once per newly admitted operation; durable outcome replay must not extend a membership lease or redo pruning with a new clock. Reads need the same freshness barrier as shard reads. Prefixes and journal identities must distinguish operation families; digest alone is insufficient. Time is UTC seconds plus nanos, including Go year-1 zero. No string/float conversion of versions, durations or UUIDs. IP byte representations are preserved by the adapter; handler address equality must normalize through net.IP.String like pinned SQL.

Future handler validates UUID length, IP/port/time bounds, operation kind/field combinations, request payload limits and full response size before journaling; reject oversized records rather than store an undeliverable outcome. Use existing owner timeout/quarantine and replay protocol. No handler is implemented here.

## Executed adapter checks and remaining gates

Run `go test -race ./internal/temporal/adapter -run '^TestCluster' -count=1`. The two named tests cover every method, deterministic protobuf roundtrip/digest, optional empty filters, max-int64 version/expiry, unknown encoding values, binary blobs/tokens, IPv4/IPv6 and nanosecond timestamps, false result/CAS typed error, transport error conversion, identity-preserving retry and bounded deadline. These are deterministic contract tests with a mocked RPC client, not a native-library experiment. `scripts/generate-proto.sh` includes the new contract for CI generation-drift checking.

Required next proof: actual Go handler with concurrent CAS, missing/delete and pagination semantics, server-clock lease/expiry boundaries, lost-response replay, stale owner fencing and acknowledged recovery on S3. Then wire the factory and run upstream ClusterMetadataPersistence suites plus actual Temporal initialization. The current low-level tests do not prove any of those gates.
