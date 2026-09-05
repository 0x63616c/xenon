# Go visibility contract

Issue #61 implements the complete pinned VisibilityStore interface, with the
existing Go Temporal query converter, typed protobuf field13, real native SlateDB
transactions, and the public custom visibility factory. Runtime application
storage and queries use no SQL. PostgreSQL is an ephemeral differential oracle.

Primary source is Temporal v1.31.2 commit
[19a774302c613da9adc4436ab14278ccdca8e0a5](https://github.com/temporalio/temporal/tree/19a774302c613da9adc4436ab14278ccdca8e0a5):
[interface](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/visibility/store/visibility_store.go),
[SQL operation contract](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/visibility/store/sql/visibility_store.go),
[unchanged upstream suite](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/tests/visibility_persistence_suite.go),
[PostgreSQL query policy](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/sql/sqlplugin/postgresql/query_converter.go).

Four fixed partitions vis-v1-0..3 use SHA256(namespace UUID bytes || run UUID
bytes), first64bits big-endian modulo4. Whole documents never span partitions.
An additional schema partition stores immutable, idempotent typed definitions
and a monotonically increasing version. The factory accepts address, index
(default xenon-visibility), and schema_partition; the actual existing Temporal
operator alias flow uses cluster metadata preallocated definitions from
sadefs.GetDBIndexSearchAttributes(nil). Those definitions must be seeded by real
cluster bootstrap, not by a fake visibility response.

START inserts only absent runs. CLOSE/UPSERT replace only smaller TaskID versions.
Document, namespace/order/type indexes and durable outcome commit together under
the existing Owner.Run gate. Type indexes use hashed scalar values and always
require canonical verification; the initial evaluator deliberately scans the
canonical namespace order index. It does not approximate predicate results.

Deletion is unconditional and retains a terminal tombstone. The delegated
coordinator decision intentionally disallows later same-run repair/reindex,
unlike SQL. Replay of an old acknowledged write cannot resurrect deletion.
No application or journal expiry is introduced here. Another delegated decision
preserves the pinned SQL/upstream empty identity case: empty input contributes
zero UUID bytes to routing but remains a distinct stored string from a real zero
UUID. Nonempty malformed UUIDs are rejected. Both identities survive reopening
and remain isolated; no successful no-op substitutes for an upsert.

Queries retain pinned grammar/alias/CHASM validation, namespace division,
three-valued missing/null logic, exact int64 values, Text lexeme policy and
DECIMAL(20,5) comparison semantics. Decimal storage rounds half away from zero;
the comparison operand stays unrounded. Times use UTC microseconds. Default
ordering is coalesced close descending, start descending, run ascending. Count
and group fanout must all succeed. Group payloads retain Temporal type metadata.
Tokens bind complete typed AST, namespace, schema version, full type/alias
context digest, partition format and last globally emitted key. Four local
streams merge in key order; byte-limited streams refill before choosing a later
global row. The live scan is not a cross-partition snapshot. A changed query or
schema rejects the old token, and an unavailable partition cannot yield a
successful partial result.

The document wire cap is2MiB; encoded pages cap at3MiB and the default response
limit remains intact. Native malformed typed attributes and wrong document
routing are rejected before writes. Existing bounded durable journal admission
still applies. These explicit limits are not production throughput claims.

Run `python3 scripts/prove.py go-visibility` from a clean checkout for native
build, all unchanged VisibilityPersistenceSuite tests through five real Go
processes, dedicated type/CHASM/byte-page/fanout assertions, native recovery and
fencing, and saved oracle checks. The process proxy in this component proof only
selects the declared partition and injects response loss; it does not claim to
prove the separate managed owner/forwarding implementation. See oracle.md for
actual ephemeral PostgreSQL verification. Full Temporal boot, SDK/UI/Omes,
managed S3 movement, crash composition and shipping gates remain independent.

Pinned PostgreSQL system-time conversion maps Go zero time to year1000 before
query/storage and maps that sentinel back to zero in responses. Xenon retains
that physical comparison/order behavior separately from returned logical time;
custom datetime attributes use their ordinary timestamp value.

The legacy Rust protobuf build boxes StoredOutcome.execution_result to avoid a
large generated enum after adding field13. This changes Rust memory layout only;
protobuf field numbers and bytes remain compatible, and the legacy Rust service
does not acquire an unimplemented visibility handler.

The executed PostgreSQL scalar oracle distinguishes generated comparison columns
from raw SearchAttributes JSON. Responses preserve original scalar payloads;
null-removal attributes are omitted as in the pinned SQL generator. Custom
datetime fractions round to microseconds using PostgreSQL's ties-to-even rule,
including second carry and pre-epoch values, while system timestamps truncate.
Double equality-index values normalize to DECIMAL(20,5) semantics before hashing,
so equivalent rounded values share candidate keys. Original double payloads and
full query operands remain unchanged. The dedicated RPC/native regressions cover
all three representations instead of assuming they are identical.

Schema additions are bounded by the encoded VisibilityResult, not merely each
individual request: the full candidate registry must fit the 3 MiB response
budget before its version or definitions are persisted. Exceeding the budget
returns a journaled ResourceExhausted result without changing the schema. GET
also rejects a preexisting oversized registry without rewriting it. The committed
schema-budget fixture performs three successful bounded additions and a fourth
aggregate overflow, then verifies unchanged definitions/version and exact rejected
outcome replay after reopening; a native pre-cap record exercises the GET guard.
