# Legacy queue contract and delegated corrections

[Implementation issue #30](https://github.com/0x63616c/xenon/issues/30), child of Wayfinder #1. This implements `persistence.Queue`, not QueueV2. One existing Go node endpoint registers the queue service alongside shard and namespace services. The adapter issues complete operations; the owner applies them inside the shared admission gate, serializable transaction and durable outcome journal. Durable application data remains SlateDB on S3; the declared local proof uses the explicit memory object-store mode.

Pinned source: Temporal v1.31.2 [`sql/queue.go`](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/sql/queue.go), [`postgresql/queue.go`](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/sql/sqlplugin/postgresql/queue.go), [`sql/common.go`](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/sql/common.go), and [`namespace_replication_queue.go`](https://github.com/temporalio/temporal/blob/19a774302c613da9adc4436ab14278ccdca8e0a5/common/persistence/namespace_replication_queue.go).

## Preserved behavior

- `Init` creates normal and DLQ metadata at version 0 without replacing existing data. Xenon initializes both atomically, avoiding a partially initialized pair.
- Normal metadata update uses exact supplied version CAS and increments stored version by one. Stale/missing metadata returns logical Unavailable without quarantining a healthy owner. Both metadata reads preserve blob bytes and numeric encoding.
- Positive logical queue types use separate negative DLQ types. Messages start at ID 0; reads are ordered ascending, strictly greater than first/last-read ID and at most the upper bound. Returned message encoding is the pinned enum string.
- `DeleteMessagesBefore(x)` deletes IDs below x; exact DLQ delete is idempotent; DLQ range delete is `(first,last]`. A failed transaction cannot publish staged deletes or its journal outcome.
- DLQ continuation is signed message ID encoded as 8-byte little-endian; malformed nonempty token returns Internal. Token overrides first ID. Normal reads continue from last returned message ID.
- Full encoded result is limited to 3 MiB, including continuation, below the Go gRPC default receive limit. A byte-limited page can contain fewer than requested records; token points to the last emitted record. Empty or oversized requests fail explicitly; accepted count bounds are 1..1000 and per-message payload <=1 MiB.

## Explicit deviations from pinned SQL defects

The coordinator adjudicated these changes with independent advocate agreement and source verification, under delegated autonomous delivery authorization. This does not claim Calum personally selected the algorithms.

| Pinned SQL behavior | Xenon behavior and reason |
| --- | --- |
| `EnqueueMessageToDLQ` leaves local lastMessageID=0 on ErrNoRows, inserts ID0, then returns1. | Return actual inserted ID0. Acknowledgement must identify the stored message, including replay after lost reply. |
| `UpdateDLQAckLevel` omits supplied Version; PostgreSQL therefore checks version0 and advances to1. Every subsequent update fails. | Use supplied version CAS, just as normal metadata does. Multiple DLQ acknowledgement updates must progress and reject stale writers. |
| Enqueue allocates from maximum extant row, resetting to0 after delete-all. | Persist per-queue highwater atomically with enqueue and journal. Never reuse acknowledged IDs after deletion or reopen; otherwise readers resuming from an acknowledged ID miss new work. |

There is no migration from old SQL storage in this checkpoint. Queue keys are new `v1/queue/<signed type>/meta`, `/highwater` and `/m/<16-digit hexadecimal ID>`. Overflow fails ResourceExhausted before acknowledgement. Metadata, messages and allocation share the control partition; outcome oneof field5 is reserved for queue (shard2, namespace3, cluster4). Journal capacity is finite and fail-closed; safe retention remains a separate gate.

## Repeatable checks and limits

`python3 scripts/prove.py go-queue` builds the exact pinned official SlateDB library and Go node, then runs the committed tests from `experiments/go-queue.json` and `proof/queue/case.json`. Real RPC tests cover every method, two successive normal/DLQ version updates plus stale rejection, dropped completed enqueue reply and unchanged retry identity, no duplicate enqueue, boundary deletes, all-record deletion followed by monotonic allocation, and large-message pagination. Native tests reopen after delete-all and replay, prove allocation survives, stage a delete then abort the journal callback, verify both data and journal rollback, and reject replay on a fenced old owner. Cancellation is normalized whether remote status or local timer wins.

These checks do not constitute SQL differential execution, a real S3 proof, process-kill recovery, ownership forwarding, QueueV2 support or full Temporal boot. Those remain explicit shipping gates. No request is reported successful by a placeholder handler.
