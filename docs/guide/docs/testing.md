# Testing

Choose the smallest tier that can establish the behavior.

| Tier | Command | Boundary |
| --- | --- | --- |
| Go unit/component | `go test ./...` | Local invariants and production seams |
| Go DST | `xenon test dst` | Virtual-time ownership, routing and faults |
| Seeded DST search | `xenon test dst --seed 42 --cases 1000` | Many deterministic schedules |
| Replay | `xenon replay failure.json` | Exact saved failing schedule |
| Minimize | `xenon minimize failure.json` | Smaller reproducer with the same fingerprint |
| Integration | `xenon test integration` | Native SlateDB/MinIO, processes and Temporal |

DST is the main correctness tool. It performs no network calls, process launches,
Docker operations or native database opens. Most ownership and timing edge cases
belong there and should complete in seconds.

The integration tier is deliberately small:

1. SlateDB and MinIO durability, reopen, exactly-once reconciliation and fencing.
2. Three Xenon processes, ownership movement, owner loss and same-address restart.
3. Temporal compatibility across owner loss and cold restart.

A timeout is a failure. Integration cleanup removes only resources created by
that invocation. MinIO does not establish AWS S3 qualification.

Older receipts remain valid historical evidence for their exact source revision.
Their Python runners and Make targets are migration inputs, not the developer
workflow. Remove them only after equivalent Go coverage exists.
