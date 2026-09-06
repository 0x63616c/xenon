# Operations and recovery

The current implementation is an experimental, explicitly administered system. These notes explain its contracts and test procedures; they are not a production runbook or availability guarantee.

## Keep durable and disposable state separate

S3 holds both SlateDB application state and conditional ownership metadata. Each partition has a bound engine data prefix. Topology and directory prefixes must not overlap any engine-owned data prefix, and different partition data prefixes must be disjoint.

Local caches and runtime directories can be replaced. That does not authorize deleting S3 objects or restoring an arbitrary old prefix. Normal SlateDB maintenance can remove objects inside its owned prefix. Use a dedicated test bucket/prefix for experiments and preserve the pinned fencing-safe maintenance configuration.

## Activate a process, then assign work

Each Xenon node announces a fresh process incarnation. An administrator conditionally publishes topology that activates that exact incarnation and assigns partitions to it. Reusing a member name or address does not let a new process inherit an old activation.

The desired owner reserves a new directory generation, opens SlateDB once for that attempt, and publishes READY conditionally. It then checks fresh activation and directory authority inside every operation's admission gate. A stale route or READY record alone does not authorize service.

Adding a node and moving a partition are explicit topology operations. The membership loop also drives an automatic S3-CAS heartbeat, and peers remove unchanged failed members when heartbeat suspicion expires. Automatic placement rebalancing remains policy-driven; the runtime keeps explicit topology intent and deterministic routes.

## Handle uncertainty without guessing

A lost response may follow a committed mutation. Retry with the same operation identity and digest so the durable journal can return the recorded outcome. Giving the retry a new identity can turn one logical operation into two.

A native timeout is not permission to destroy its resources while work is still executing. The owner quarantines the handle, rejects new admission, and retains in-flight resources until completion. Recovery obtains a fresh reservation; an obsolete attempt must not be reused.

A delayed stale opener can fence a newer writer. The protocol recovers through fresh activation checks, reservations and engine fencing. Tested finite delayed contenders do not imply progress under infinitely recurring ownership changes.

## Observe real work

Managed nodes can expose `GET /outcomes` when `XENON_METRICS_LISTEN` is configured. The response binds counters to node and process incarnation and includes per-partition journal capacity/accounting information.

`local_dispatch.successful_operations` increments only for locally executed persistence responses with a successful result. Metrics scrapes and forwarded requests do not inflate it. Compare counters for the same process incarnation when proving a new node served traffic.

`XENON_MAX_OUTCOMES` bounds retained journal entries. Automatic outcome collection is not implemented. Monitor remaining capacity and incomplete legacy accounting; do not market an indefinite-retention capacity claim from a short test.

## Diagnose a failed proof

1. Preserve its original `result.json`, command logs, inputs and binary hashes.
2. Identify the first failed assertion or command, including its exact deadline.
3. Separate preceding passes from gates that were never reached.
4. Correct a demonstrated defect with a committed regression or explicit prospective configuration change.
5. Rerun from a clean revision. Never relabel the original failed report as passed.

The current cold restart proves why a TCP listener is not enough readiness. The controller reads existing cluster metadata through the actual Xenon ingress before launching Temporal. It retries only transient availability/deadline errors within its declared 60-second window. Missing or malformed metadata is an immediate failure.

That fixed startup path has passing receipts in the latest implemented runs. The full Omes + visibility + fault profile remains open; see [verification status](./status.md) for historical and current receipts.

## Cleanup and external boundaries

The ministack controller saves scoped Compose logs and stops its own process groups, containers and volume. Check cleanup errors in the receipt. Do not use broad Docker prune commands or delete unrelated worktrees, caches or evidence to recover a test.

The smoke is a MinIO experiment. Real AWS credentials stay external to committed configuration, and real S3 writes require an authorized bucket/prefix. Hosted CI and real-AWS validation remain independent shipping gates.
