# Execution operation admission

## Delegated decision

The coordinating agent accepted explicit `Writer.BeginOperation`,
`Operation.Begin` and idempotent `Operation.Release` after independent adversarial
review on 2026-09-06. This is a delegated implementation decision under the
repository authorization, not personal approval of this API by Calum.

Existing `internal/node/execution.go` holds `Owner.run` admission across root
journal lookup, independently durable history prewrites, the root mutation, and
an optional logical-error journal after root rollback. History prewrites survive
a later execution guard failure. A root replay skips those prewrites, preventing
resurrection after deletion. Releasing admission between these transactions
would change the existing concurrency semantics.

The opaque engine therefore needs one admission capability that can contain
multiple transactions. A persistence-service mutex would not exclude ordinary
writer transactions, durable reads or shutdown. A callback API would still need
native-worker retention while adding application callbacks to the engine seam.

## Contract

The native implementation reuses its existing writer gate. Ordinary `Begin`
creates an implicit operation, released after abort or native durability.
Explicit operations keep that gate between transactions until `Release`.
Only one child transaction or native receipt can be active. A second child
returns `ErrBusy`; it never reacquires the outer gate. Successful abort or native
durability permits the next child. `ReadDurable` uses ordinary `Begin` and is
excluded for the entire explicit operation.

`Release` first invalidates the capability, rejects later children, and requests
abort of an idle transaction. It is idempotent and does not wait for busy native
work. A racing child is either counted before invalidation or rejected. Only the
native worker can dispose an active native handle. A submitted receipt retains
admission until its actual native durability completion, including when the
caller has stopped waiting. Cancellation after dispatch or an unknown commit
outcome retires the writer, preventing subsequent children. A queued caller's
cancellation does not retire the writer. `Close` retires before waiting on the
same outer gate and cannot destroy an active transaction or receipt.

A caller must defer `Release` immediately after obtaining an explicit operation.
It must still await each commit receipt before acknowledging that transaction.
Release does not roll back transactions that were already submitted or durable.

## Qualification and migration boundary

`internal/partitions/slatedb/operation_test.go` exercises real native transactions,
manual WAL flushing, blocked native calls, concurrent release and child admission,
and read/close exclusion. The existing native lifecycle suite remains applicable
to implicit operations. These tests qualify this adapter lifecycle against the
pinned binding; they are not full multi-node or real-S3 acceptance evidence.

The execution/history extraction is a subsequent batch. It must preserve the
private persisted child journal identities `root-h-index`, without applying
public canonical operation-ID validation to those historic keys. It must also
preserve history's explicit remote-durability scan semantics; the current opaque
scan default is memory durability. No journal namespace or scan behavior is
changed by this admission capability.
