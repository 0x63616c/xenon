# Native partition engine contracts

Run from a clean checkout:

```sh
python3 scripts/prove.py native-engine-contracts
```

The manifest builds and verifies the pinned official SlateDB Go v0.16.0 binding
against native commit `3fb9e8abab0c9f5833f0c154140ceef009fea02a`, Rust 1.94.0 and
Go 1.27.1. The existing build runner recreates missing native source/artifacts.
The thin fixture starts the digest-pinned MinIO image with Docker 29.4.0/Compose
5.1.2 on a dynamically allocated localhost port and isolated volume/network,
runs all eight native contract tests with the race detector, and removes its
scoped resources on success/failure/interruption. The parent allocates the project
identity before launching the child and performs a bounded 60-second fallback
Compose cleanup even if the child is killed before its own finally block. Parent
receipts retain the project ID and keep cleanup failures separate from the primary
failure. An actual SIGKILL child control checks that fallback path. No cloud credentials or spend
are required. Setup/run/cleanup logs, source revision, tool/native hashes and
input hashes are retained by prove.py under `.local/evidence/<run-id>/`.

The committed `case.json` names the exact fault transitions. Manual native WAL
flush disables periodic flush in the pending-durability cases: the caller stops
waiting, admission stays excluded, Close cannot destroy the database, then
explicit WAL flush completes the original mutation. Recovery must include both
application state and outcome. Tests also cover bounded transactional scans,
abort/delete, foreign receipts, a nonempty read barrier, terminal fencing and
recovery after a finite delayed stale opener. They exercise actual Rust/S3 APIs;
real native/OS timing is deliberately outside deterministic simulation.

Four committed source mutations execute the same native tests against isolated
S3 buckets in the same emulator: forging successful fenced commit/durability,
bypassing the durable-read barrier, omitting the replay outcome Put, and reporting
Close success while the native mutation is still active. Each must fail its named
assertion; compilation errors, unrelated failures, or an unexpected pass fail the
control runner. Original and mutated source hashes, exact commands and failing
logs are retained under the parent evidence directory's `negative-controls/`.
The stale-writer regression stages a real user transaction before takeover, then
attempts Commit after replacement Open and verifies the mutation is absent both
at the replacement and after another recovery.

The public engine accepts direct `s3://` object-store URLs. Open metadata must
include stable path, typed partition/reservation/incarnation IDs and nonzero
assignment/generation. The future controller validates the authoritative registry
record; this seam does not substitute registry metadata for native fencing.
Open is synchronous after native dispatch because the binding cannot cancel Build.
Cancellation during Build does not complete the Open effect: native Build must
return and any resulting writer must finish Close before the call returns an
unknown outcome. Cleanup failures are joined to cancellation, not discarded.
The driver must retain its owned pending call through completion; it may run that
call in its own worker and watchdog it, but must not start a replacement open for
the same reservation merely because the context expired. No hidden worker can
outlive the completed Open effect.

Transactions retain admission until Abort or native durability. Callers own an
idle transaction and must eventually Commit/Abort it. Abort returns `ErrBusy`
while a native transaction call is active, and `ErrTransactionDone` once commit
is dispatched. Caller cancellation during native work retires that writer; the
worker drains and releases handles without delivering a late success. Commit
returns its receipt before durability and can return that receipt with an unknown
outcome; AwaitDurable reconciles the same mutation without admitting new work.
If native work never completes, process termination is the containment boundary.

This is bounded MinIO/native contract evidence, not AWS qualification, production
service/controller cutover, process-kill testing, or 100-node capacity evidence.
No new application persistence path is enabled by this batch.
