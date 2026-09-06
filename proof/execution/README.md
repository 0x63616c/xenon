# Mutable-state proof

From a clean checkout run `python3 scripts/prove.py go-execution`.

The registered manifest builds the Go service against the exact official SlateDB native binding, runs the actual gRPC adapter test and native recovery test, and writes machine-readable evidence bound to the source commit, input hashes, native library and Go binary. The fixture declares IDs, timeouts, topology and lost-response fault. Dirty development runs cannot produce proof PASS.

The proof covers the nine-method component, typed conflicts, state/current/task rollback, history prewrite ordering, durable replay after deletion/reopen, and stale-owner fencing. This memory object-store experiment is not the full S3/Temporal/Omes shipping gate. See `docs/research/execution-contract.md` for the exact contract and explicit limits.
