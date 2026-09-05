# Legacy queue proof

From a clean checkout with pinned Go 1.27.1, Rust 1.94.0, a C compiler, Git and Python:

```sh
python3 scripts/prove.py go-queue
```

The manifest builds the official native library and the single Go node binary, records hashes and exact test results, and runs actual gRPC plus native recovery/rollback tests. The fixture declares the dropped completed DLQ enqueue response. Missing fault observation fails the test. Evidence is written under `.local/evidence/`; dirty development runs cannot produce proof PASS.

This is a bounded memory-object-store proof for legacy Queue. Read `docs/research/queue-contract.md` for three explicitly adjudicated deviations from pinned SQL defects and remaining S3/Temporal/QueueV2 gates.
