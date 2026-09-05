# Execution task remainder proof

Run `python3 scripts/prove.py go-executiontasks` from a clean checkout. The manifest builds the pinned native library and Go binary, runs all declared test names, and emits source/config/tool/artifact hashes with a machine-readable result. Dirty developer runs cannot emit proof PASS.

Five commands cover the actual DLQ RPC, unchanged pinned Temporal task suite (15 tests), native batch rollback/reopen/fence and 3 MiB pages. The fixture declares the response-loss fault, IDs, limits and upstream random seed. Source scope and the delegated MaxInt64 emptiness correction are in `docs/research/executiontasks-contract.md`.

A full low-level ExecutionStore is composed, but the proof remains a memory-object-store component test. Temporal boot and the S3/Omes/multinode shipping gates remain open.
