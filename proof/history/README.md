# History component proof

```sh
python3 scripts/prove.py go-history
```

Requires the pinned Go/Rust/C build prerequisites used by the other native proofs. The committed fixture and manifest drive actual adapter-to-Go-node RPC and native rollback/reopen/fencing checks. A clean report records source/input/native-library/binary hashes; dirty development runs cannot pass shipping proof.

Scope is seven history component operations on one logical partition using the explicit memory object store. Read `docs/research/history-contract.md` for the delegated reverse-query corrections, payload ceilings, and remaining full ExecutionStore/S3/Temporal gates.
