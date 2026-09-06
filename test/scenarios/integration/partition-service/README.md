# Partition service with a real registry and native engine

Run from a committed checkout:

```sh
python3 test/scenarios/integration/partition-service/run.py
```

The runner rebuilds/verifies the pinned SlateDB native source and official Go
binding with `scripts/build-go-node.py`, then reuses the digest-pinned MinIO
Compose fixture in `../native-engine/compose.yaml`. `case.json` specifies the
workload, ordered fault intent, Docker/Compose versions, and phase/test/cleanup
budgets. Go and Rust versions come from `tools/slatedb-native.json`. A fresh
checkout needs those declared toolchains, Docker, Python 3, and network access to
the pinned sources/images; unavailable tools fail the run.

Five tests call the production partition `Service`, `Step`, S3 registry adapter,
cluster control mutation functions, and native SlateDB engine:

1. Owner A reserves, opens and becomes ready. A native transaction atomically
   commits application state and an outcome record, and `AwaitDurable` completes
   before the harness acknowledges it. An old coordinator plan is prepared;
   coordinator B replaces A and assigns the partition to B. The old plan's real
   S3 conditional publication must fail. A drains and B opens the same stable
   database path, recovering both acknowledged values.
2. A's real native Open completes but a channel holds response delivery to its
   service. Coordinator replacement and movement make B ready. B durably writes
   both values, then the old completion is delivered. A must close its obsolete
   handle, never become ready, and leave B's authoritative readiness and
   acknowledged values intact. The wrapper delays only completion delivery;
   registry, database, transactions and fencing are real implementations.

3. The production shard persistence executor commits and acknowledges a shard,
   moves ownership, and replays the same operation through the new owner. Changed
   digests and exhausted outcome capacity must retain their typed errors.
4. A native Open is delayed before it executes, then released after B acknowledges
   a write. The obsolete opener fences B; A retires without becoming ready, and B
   reserves and opens again to recover its acknowledged data.

5. Cluster metadata, Nexus and namespace services share one native writer and
   journal. After movement, all three outcomes replay unchanged, the journal still
   contains three operations, and fresh read operations recover all catalog data.

Each test has separate registry/database prefixes in a fresh run-owned bucket.
The runner saves the source revision, dirty status, input/config hashes, native
build and library hashes, complete Go test JSON output, and cleanup result under
`.local/evidence/xenon-partition-*`. Required test names must execute and pass;
skips fail the proof. `--allow-dirty` is for development and is recorded explicitly.
Build/test subprocess groups are terminated on timeout/interruption. Cleanup
removes and checks only resources bearing this run's Compose project label;
cleanup failure fails the proof and cannot replace its original failure.

The test harness uses real timers solely for bounded integration polling and
budgets; the controller receives explicit Poll events. This is not deterministic
simulation, real AWS qualification, multiple OS processes, a coordinator election
failure-detector test, `cmd/xenon` acceptance, Temporal replay validation, or a
capacity claim. Coordinator loss is represented by a replacement control CAS,
not by killing a process. The shard case exercises the production persistence replay API; other persistence
families and the complete Temporal runtime remain separate acceptance gates.

The committed [four-case receipt](evidence/0d409d2-minio/report.json) records a clean
`0d409d2` run with all four cases passed, unchanged inputs and successful cleanup.
Its [test output](evidence/0d409d2-minio/tests.jsonl) includes direct recovered shard
state validation. This is native service composition evidence, not full application
activation or the ten-minute Temporal acceptance gate.

The [composed batch receipt](evidence/87690e5-minio/report.json) repeats all four
cases at clean revision `87690e5`, after renewal/checker fixes, canonical-ID
compatibility fixes, cluster metadata extraction and origin-only routing changes.
All cases and cleanup passed with unchanged inputs. The cases still exercise the
shard family and partition services; they do not establish complete runtime routing.
