# Frozen visibility seed concurrency regression

The actual runtime at ed7cd1e failed before movement: command-26 exhausted the
probe's 15-minute context during sequential seeding. Its receipt is
20260906T004954Z-xenon-ministack-bce1ec27f0d8; no final probe receipt existed.

Seed the same deterministic 2,000 records using one serial worker per each of
four computed visibility partitions. Keep the shared 15-minute context and
existing 30-second adapter invocation limit. Join all workers on errors, without
canceling healthy admitted writes. Failure receipts report only acknowledged
counts, never successful verification. This change is not a measured runtime
speedup or a passed movement gate.

Repeatable standard-library race controls (no native build required):

```
go test -race cmd/xenon-visibility-probe/seed.go cmd/xenon-visibility-probe/seed_test.go
```

Controls require all four workers to enter before release, exactly one write per
record, full peer completion after one worker fails, precise partial counts, and
zero admitted writes under a pre-canceled context. The existing full public
visibility fixture and runtime verification remain required.
