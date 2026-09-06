# Measured 16-shard grouping comparison (2026-09-06)

Clean measurement source: `017b1ca73ed4c16801da505cc4929f488d755949`. All six cases passed, each recovered all 256 acknowledged outcomes and final shard states after takeover and reopen. Scoped MinIO cleanup passed. The three supervisor/percentile controls also passed.

Raw evidence: [result.json](evidence/20260906-b893c28f7ceb/result.json), accompanied by exact per-operation JSONL, RSS/CPU samples and pinned native build receipt in that directory. See [measurement boundaries](README.md).

| Physical writers | Repeat | Durable ops/s | E2E p50 / p99 (ms) | Own durable p50 (ms) | Idle RSS (MiB) | Loaded peak RSS (MiB) | Idle RSS delta (MiB) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 9.83 | 1630.48 / 1640.66 | 101.65 | 40.52 | 43.41 | 24.70 |
| 4 | 1 | 39.42 | 406.86 / 440.04 | 101.48 | 43.81 | 45.50 | 27.95 |
| 16 | 1 | 111.46 | 100.73 / 719.99 | 100.26 | 49.52 | 53.36 | 33.70 |
| 16 | 2 | 157.58 | 101.35 / 167.83 | 101.21 | 49.16 | 52.91 | 33.34 |
| 4 | 2 | 39.52 | 406.78 / 421.40 | 101.49 | 43.41 | 45.09 | 27.38 |
| 1 | 2 | 9.83 | 1630.03 / 1639.29 | 101.43 | 40.42 | 43.19 | 24.38 |

Baseline process RSS was 15.81–16.05 MiB. The larger writer count modestly increased process RSS in these cases; this is shared process/native allocation plus in-process metering, not a per-database memory reservation or a whole-server footprint.

The dominant measured effect is admission queueing: own-write durability stays near 100 ms while 16 logical workers share one, four or sixteen serialized writer gates. The pinned native default sets `flush_interval = 100 ms` (`slatedb/src/config.rs:1104` at native commit `3fb9e8abab0c9f5833f0c154140ceef009fea02a`). This supports investigating the gate/flush policy when evaluating consolidation; it does not establish a new batching policy or safe concurrency change. Both 16-writer repeats are retained: one had a substantially slower tail and lower aggregate throughput.

| Physical writers | Repeat | Idle 2s requests | Loaded requests | Loaded request / response body bytes | Native takeover Open median (ms) | Takeover all + verify (ms) | Reopen all + verify (ms) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 35 | 338 | 143312 / 33390 | 350.86 | 454.15 | 136.17 |
| 4 | 1 | 51 | 336 | 143312 / 32598 | 230.68 | 1198.37 | 703.33 |
| 16 | 1 | 98 | 334 | 143312 / 31698 | 62.87 | 2965.51 | 2427.12 |
| 16 | 2 | 96 | 312 | 143312 / 22770 | 58.95 | 2292.12 | 2303.95 |
| 4 | 2 | 52 | 332 | 143312 / 30942 | 110.96 | 663.11 | 580.13 |
| 1 | 2 | 35 | 338 | 143312 / 33390 | 345.37 | 457.38 | 134.08 |

Request snapshots include native background work. The active interval is shorter with more physical writers, so active request totals are not a fixed-time background-cost comparison. All cases sent the same 143,312 request body bytes during the loaded phase. The two-second idle interval immediately after opening is startup/short-WAL evidence; no long-run compaction or steady maintenance cost is established.

Takeovers/reopens run physical writers sequentially. Verification is one ReadDurable barrier per physical writer, with 272 keys in the single-writer layout and 17 keys per writer in the 16-writer layout. Total recovery therefore includes different numbers of barriers and different per-writer replay/verification volumes. Separate native Open samples are retained to make that distinction explicit. These small quiescent state sets do not predict populated production recovery.

This evidence argues against consolidating these sixteen independently progressing synthetic domains into fewer writers **under the current serialized gate and flush defaults**. It does not select 256/1024 physical writers, predict 100 servers, justify changing existing persisted partition mappings, or adopt a default database count. Physical database groups remain indivisible ownership/transaction units; production grouping requires the independent domain/placement/control-record decision. Real AWS, whole-server memory, sustained large-state compaction, offered-load saturation, process-fault recovery and shared production capacity remain unmeasured.
