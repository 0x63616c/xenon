# Frozen visibility component fixture

`python3 scripts/prove.py go-visibility-frozen` builds the real pinned native engine and runs the committed deterministic2000-record recipe from frozen.json. Five native owners serve real gRPC: four fixed visibility partitions plus the schema/control owner. This is an in-process component fixture, not ownership movement, process failure or S3 evidence.

Each owner uses declared1ms native WAL flushing to keep thousands of durability barriers practical; production defaults are unchanged and component durations are not production performance evidence. Every complete adapter call retains its existing30-second limit. The enclosing test has10minutes and registered command has15minutes including setup. These do not replace acceptance RPC budgets.

The exact UUID/data/time/type recipe is committed; all four actual partition mappings must be populated. Every page size1/7/100 must enumerate exactly2000unique expected IDs, count must equal2000, and four ExecutionStatus groups must each contain500records. The three latest records carry1.1MB memo payloads, forcing byte-limited underfilled pages at sizes7and100. Actual ownership movement, concurrent mutations, all-type/CHASM/UI fixtures and no-fault/fault runtime replay remain separate required gates.
