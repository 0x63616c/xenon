# Configurable native WAL flush cadence

Delegated coordinating-agent decision following independent user-priority and
adversarial review: expose a settings-only cadence override. This is not a claim
that Calum personally selected a tuning value.

`service_storage.wal_flush_interval_ms` is an optional integer, 1 through 1000.
Omission preserves the 100 ms default. Explicit zero, null, negative, fractional,
string and out-of-range values are rejected during configuration validation
before storage provisioning. The setting is process configuration outside the
immutable layout and is copied across lifecycle boundaries. Every native Open,
including replacement writers, applies the effective value through SlateDB's
JSON-duration Settings API. `/version` storage diagnostics report the effective
`wal_flush_interval_ms` without performing native I/O.

The agent acceptance fixture explicitly selects 10 ms for all three instances.
This changes flush cadence only. Writer operation exclusion, fencing, durable
replay, unknown outcomes, AwaitDurable-before-acknowledgment and every workload
and operation deadline remain unchanged. Restart is required to change a node's
configured cadence. Existing deployments omitting the field retain 100 ms.

Native single-writer measurements motivated this controlled next experiment;
they do not establish the cause of mixed40's actual heartbeat failure. Smaller
intervals can increase object-store PUT frequency and cost. Actual PUT counts
and AWS costs have not been measured, and 10 ms is not presented as a universal
production recommendation. The original acceptance workload must still pass.
