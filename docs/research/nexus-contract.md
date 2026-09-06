# Pinned Nexus endpoint persistence contract

Baseline Temporal v1.31.2 `19a774302c613da9adc4436ab14278ccdca8e0a5`, SQL store, PostgreSQL plugin and shared `SqlStore.txExecute`.

Four operations share one catalog partition. Table version starts at zero; initial successful insert sets it to one. Each successful upsert/delete increments it once. Endpoint creation stores version one; updates compare supplied endpoint version and store version plus one. Check both guards before writing so endpoint failure never advances the catalog version.

SQL error mapping matters: `txExecute` wraps FailedPrecondition and Internal as Unavailable. Preserve observable Unavailable on ordinary table/endpoint conflict, but initial table insertion duplicate is ConditionFailed. Missing delete rolls back catalog version and returns NotFound. List returns the observed table version even on version conflict. Negative list size is rejected by the upstream manager; zero fetches only the catalog version.

Pagination orders binary UUIDs and carries logical cursor bytes only. A fixed catalog version across pages detects intervening mutations. A byte budget may shorten pages. Replay preserves the original result and version, including a conflict response, after a durable nonempty fencing barrier.

Implementation and declarative proof are tracked by #37; this audit is not runtime evidence. Full Temporal boot, S3 recovery and routing remain independent gates.
