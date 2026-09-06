# Managed visibility durable result barrier

Actual seed run ec92f15 /20260906T013631Z-xenon-ministack-65f9430e5756
failed at the unchanged15-minute context, with448 acknowledged records on each
partition (1792 total). No movement was executed.

Pinned SlateDB3fb9e8 config.rs defaults flush_interval to100ms, and Xenon's
managed builder does not override settings. There is no evidence of a1-second
flush default. Exact per-stage latency has not been measured in that run.

Every adapter document conversion calls visibilityTypes, which sends GET_SCHEMA
to the shared global partition before START goes to its visibility partition.
Thus four seed workers do not eliminate the serial global schema-read queue.
Previously every visibility Execute nested journal inside exported Owner.Run:
managed successes awaited both the journal and a separate authority-barrier
write. The source confirms redundant serial durable writes, not an exact
attribution of all observed latency to flush scheduling.

Use the existing constrained runJournalResult for visibility, including schema
reads. Only its final durable outcome is returned. Fresh outcomes remain atomic
with mutation; replay still commits a nonempty durable fencing marker. No stale
schema cache is added. Arbitrary Owner.Run callbacks retain their barrier.

Registered native controls cover authority-enabled document mutation, delete,
reopen/replay/no resurrection, and stale-writer fencing, plus schema fresh/replay
and fencing without a redundant ownership barrier. Their next execution and
actual runtime speedup remain pending; no deadlines or datasets changed.
