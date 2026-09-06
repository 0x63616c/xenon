# Omes admission deadline checkpoint

Clean source `c879d0d512056f4958ace633ce41a04b4e928a4c`, reproduced with
`python3 scripts/agent-smoke.py` and committed configuration/tool/workload pins.

The third node joined and served matching, and B was killed and evicted.
Omes iteration 3 then failed starting its workflow with context deadline
exceeded. Iteration 4 cancellation is secondary. This run did not reach
Omes20 completion or cold recovery. It is **FAILED**.

The unchanged receipt, Omes log and bounded failure-time storage/control
snapshots are preserved. Diagnostics are sequential observations, not an atomic
snapshot; their differences alone do not establish a control inconsistency.
All four diagnostics were captured and scoped teardown returned zero.
