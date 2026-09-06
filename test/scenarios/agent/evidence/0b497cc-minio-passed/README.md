# Format-2 MinIO smoke receipt

Unchanged receipt from `agent-20260906T141816-873f51`, tested clean source
`0b497cc07745067db06c5e175174c7d7f81f77a6`. Result: `component-passed`, profile
`smoke`, `full_acceptance: false`. Receipt SHA-256:
`599dbc85a4960c71b092db1f525c2e1c157fb4b65e9b9eef539f431cc62406ea`.

The pinned `scripts/agent-smoke.py` run exercised three combined Xenon/Temporal
agents with shared MinIO storage, SDK workflows and 20 Omes kitchen-sink
iterations through one endpoint. It observed C serving an assigned partition,
killed B while Omes workflows were running, observed eviction/rebalancing,
restarted B, checked completed-workflow visibility and Temporal HTTP/UI, then
cold-restarted all agents and compared SDK histories and visibility again.

Independent preservation review checked all 75 command output hashes and all
87 saved log hashes against the original local evidence. All 909 recorded
tracked-input hashes match the exact tested Git revision, including the harness,
scenario and reused pins. The saved build/version output agrees with the
receipt's native attestation, clean revision, Temporal 1.31.2 and SlateDB Go
0.16.0. The runner checks source, binary and native-library hashes before pass
and again after cleanup; Omes source/prepared-worker hashes are checked after
its workload. Historical executable bytes are not archived here and were not
independently re-hashed after subsequent builds.

Both SDK history JSON files are byte-identical before/after cold recovery.
The two nonzero commands are bounded initial phase observations (13 and 14),
followed by successful progression; they are retained unchanged. Omes logs show
normal run completion. The exact runner requires Omes exit zero and treats
unexpected monitored child exits as failure. Its final cleanup signals and
reaps child process groups, and any cleanup error changes the result to failed.
Command 74 exited zero and its log confirms removal of both containers, the
network and the object volume; the receipt has no cleanup errors.

Only the receipt is committed; original logs and history/control snapshots were
inspected locally and are referenced by their recorded hashes. This is evidence
of this finite MinIO smoke schedule, not real AWS S3, ten-minute/Nexus/capacity
acceptance, exhaustive fault safety, or the later per-instance schema readiness
precondition. Reproduction uses the scenario and runner at the tested revision;
keep credentials external as described in the scenario documentation.
