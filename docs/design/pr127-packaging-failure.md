# PR 127 hosted failure follow-up

Hosted run [34053219690](https://github.com/0x63616c/xenon/actions/runs/34053219690)
tested revision `5f1dc97bd62231591b43882b53f0b7bd9989d88c`. Its unified-container
build failed when the image's `check-config` command rejected the packaged example
with `explicit service_storage format2 required`. The example predated the service
runtime activation. It now carries the complete explicit format 2 layout and
limits, using the existing committed scenario settings and paths beneath its own
prefix. A CLI regression validates the exact packaged file without starting a
backend. This correction does not establish successful container or workflow proof.

The independent unified-agent failure is preserved in artifact
[unified-agent-evidence, 9995276036](https://github.com/0x63616c/xenon/actions/runs/34053219690/artifacts/9995276036),
receipt `agent-20260906T185536-f89e1c`. Its result is
`ScenarioInvariant: background process exited unexpectedly: omes (1)`.
`omes.log` records the primary at `2026-09-06T19:01:24.781Z`: iteration 3 failed
to start its kitchen-sink workflow because `search attribute OmesExecutionID is
not defined`. Omes reports fatal at `19:01:26.144Z`. Iteration 1's subsequent
`context canceled` is cooldown fallout, not the original cause.

The receipt observed C join and serve matching, then killed B while Omes
workflows 1 and 2 were running. This is a failed workload gate, not an acceptable
transient. Source correlation: `scripts/agent-smoke.py` bootstraps search
attributes before launching Omes; `cmd/xenon-sdk-probe/main.go` registers
`OmesExecutionID` through Temporal's operator API after seeding cluster slots.
B's log at `19:00:56.137Z` says that attribute already exists; C starts at
`19:01:03.087Z`. Pinned Temporal v1.31.2 caches search attribute metadata and
namespace mappings, but these observations do not identify the serving instance
or prove whether a cache or persisted mapping caused the missing attribute.
A targeted per-instance mapping/metadata observation is needed before choosing a
correction. No timeout or workload assertion is weakened by this packaging fix.
