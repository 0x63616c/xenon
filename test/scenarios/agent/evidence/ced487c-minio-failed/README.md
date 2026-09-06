# Failed service-runtime join checkpoint

Source: `ced487ce2a4323e44442dbf926704b255d6c37b2` (clean at start).
Command: `python3 scripts/agent-smoke.py`, using its committed MinIO topology,
configuration, pinned tools and workload. The original machine-generated
`result.json` is retained unchanged. Absolute paths describe the original run;
they are not required reproduction paths.

The run reached actual SDK execution, an observed running Omes workflow,
and node C health, then C exited with status 1. Temporal worker startup
fatally failed to initialize membership heartbeats because persistence returned
`no ready partition owner`. `fatal.json` retains the exact fatal record and
hash of the original complete C log. This establishes the failure location,
not the complete underlying cause or evidence of data loss.

The harness detected the unexpected child exit and cancelled its in-flight
control read. Scoped Compose teardown returned zero; no cleanup error was
recorded. Ownership movement, subsequent crash/restart, cold recovery and
full ten-minute/Nexus acceptance did not complete. Earlier passing runtime
checkpoints do not turn this run into a pass. No workload deadline was changed.
