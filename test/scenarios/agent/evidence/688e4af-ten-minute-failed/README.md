# Ten-minute profile: retained primary failure

Exact tested source `688e4af8720c35303c417af3cb57dc0daae1cbf2`.
The CLI ten-minute invocation failed with exit 1; its receipt records
`cleanup_verified: true`, and the helper result has no cleanup errors.
This is a failed delivery gate, not ten-minute acceptance.

Unchanged CLI request/source/result, helper result, first-failure snapshot and
primary `command-37.log` are preserved here. Independent preservation checked
all 49 aggregate log hashes against the local evidence before copying.
The primary Omes mixed-workload failure is iteration 4's heartbeat activity
timeout at 2026-09-06 14:34:04.664 -0700. Later cancellation is secondary.
Schema readiness and actual Nexus results on both initial instances precede
this failure; neither establishes success of the failing workload. No workload
limit or assertion was changed to preserve this receipt. Runtime timing/root
cause remains under investigation.

Original evidence directory: `.local/evidence/cli-ten-minute-688e4af`.
The result includes the exact profile, expanded inputs, source/binary and log
hashes. Only the selected files above are archived here; this is not a complete
log bundle. See `scripts/agent-smoke.py` and the scenario documentation for
the declarative runner entry points at the tested revision.
