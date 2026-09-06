---
name: xenon-temporal-upgrade
description: Assess and implement explicitly selected Temporal Server version upgrades in Xenon using source-impact reports, pinned scenario evidence, and separate fresh-install and existing-state compatibility checks.
---

Use the repository's [Temporal upgrade runbook](../../../docs/temporal-upgrades.md). Resolve paths from the Xenon repository root; this skill stays project-local and does not alter user memory or global configuration.

Require an explicit old and target revision and a clean local Temporal checkout containing them. If the target is missing, ask for the intended revision; do not pick “latest.” Source comparison does not itself authorize a dependency upgrade.

Run `python3 scripts/temporal_upgrade.py --source SOURCE --old OLD --new NEW --output NEW_OUTPUT_DIRECTORY`. Read `report.json` and `temporal.diff`, including all unmapped paths. Follow the seam table into the adapter, factory, wire serialization, native handlers and query code; classify semantic changes as well as signatures. Preserve opaque stored values, typed errors, transaction boundaries, stable partition identities, durable replay and the S3-only requirement.

When implementation is authorized, isolate it in a branch, update explicit pins and contracts, and run the runbook's existing scenario gates. Keep failed receipts. Report fresh-install and existing-state upgrade separately. An existing-state claim requires old-version-written objects read and continued by the target with cold local state; do not substitute empty-prefix success. Mixed-version operation and rollback remain unproven unless exercised explicitly.

Finish with resolved commit IDs, impacted seams, actual test receipts, each compatibility outcome, and concrete remaining blockers. Never label the path inventory as a compatibility pass or merge a PR merely because it compiles.
