# Working on Xenon

Start with [the autonomous delivery handoff](docs/handoff-autonomous.md), README.md, CONTEXT.md, docs/agents/issue-tracker.md and the live Wayfinder map.

## Current user authorization

Calum explicitly delegated autonomous project decisions and delivery to the coordinating agent. This supersedes earlier requests for live human approval of each architecture choice. Work through planning, implementation, integration and end-to-end proof without routine clarification.

For decision tickets, use two independent agents: a user-priority advocate grounded in Calum's stated requirements and an adversarial systems reviewer. The coordinator adjudicates using evidence and experiments. Record outcomes as delegated agent decisions, not claims that Calum personally approved a design. Do not impersonate him or invent preferences.

This project adapts Matt Pocock's Wayfinder: execution is in scope, agent debate replaces live-human grilling, and multiple tickets may be resolved per session. Continue beyond charting until the delivery gates pass or a concrete external blocker remains. Keep research, implementation, review and test tasks bounded; isolate concurrent edits in branches/worktrees.

## Requirements

S3-only durable application storage; disposable local disks; retain Temporal Server and existing SDKs/UI; execution and visibility; Omes; dynamic Xenon storage-node addition and ownership movement; multiple Temporal instances; crash recovery; preserved acknowledged writes. Slower is acceptable, correctness is mandatory. SlateDB and adapter-side routing remain candidates until delegated decisions validate them.

Do not silently weaken scope or confuse source review with executed tests. Pin versions, cite primary evidence, and report failures honestly. A debate is not a correctness proof.

## Delivery

Use GitHub issues as the Wayfinder map and publish linked resolution evidence. Claim tickets before work. Commit, review, test, push and integrate completed work, respecting actual branch protections. Former grilling tickets no longer require Calum to reply. Preserve historical comments and annotate the new delegation.

Research reports currently live on research branches; integrate validated artifacts so main becomes self-contained. Build the runnable proof, CI, operational documentation, compatibility matrix, demonstration and release-ready packaging. Prepare an accurate landing-page draft after the core proof if useful; production SaaS/billing/dashboard remain later scope.

Keep this private repo private pending explicit public-release authorization. Use existing authorized resources within their scope; do not invent credentials or assume permission for external spend. Missing external access must be recorded precisely while independent work continues.

Consult docs/handoff-autonomous.md for exact ticket URLs, research commits, architecture evidence, acceptance gates and the full delegation protocol.
