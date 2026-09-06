---
name: xenon-delivery
description: Plan, track, implement and review Xenon delivery work using its existing GitHub project and broad issues. Apply when working on Xenon tickets, specs, implementation batches or project status; keeps generic engineering skills aligned with the repository workflow.
---

# Xenon delivery

Resolve paths from the Xenon repository root. Read `AGENTS.md` and `docs/agents/issue-tracker.md` for the current board, milestones, authorization and evidence requirements. These rules adapt the generic engineering skills for this repository; do not alter global skill installations or other projects.

- **Plan/spec/tickets:** inspect project #8 and existing issues first. Keep the spec in `docs/design/` and link it from broad issues. Use native dependencies where real, not one ticket per function. No new project, sprint ceremony or milestone per implementation stage.
- **Implement:** claim the existing issue and set In Progress. Parallel agents use isolated worktrees within cohesive batches. Keep focused tests running; obtain independent review of the batch before integration. Do not impose a fresh review barrier for each small ticket.
- **Review/finish:** distinguish source review, component proof and full acceptance. Link the merged PR and reproducible evidence to its issue. Close/mark Done only when the whole issue scope is met; leave partial delivery In Progress.
- **Report:** post a native project status update for a meaningful integrated batch or changed blocker/outlook, with links and concise next steps. Do not duplicate every issue comment or agent update there. Project README is stable scope, not a running log. Never invent dates or delivery health.

Use one repository-filtered board, its existing Todo / In Progress / Done options and the two existing milestones. Discover live IDs and verify writes as described in the tracker guide. Preserve historical boards and unrelated configuration. Missing tracker access is a precise external limitation, not permission to create a competing local tracker or stop independent implementation.
