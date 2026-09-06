# Issue tracker

Current board: [Xenon Architecture & Testing Delivery](https://github.com/users/0x63616c/projects/8). Keep tracking lightweight: two delivery milestones (**Build and harden**, **Release proof and polish**), broad tickets, Todo/In Progress/Done, and batched reviews. Earlier boards and milestone descriptions are historical context; they do not override the reviewed spec or latest user priorities.

Canonical map: [Prove S3-backed Temporal with dynamically scalable Xenon storage](https://github.com/0x63616c/xenon/issues/1).

## Wayfinding operations

Use GitHub issues, `wayfinder:map` and `wayfinder:<type>` labels, native sub-issues and native blocking relationships. Claim a ticket by assigning 0x63616c before research. Decision tickets use delegated advocate/reviewer debate under [the autonomous delivery handoff](../handoff-autonomous.md); the coordinator records the outcome without waiting for Calum. Record research resolutions as comments, close the research issue, and add a linked context pointer to the map. Research assets live on `research/<name>` branches under `docs/research/`.

Use the minimal repository label vocabulary and ownership rules in
[labels.md](labels.md). Project `Status`, milestones, assignees, and native issue
relationships remain authoritative for workflow state, release membership,
claims, and dependencies respectively.

## Verified native graph

The authenticated GitHub REST API repaired the parent and blocking relationships on 2026-09-05. Readback verified all eight children and nine dependency edges against the manifest below. Use `issues/{number}/sub_issues` and `issues/{number}/dependencies/blocked_by`; inspect existing links before writing to avoid duplicates. Historical capability-gap comments remain preserved.

## Intended graph

Every ticket below is a child of the map. In this manifest, identifiers are GitHub issue numbers in this repository. `blocked_by` points from the waiting issue to its prerequisite issues.

```json
{
  "map": 1,
  "children": [
    {
      "number": 2,
      "title": "Identify transaction-safe persistence partitions",
      "blocked_by": []
    },
    {
      "number": 3,
      "title": "Establish durable writes and safe SlateDB ownership transfer",
      "blocked_by": []
    },
    {
      "number": 4,
      "title": "Define the visibility compatibility and indexing options",
      "blocked_by": []
    },
    {
      "number": 5,
      "title": "Choose the storage partition layout and engine",
      "blocked_by": [
        2,
        3
      ]
    },
    {
      "number": 6,
      "title": "Choose routing and ownership handover",
      "blocked_by": [
        5,
        3
      ]
    },
    {
      "number": 7,
      "title": "Choose visibility layout and supported proof coverage",
      "blocked_by": [
        4,
        5
      ]
    },
    {
      "number": 8,
      "title": "Agree the Omes and failure-test acceptance criteria",
      "blocked_by": [
        6,
        7
      ]
    },
    {
      "number": 9,
      "title": "Agree the implementation sequence and local harness",
      "blocked_by": [
        8
      ]
    }
  ]
}
```

## Ticket index

- [Identify transaction-safe persistence partitions](https://github.com/0x63616c/xenon/issues/2)
- [Establish durable writes and safe SlateDB ownership transfer](https://github.com/0x63616c/xenon/issues/3)
- [Define the visibility compatibility and indexing options](https://github.com/0x63616c/xenon/issues/4)
- [Choose the storage partition layout and engine](https://github.com/0x63616c/xenon/issues/5)
- [Choose routing and ownership handover](https://github.com/0x63616c/xenon/issues/6)
- [Choose visibility layout and supported proof coverage](https://github.com/0x63616c/xenon/issues/7)
- [Agree the Omes and failure-test acceptance criteria](https://github.com/0x63616c/xenon/issues/8)
- [Agree the implementation sequence and local harness](https://github.com/0x63616c/xenon/issues/9)

## Current execution frontier

The initial research graph above is historical. Read project #8 and open issues for the current frontier rather than resuming from old PR numbers in handoffs. The board currently uses **Build and harden** (#3) and **Release proof and polish** (#1); resolve their live IDs before changing assignments. Completed component evidence does not establish release acceptance.

## Board operating rules

- Account owner: `0x63616c`; project number: `8`; linked repository: `0x63616c/xenon`. Both Tickets and Kanban use `repo:0x63616c/xenon` as their view filter. Linking/filtering does not prevent adding unrelated items: check repository identity before adding anything.
- Keep broad delivery issues as board items. Link implementation PRs to them; add a PR separately only when it needs independent tracking. Search existing issues before creating new work. Small related changes can share an issue and review batch.
- **Todo** means not actively being worked. **In Progress** means claimed active work, including review/testing and any documented blocker. **Done** means the issue's complete scope is integrated with relevant acceptance evidence. Do not turn draft PRs, passing isolated tests or a partial merge into Done.
- Use only the two current milestones for this delivery. Deferred work stays in its existing issues without inventing another milestone or board. Use native blocking relationships for actual dependencies; no duplicate status labels or mandatory custom fields.
- Native project status updates are occasional high-level reports. Include what changed, work in progress, next step and material risks, with evidence links. Use issue comments for detailed investigations and PRs for implementation review. Avoid repeated unchanged updates or invented dates.
- Preserve existing project visibility, historical projects and unrelated account configuration. The current board is repository-focused, not a reason to modify other projects.

For live discovery use `gh project view 8 --owner 0x63616c --format json`, `gh project field-list 8 --owner 0x63616c --format json`, and `gh project item-list 8 --owner 0x63616c --limit 100 --format json` (paginate if necessary). Use returned item/field/option IDs rather than assuming IDs from an old handoff. Check `gh api repos/0x63616c/xenon/milestones` before assigning milestones.

Project item updates and status updates are different APIs. Use `gh project item-edit` for an item's Status. Native project updates use GitHub GraphQL `createProjectV2StatusUpdate`; inspect its schema when needed. For multiline issue/PR bodies use `--body-file`; for GraphQL mutations pass a structured JSON variables file. After mutations, read back the changed fields/items/update. If a required API is unavailable, record the exact limitation and continue engineering work without creating a duplicate tracking system.
