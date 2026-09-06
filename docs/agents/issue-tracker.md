# Issue tracker

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

The initial research and delegated decision tickets are closed. Their graph above is historical context, not an instruction to repeat those decisions. The live map remains authoritative.

Implementation is under integration in PR55 and PR66. The active runtime frontier is visibility #61, the real multi-instance Temporal ministack #63, and saved fuzz replay #69. Native maintenance #64 and bounded outcome-capacity accounting #67 have clean component proofs; their full workload composition and final integration remain tracked. Fixed history routing #62 and factory #60 still require their documented runtime acceptance.

The first ministack profile is explicitly a smoke gate. Preserve the larger workload, fault, scale-out, visibility and measurement requirements in `docs/design/acceptance.md`; a smaller passing profile does not discharge them. Hosted CI is externally blocked by the account billing/spending-limit annotation recorded on the map. Real-S3 validation requires an authorized bucket/prefix and external credentials. Neither is a pass.
