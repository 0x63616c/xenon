# Issue tracker

Canonical map: [Prove S3-backed Temporal with dynamically scalable Xenon storage](https://github.com/0x63616c/xenon/issues/1).

## Wayfinding operations

Use GitHub issues, `wayfinder:map` and `wayfinder:<type>` labels, native sub-issues and native blocking relationships. Claim a ticket by assigning 0x63616c before research. Decision tickets use delegated advocate/reviewer debate under [the autonomous delivery handoff](../handoff-autonomous.md); the coordinator records the outcome without waiting for Calum. Record research resolutions as comments, close the research issue, and add a linked context pointer to the map. Research assets live on `research/<name>` branches under `docs/research/`.

## Current capability gap

The available GitHub connector created all issues and applied labels, but does not expose native sub-issue or dependency mutations. Links in issue bodies are temporary navigation only, not native blocking edges. Native wiring is pending; do not claim the graph is complete or rely on native frontier queries until repaired. GitHub itself supports these relationships. The manifest below records exactly what needs wiring.

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

## Research frontier

The first three research tickets are closed with source-backed reports, not runtime proofs. The next frontier is Choose the storage partition layout and engine. Former grilling tickets are now autonomous decision tickets: resolve their prerequisites and run the advocate/reviewer process without requesting live user approval. Continue creating implementation and validation tickets until the end-to-end destination is met. Assignment to 0x63616c records the agent's claim, not a request for Calum to act.
