# The path to durable state

Follow a request, inspect an ownership move, or inspect a Xenon agent. Select a stage to see the contract it enforces.

<XenonArchitecture />

## The boundary that matters

The adapter submits a **complete persistence operation**. Conditions, mutations and its outcome belong inside the owning partition's transaction. A remote key-value interface would leave the caller responsible for coordinating those pieces across failures.

The routing decision and the authority decision are separate. Forwarding chooses a destination. Under the owner's shared gate, fresh topology activation and an exact READY directory record establish whether that process may attempt the operation. SlateDB fencing and a durable barrier determine whether it may publish the captured result.

## Stable partitions, replaceable processes

| Domain | Current placement | Consequence |
| --- | --- | --- |
| Execution and history | Ordered list; four in the ministack | Stable shard-ID placement; global listing fans out across the fixed list. |
| Matching | One matching partition | Task-queue subqueues stay colocated; namespace user-data batches remain atomic. |
| Global metadata | One global partition | Namespace and cluster/control records use a stable transaction domain. |
| Visibility | Four fixed partitions | Bounded fan-out and a global schema context support list/count operations. |

Node addresses are not stored in application keys or page tokens. The S3 directory can contain owner contact metadata. Membership changes move existing partitions; they do not split them or rewrite their application identities.

## Three durable layers

**Topology intention** states which process incarnation is activated and where partitions should live. It is changed explicitly using conditional S3 publication.

**The ownership directory** binds a partition and data prefix to a generation, transition and opening/ready owner. Reservations are one-shot; an old attempt is not reconstructed after restart.

**SlateDB state** contains application records and durable outcomes. A new engine opener fences an older writer. Publication waits for durable work, including a nonempty barrier on reads and replay.

Topology and the directory are different S3 objects. Their checks are not a cross-object atomic transaction. The admission protocol accounts for that race instead of treating READY alone as authority.

## A shareable overview

[Open the standalone SVG](/architecture-overview.svg) for a compact diagram that can be saved or embedded. It describes the implemented architecture, not a performance result.

![Xenon architecture overview](/architecture-overview.svg)

For recovery boundaries and the unresolved runtime gate, continue to [operations](./operations.md) and [verification status](./status.md).
