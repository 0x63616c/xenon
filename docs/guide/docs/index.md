# Meet Xenon

Xenon is experimental S3-backed persistence for Temporal. Go storage nodes embed SlateDB through its official Go bindings. Temporal keeps its workflow engine, frontend API, existing SDKs and UI.

Instead of writing to a conventional persistence database, a Temporal adapter sends a complete operation to one Xenon service endpoint. The receiving node either executes it on a partition it owns or forwards it to the current owner. SlateDB writes durable application state to S3. Conditional S3 objects hold ownership and placement metadata.

## What stays familiar

Applications still start, signal and query workflows through Temporal. Temporal services still decide how workflows advance. Xenon implements the persistence interfaces underneath those services; it does not replace the workflow scheduler or introduce a new application SDK.

The pinned integration uses Temporal Server **1.31.2**, the smoke SDK **1.41.1**, Temporal UI **2.53.3**, and SlateDB **0.16.0**. The separately prepared Omes Go worker uses SDK **1.48.0**. These are tested inputs, not a claim of compatibility with arbitrary versions.

## The storage model

A **storage partition** is Xenon's unit of atomic operation, admission and write ownership. It is distinct from a Temporal **history shard**, which groups workflow executions within the Temporal engine.

History placement uses an immutable ordered partition list; the current ministack configures four history partitions. Matching uses a shared partition; global metadata uses another; visibility uses four fixed partitions. One node may own several partitions. Adding a node moves existing partitions without changing their identities.

Changing partition count or order is a data-layout migration. It is not the same operation as moving an existing partition to another node.

## Where the project stands

Persistence families, any-node forwarding, conditional ownership, and a real multi-instance smoke controller are implemented. Component proofs and parts of the integrated runtime have passed. **The complete shipping proof has not passed.** The recorded integrated smoke failed its post-cold Omes visibility check after exact workflow-history recovery succeeded. A later fuzz-soak startup failed functional Nexus readiness before any saved corpus input ran.

Read [verification status](./status.md) before interpreting a feature list or running the proof. The larger acceptance profile, saved fuzz replay under faults, final measurements and real AWS S3 validation remain separate gates.

## Start here

- [Explore the architecture](./architecture.md) to follow an operation and a move.
- [Set up local development](./development.md) to run the committed proofs.
- [Read the code tour](./code-tour.md) to find the relevant implementation.
- [Understand recovery](./operations.md) before changing topology or investigating a failure.

Xenon is public experimental software under the MIT License. No hosted Xenon service is available today.
