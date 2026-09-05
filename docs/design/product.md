# Xenon proof specification

Status: required delivery contract; implementation and validation pending.

## Storage and compatibility

S3 is the sole durable application dependency. SlateDB stores execution and visibility data directly in object storage. Local caches may be destroyed at any time without losing acknowledged mutations. The SQLite snapshot experiment was rejected by user steering and is not part of this design.

Retain Temporal Server v1.31.2 (`19a774302c613da9adc4436ab14278ccdca8e0a5`), its public frontend and workflow engine. Implement complete persistence operations behind versioned gRPC calls, preserving typed conditions and full request/response semantics. Existing SDKs and unchanged Temporal UI must work. Missing methods fail explicitly and remain release blockers where required by the pinned proof; generated stubs cannot count as implementation.

## Topology

Multiple Temporal instances operate independently of multiple Xenon storage nodes. Logical storage partitions contain complete transaction domains. Adding a storage node under active workload must redistribute existing partitions and produce traffic on both nodes, without changing Temporal history shard count.

## Correctness

A successful mutation is recoverable from S3 after serving-process failure and cache loss. Reads must not expose volatile commits. Ownership movement preserves acknowledged writes and eventually resumes work after faults stop. Lost responses are unknown outcomes, reconciled using durable identity/results without rewriting conditions as success.

## Proof and release

The delivery includes pinned Omes mixed workflows, visibility API assertions, unchanged UI exercises, fault injection, benchmarks and real-S3 validation. Emulator results do not satisfy the real-S3 gate. Numeric targets require delegated advocate/reviewer agreement before being used as acceptance gates.

The repository remains private. Deliver runnable setup, teardown, CI, recovery instructions, compatibility matrix and exact evidence. The Wayfinder map and active goal remain open until all shipping gates pass, or external blockers are accurately recorded after independent work is exhausted.

## Reproducible experiments

Every accepted proof is committed and rerunnable from a clean checkout. Pin tools and container digests; declare topology, storage configuration, workload inputs and fault schedules as versioned files. Preserve generated fuzz inputs as content-addressed replay assets. One entrypoint recreates the environment, runs assertions, and emits machine-readable evidence tied to the source commit and input/configuration hashes. Include teardown and retention behavior. Real-S3 resources use declarative configuration with credentials supplied externally. Ad hoc observations do not satisfy shipping gates.
