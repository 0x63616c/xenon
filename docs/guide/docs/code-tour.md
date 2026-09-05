# A tour of the source

The repository is one Go module with a pinned Rust engine dependency. Start with the operation you are changing and trace its complete path through the adapter, RPC contract, handler and owner gate.

## Entrypoints

| Path | Role |
| --- | --- |
| `cmd/xenon-go-node/` | The Go storage process, standalone or managed ownership mode. |
| `cmd/xenon-topology/` | Explicit conditional topology administration. |
| `cmd/xenon-temporal/` | Pinned Temporal launcher with custom persistence factories. |
| `cmd/xenon-sdk-probe/` | Real SDK workload, health, visibility and history assertions. |
| `cmd/xenon-visibility-probe/` | Declared visibility fixtures and public API checks. |

The Temporal launcher uses the `ministack` build tag. The runtime controller supplies its actual build command and configuration; it is not an arbitrary installed Temporal binary with a runtime-loaded plugin.

## From interface to operation

`internal/temporalstore/` implements the factory seam. Execution configuration selects a stable service address, an ordered `historyPartitions` list, a `matchingPartition` and a `globalPartition`. Visibility has its own factory and index/schema options.

`internal/adapter/` implements the pinned persistence interfaces, serializes typed requests, normalizes errors and preserves operation identity across retries. `proto/` declares the wire contracts; `gen/` contains the generated Go bindings. Durable outcome variants distinguish operation families.

`internal/node/` implements the operation families and shared owner lifecycle. Read the relevant handler with its tests. Validation order, conditional errors, rollback and page-token semantics are part of the compatibility contract, not incidental SQL behavior to simplify away.

## Routing and ownership

`internal/routing/` provides bounded gRPC forwarding through the current owner. `internal/directory/` implements conditional S3 ownership records. `internal/ownership/` composes topology activation, one-shot engine acquisition, managed dispatch and authority checks.

These modules answer different questions: where to send a request, which owner is advertised, and whether this specific handle may publish a result. Do not move the final authority check out of the shared owner gate.

## Visibility and query semantics

`internal/query/` handles the query representation and parsing. `internal/visibility/` handles typed document semantics and evaluation. Visibility adapter/node code connects those semantics to durable records, schema context and bounded fan-out.

The pinned SQL behavior matters for nulls, numeric precision, timestamps and search-attribute aliases. Candidate indexes do not justify claiming that every query is index-driven. Review the committed query and visibility contract tests before changing normalization or cursor ordering.

## Native engine and proof tools

`test/compatibility/rust/` retains the Rust workspace, primitives and reference probes. `scripts/build-go-node.py` prepares the official pinned SlateDB Go bindings and native library. Those Rust components remain useful for cross-language and engine proofs; application node code is Go.

`internal/processcut/`, `internal/rpctrace/` and `internal/proof/` contain explicitly scoped test/measurement helpers. Optional hooks are disabled in the ordinary runtime profile. Their control tests are not equivalent to executing a complete application fault workload.

## Keep the evidence close

Go unit and integration tests live beside their packages. `experiments/` binds runnable proof commands to expected assertions. `proof/` stores workloads, manifests and fixtures; `deploy/` stores local topology definitions. `docs/research/` records detailed compatibility decisions and their limits.

Add a new operation as a complete vertical slice: contract, adapter, handler, durable outcome and tests. Managed RPC descriptor coverage should fail when a newly registered family is missing from forwarding or dispatch. Avoid splitting a single atomic persistence operation across remote storage calls.

The repository development loop is recorded in `docs/development-loop.md`. The [testing guide](./testing.md) explains how its gates fit together; [Temporal upgrades](./upgrades.md) describes the maintained version-change procedure.
