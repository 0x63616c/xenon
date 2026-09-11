# Input-derived execution graph (partial ORACLE-01 / GEN-02)

`xenon test workflow --expected-graph ...` selects the explicit
`omes-serial-intent-v1` capability. The same resident flags apply to resident
search/replay/minimize commands. Broad exploration remains the default and its
provenance says `exploratory-history-only`; it does not claim input-derived
semantic graph acceptance.

The strict path calls `workflowgraph.Derive(savedTestInput)` before any resident
member dispatch. Every batch member is preflighted before any worker starts.
`OmesRuntime.Execute` repeats the check for direct callers, saves the expected
logical graph and input digest before launching Omes, and asks the external
oracle to use `--expected-input input.proto`. That oracle derives expectations
before contacting Temporal, fetches complete bounded/paginated histories, then
calls `workflowgraph.Check`. Its successful receipt includes the contract name
and input hash. Resident audit requires those fields to match the saved input.

## Narrow supported grammar

The parser reads the pinned Omes protobuf directly using the existing Go
protobuf wire library; it does not import worker decision code. Supported input:

- One TestInput WorkflowInput with serial initial action sets.
- Awaited children with nonempty unique explicit workflow IDs, default options
  and exactly one binary/protobuf WorkflowInput payload. Await choice must be
  absent (the pinned worker's default is wait-for-completion).
- Nested serial action sets.
- Explicit non-nil return payloads, or Continue-As-New with exactly one
  binary/protobuf WorkflowInput argument and default options.
- At most 1 MiB input, 2,048 actions, 128 execution nodes and depth 32.

Client actions, signals/updates, concurrent sets, implicit completion, activities,
Nexus, cancellation/abandonment, child ID reuse, extra options and unknown fields
are rejected. This is stricter than the broad seeded Omes generator: a generated
input outside this capability fails; it is never silently skipped or resampled.
To expand the accepted grammar, extend its input semantics and independent
negative controls together. No YAML or scenario DSL is introduced.

A root is a logical input node. Upstream Omes generates its opaque ExecutionID,
and Temporal generates run IDs; those cannot be known from workflow bytes.
The checker requires exactly one initial root under the saved case's Omes ID
prefix (pinned Omes first iteration is one), with matching input semantics. Child workflow IDs come
exactly from input. Opaque child/continuation run IDs are bound through complete
histories, then checked against all expected nodes, parent links and predecessor
links. They never determine which nodes ought to exist. Omitted child commands
and children missing from both visibility and parent history therefore fail.

Expected terminal states and payloads come from the input, including results of
continued children. The checker compares protobuf payload semantics, unwrapping
the pinned Omes `_passthrough` encoding. Initial root input uses Omes's
`json/protobuf` converter; JSON whitespace and map ordering are insignificant.
The checker never derives expected result values from observed results.

## Executed controls and reproduction

```sh
go test -race ./internal/proof/workflowgraph ./cmd/xenon-omes-oracle
# With the normal pinned native-library environment configured:
go test -race ./internal/simulation ./cmd/xenon \
  -run 'TestResident|TestOmesExpected|TestWorkflowBatch|TestEveryCommandHelp|TestLightweightCommands' -count=1
```

The code-authored four-execution fixture includes an initial root, awaited child,
continued child and continued root, with different explicit child/root values.
Controls omit the child from both inventories, omit its parent command, corrupt
child/root results, omit a continuation, corrupt parent/continuation input and
remove a history event. Each fails its named `expected_graph/*` assertion.
Unsupported grammar controls fail before dispatch. A later invalid batch member
prevents even an earlier admissible member from starting.

`readCompleteHistory` is the production pagination path. Its controls cover two
pages, an omitted event, repeated cursor, unavailable page and nil page. Saved
complete histories—not a first page or list/count agreement—reach graph checks.

The committed `internal/proof/workflowgraph/testdata/omes_serial_payloads.json`
was emitted by the pinned Omes protobuf package and actual custom converter,
using the adjacent code-authored `omes_payload_probe.go.txt`. It checks the wire
field and encoding assumptions independently of Xenon's parser. Regenerate with
an existing verified prepared Omes source bundle (no services or mutation of the
bundle): copy the probe to a temporary `.go` file and run `go run /absolute/probe.go`
from that bundle's `worker/source`, with `GOFLAGS=-mod=readonly` and
`GOTOOLCHAIN=go1.27.1`. Compare decoded protobuf/JSON semantics; protobuf JSON
whitespace and embedded protobuf map ordering may differ between emissions.

Pinned source inputs used for the fixture:

- Omes commit `c6978ba39aa03551ce28974117e8d7ecf983d2b3`, corrected overlay
  `cdb70939ac3a6f0449534421dd13579984c69fcb1c5b737c72d744a63b47bc09`.
- Temporal API `d96bd55e87799e9f6a33a1c40a56cfa932566bdf`, Go SDK `v1.48.0`.
- Prepared `workers/proto/kitchen_sink/kitchen_sink.proto` SHA256:
  `f564ee09e6e23b39bb31f1a8f1427a4c2939024d41234de9af0682cf8ced1df0`.
- `clioptions/client.go` SHA256:
  `7c0ed9a924655ab07edb2c1dc205f35440626162d7edbc78d65d7921ce96bdb4`.

These are component proofs with synthetic complete histories and real pinned
protobuf/converter output. No new real Temporal journey was run for this batch.
Nexus intent/handler graphs, concurrent/signal-driven semantics, full fan-out
limits, and the complete ORACLE-01/GEN-02 gates remain unfinished.

## Parent-side await and ordering controls

The checker also requires each input-declared child to have matching parent-side
StartChildWorkflowExecutionInitiated, ChildWorkflowExecutionStarted and
ChildWorkflowExecutionCompleted events. It verifies both event-ID links, the
initiated input, final continued-child run identity and copied result. Each child
initiation must follow the preceding child's completion in input order; every
completion must precede the parent's terminal action. A child that completes
elsewhere without its parent awaiting it cannot satisfy this contract.

Positive histories include these parent-side completion events. Additional
controls remove an await, reverse two fixed children while preserving their
individual links/results, issue the next child too early, corrupt the completion
links/result, and complete the parent too early. A continued child must complete
with its final run ID, not its original started run ID. This matches the pinned
Temporal server's `tests/continue_as_new_test.go` child completion assertions and
`MutableStateImpl.AddChildWorkflowExecutionCompletedEvent` behavior. These source
checks do not substitute for a newly executed real workflow.

The root ID suffix is `-1`: pinned Omes `GenericExecutor` calls `NewRun(i + 1)`.
A negative control rejects a `-0` first-iteration identity. The saved upstream
payload fixture contains no history or runtime IDs and needs no encoding change;
its accompanying hand-authored histories now model the required await events.
