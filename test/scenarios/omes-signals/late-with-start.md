# Late activity SignalWithStart

Pinned Omes `c6978ba39aa03551ce28974117e8d7ecf983d2b3` constructs an empty
`WorkflowInput` in Go `ClientActivities.ExecuteClientActivity`. If an optional
numbered SignalWithStart reaches the server after its parent closes, that empty
fallback creates a workflow which rejects the generated signal as undeclared.

The overlay changes only the Go activity's fallback constructor. It derives an
optional, deduplicated set of all positive numbered signals in the complete
client sequence, including nested client action sets. This allows later signals
to reach the same newly running fallback, whose start argument cannot be changed
by subsequent SignalWithStart calls. It does not alter signal actions, required
IDs, ordinary loadgen workflow inputs, or either corpus. An existing execution
ignores the fallback argument under Temporal's API contract.
A newly started workflow keeps the upstream empty-input lifetime: this change
accepts the actions, but does not invent a completion action or claim that every
such new run terminates.

The registered worker proof captures the actual client executor's SDK arguments
and passes the captured fallback plus unchanged signal to the real Go worker in
the SDK test environment. It also checks nested signal discovery, deduplication and immutable actions; two signals execute in one real worker, with the second awaiting state set by the first.
This is not an executed server-side late-start or exhaustive execution-census
proof. The corrected soak and its eventual all-started oracle remain separate.
