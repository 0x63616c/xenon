# Late activity SignalWithStart

Pinned Omes `c6978ba39aa03551ce28974117e8d7ecf983d2b3` constructs an empty
`WorkflowInput` in Go `ClientActivities.ExecuteClientActivity`. If an optional
numbered SignalWithStart reaches the server after its parent closes, that empty
fallback creates a workflow which rejects the generated signal as undeclared.

The overlay explicitly enables fallback metadata only for that Go activity
executor. For an actually empty input and a positive numbered WithStart signal,
it clones the fallback and declares only that signal optional. It does not alter
signal actions, required IDs, existing nonempty inputs, or either corpus. An
existing execution ignores the fallback argument under Temporal's API contract.
A newly started workflow keeps the upstream empty-input lifetime: this change
accepts the actions, but does not invent a completion action or claim that every
such new run terminates.

The registered worker proof captures the actual client executor's SDK arguments
and passes the captured fallback plus unchanged signal to the real Go worker in
the SDK test environment. It also checks default-off and nonempty-input controls.
This is not an executed server-side late-start or exhaustive execution-census
proof. The corrected soak and its eventual all-started oracle remain separate.
