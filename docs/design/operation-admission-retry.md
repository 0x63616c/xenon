# Same-operation admission retries

The clean `c879d0d` composed run `agent-20260906T135613-908ab4` failed when
Omes iteration 3 could not start its workflow within the pinned SDK's default
10-second RPC budget. Earlier join and B crash/eviction assertions passed.

For workflow `w-xenon-agent-omes-a2d5a8ca53bd3f3e-3`, the pinned Temporal mapping
of namespace `f09cf832-9747-46fe-bd12-189f28c00c4c` and workflow ID produces
Temporal shard 1 and Xenon `history-1`. A logged failed starts at 13:58:37.877,
38.643, 39.783 and 41.497 with B's storage endpoint refusing connections, then
44.880 with no ready owner. A's shard-1 acquisition failed at 42.960 and 43.940,
then succeeded at 46.266; the SDK deadline expired at 47.584. StartWorkflowExecution
must obtain that shard's engine before executing the workflow operation. This
identifies shard admission as a blocking dependency; the logs cannot attribute
every earlier frontend error to an individual persistence envelope. The later
all-ready control snapshot is not evidence of readiness at the earlier timestamps.

Eleven adapter families still exposed a transient admission failure after three
attempts and 20/40ms waits. This layered short inner retry with increasing outer
SDK retry waits, potentially missing recovery before the original SDK deadline.
The shared `retryOperation` helper now implements the previously reviewed cluster
policy for all twelve families: reuse the exact prepared envelope, operation ID,
digest and caller/invocation deadline; retry transport Unavailable with 20ms
backoff capped at 250ms; stop immediately on other terminal transport errors.
Router hop and owner-attempt bounds remain unchanged. No SDK timeout increased.

The first explicit routing UNKNOWN_OUTCOME survives later admission failures,
malformed/nil results and terminal errors through Temporal status conversion.
A valid durable family result resolves ambiguity, including a persisted logical
failure. Family logical error mapping remains outside transport retry. The helper
rejects unbounded contexts, and all existing invocation bounds are retained.

Delegated coordinator decision, independently reviewed: visibility formerly had
only its caller's context and no invocation bound. It now supplies a 30-second
fallback only when no caller deadline exists. Any supplied caller deadline is
preserved exactly. This avoids introducing an unbounded loop for Background
callers while preserving existing bounded requests.

Tests cover all twelve actual adapter invoke paths with byte-identical requests
and deadlines across four transport attempts, parent cancellation/exhaustion,
permanent status precedence, first actual unknown precedence, valid logical
responses and nil/unknown-enum rejection. Existing upstream/native adapter tests
remain compatibility coverage. Active experiment manifests include the shared
helper. A new composed fault run is required to establish actual end-to-end
recovery; this correction alone does not prove failover within every SDK budget.

The full adapter compatibility run also exposed a recorder interaction: permanent
local observer failure had the same generic Unavailable transport status as a
recoverable owner gap. Retry now consults that invocation's explicit failed latch
and stops immediately; active registration alone is not failure. Final observer
reporting joins its error with the operation error so it cannot erase an earlier
unknown outcome. The existing recorder-death control still requires zero backend
calls and fail-fast completion, with an added unknown-preservation regression.
