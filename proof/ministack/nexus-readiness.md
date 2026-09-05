# Named endpoint readiness diagnostics

Run `python3 scripts/prove.py nexus-readiness` from a clean checkout. These are SDK testsuite and diagnostic controls, not a live readiness result. The runtime uses `xenon-sdk-probe --mode fuzz-endpoint-ready` after endpoint creation and before corpus execution.

The preserved run36e3028/96ea9ccced55 failed command30 after60s; no saved fuzz input ran. Its first readiness workflow was repeatedly stuck in history transfer processing with3s context deadlines. Matching logged5s GetTaskQueue/UpdateTaskQueue timeouts, partition not ready, and duplicate queue creation before teardown. Frontends also explicitly warned that HTTP API ports were unset and Nexus endpoints unavailable. These are distinct observations: the workflow may not yet have reached its Nexus command, and enabling transport alone is not demonstrated to repair the matching stall.

Readiness now logs each attempted workflow/shard and acknowledged run ID; while waiting, every2s it fetches a bounded1000-event history sample with a2s RPC deadline. It emits only event types/counts, last ID/type and WorkflowTaskFailed cause, never workflow payloads. A final bounded diagnostic sample may take2s beyond the unchanged60s functional deadline; it cannot change a failed verdict. A sample with further pages is marked incomplete and is not a full-history oracle.

The workflow-only readiness worker no longer polls unused activity queues. The endpoint worker registers only Nexus service work and disables workflow/activity polling. SDK source v1.41.1 NewAggregatedWorker gates workflow/activity workers using DisableWorkflowWorker and LocalActivityWorkerOnly; Nexus service worker startup is independent. This reduces unnecessary matching initialization traffic without changing Omes worker configuration, durable semantics or deadlines.

Actual next-run requirements remain: all four selected history shards execute the named Nexus echo and return the fresh nonce; the parent must separately correct declared HTTP configuration and investigate persistent matching stalls. Keep the previous timeout receipt unchanged.
