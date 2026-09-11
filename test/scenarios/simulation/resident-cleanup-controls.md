# Resident cleanup failure controls

The CLEAN-01 tests in `workflow_cleanup_controls_test.go` exercise the production
`ResidentWorkflowDriver` and shared Runner with an injected runtime. They add four
controls beyond the existing paused-child and prior-case-audit regressions:

| Control | Required observation |
| --- | --- |
| Exact census unavailable | Cleanup remains unverified, zero cases complete, active ownership is retained and a fresh run ID is rejected before another runtime check or submission |
| Cleanup callback panics | Runner records the exception as cleanup failure with secondary evidence; the same reuse prohibition and immutable saved-input checks apply |
| Late creation after cancellation | A previously admitted Execute creates an execution before returning; cleanup cannot query the census before that producer drains, then sees and removes the late execution |
| Census hangs after producer drain | Producer drain consumes 500ms of a 1s virtual cleanup budget; census receives only the remaining 500ms, timeout retains unresolved execution/ownership and refuses reuse |

The two uncertain-census controls request two cases and prove only one is admitted.
They inspect persisted `result.json` and the unchanged scenario artifact. The late
creation and deadline controls demonstrate recovery only for the *same* owned
case after uncertainty resolves; they never admit a new case while cleanup is
unverified. No automatic destructive fixture reset is performed.

```sh
go test -race ./internal/simulation \
  -run 'TestResident|TestBlockedCleanup|TestSuccessfulCleanup|TestCleanupCancellation' \
  -count=1
```

The focused new controls passed in 1.703s, and the broader resident/shared-cleanup
selection passed in 2.278s (Go 1.27.1 darwin/arm64). Logical timing is injected;
three-second external control watchdogs only fail stuck tests.

These are deterministic lifecycle-seam proofs, not real Temporal visibility
census, OS process enumeration, full-disk injection, or complete CLEAN-01
acceptance. Existing production lifecycle behavior passed, so no alternate
cleanup implementation or new resource abstraction was added.
