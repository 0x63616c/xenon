# Preserve the observed stop cause

STOP-01 requires the earliest observed cause to remain immutable. The command
contract also distinguishes correctness failure (exit 1), exploration budget
(exit 2), and user cancellation (exit 130).

Previously, Search classified a returned error using `errors.Is` against context
sentinels after its phase latch had already selected the primary failure. An
internal canceled request or RPC deadline could therefore become user cancellation
or an exhausted exploration budget even while the parent context and virtual
budget were still live. The regression test reproduced three such misclassifications.

Case receipts now retain the phase's observed `stop_reason`; Search uses that
rather than interpreting the selected error's wrapping chain. Generation likewise
records an error observed before parent cancellation or the logical deadline.
Internal request failures remain `first_failure`. Actual earlier parent
cancellation and actual earlier run-budget expiry retain `canceled` and `budget`.
A settle-budget failure remains a failure, matching the existing contract.

`stop_reason_test.go` exercises both context sentinel errors in generation, Run
and Settle (six cases), plus both orderings of an internal deadline failure versus
actual cancellation and actual logical-budget expiry (four cases). Assertions
check no completed case, exactly one admitted generation, matching persisted case
and run outcomes, and verified cleanup for controlled competing-stop cases.
Generation failure additionally must never reach driver validation.

```sh
go test -race ./internal/simulation \
  -run 'Test(Internal.*Context|ReportedFailureOrdered|ReportingBoundaryRace|CleanupCancellation|SearchCoupledSavedReplay|GenerationAndSettle|LogicalBudget|CanceledRun)' \
  -count=1
```

This focused set passed in 3.259s on Go 1.27.1 darwin/arm64. It strengthens the
existing stop-ordering tests rather than claiming all STOP-01 fault combinations
or real node-death/remote cleanup coverage. The tests use injected time; their
external watchdogs cannot make incomplete execution pass.

The broader shared-runner, replay, minimizer, resident-driver and workflow-batch
race selection also passed (42.736s); no real server or fixture was launched.
