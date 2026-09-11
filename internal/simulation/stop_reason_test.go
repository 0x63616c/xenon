package simulation

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestInternalOperationContextErrorIsFirstFailure(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		for _, phase := range []string{"generation", "run", "settle"} {
			t.Run(phase+cause.Error(), func(t *testing.T) {
				failure := fmt.Errorf("internal storage request: %w", cause)
				d := testDriver{}
				if phase == "run" {
					d.run = func(context.Context, Scenario, func(json.RawMessage) error) error { return failure }
				} else if phase == "settle" {
					d.settle = func(context.Context) error { return failure }
				}
				runner := testRunner(t, d)
				runner.Clock = &manualClock{}
				generator := &testGenerator{}
				if phase == "generation" {
					generator.fail = failure
					d.validate = func(Scenario, WorkloadLimits) error { t.Fatal("generation failure reached driver"); return nil }
					runner.Driver = d
				}
				result, err := Search(context.Background(), searchConfig(), generator, runner)
				if err == nil || result.StopReason != "first_failure" || result.Completed != 0 || generator.calls != 1 {
					t.Fatalf("internal error reclassified as user/budget stop: %+v %v", result, err)
				}
			})
		}
	}
}

func TestInternalContextFailureOrderingAgainstActualStops(t *testing.T) {
	for _, stopKind := range []string{"cancel", "budget"} {
		for _, failureFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/failureFirst=%t", stopKind, failureFirst), func(t *testing.T) {
				clock := &manualClock{}
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				finished := make(chan struct{})
				driver := &reportingDriver{}
				failure := fmt.Errorf("internal deadline: %w", context.DeadlineExceeded)
				stop := func() {
					if stopKind == "cancel" {
						cancel()
					} else {
						clock.advance(time.Minute)
					}
				}
				driver.run = func(context.Context, Scenario, func(json.RawMessage) error) error {
					defer close(finished)
					if failureFirst {
						driver.report(failure)
						stop()
					} else {
						stop()
						driver.report(failure)
					}
					return failure
				}
				driver.cleanup = func(context.Context) error { <-finished; return nil }
				runner := testRunner(t, driver)
				runner.Clock = clock
				generator := &testGenerator{}
				result, err := Search(parent, searchConfig(), generator, runner)
				expected := "first_failure"
				if !failureFirst {
					if stopKind == "cancel" {
						expected = "canceled"
					} else {
						expected = "budget"
					}
				}
				if err == nil || result.StopReason != expected || result.Completed != 0 || generator.calls != 1 {
					t.Fatalf("wrong earliest cause: %+v %v", result, err)
				}
				var detail caseResult
				raw, readErr := os.ReadFile(filepath.Join(runner.Directory, "case-00000000000000000000", "result.json"))
				if readErr != nil {
					t.Fatal(readErr)
				}
				if err := json.Unmarshal(raw, &detail); err != nil {
					t.Fatal(err)
				}
				if detail.StopReason != expected || !detail.Cleaned {
					t.Fatalf("case outcome differs: %s", raw)
				}
			})
		}
	}
}
