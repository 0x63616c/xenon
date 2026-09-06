// Package ministack contains unchanged-SDK workloads used by the real server proof.
package ministack

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type Input struct {
	Stage  int
	Result Result
}
type Result struct {
	Attempt   int    `json:"attempt"`
	Child     string `json:"child"`
	Signal    string `json:"signal"`
	Update    string `json:"update"`
	Continued bool   `json:"continued"`
}

func RetryActivity(ctx context.Context) (int, error) {
	attempt := activity.GetInfo(ctx).Attempt
	if attempt == 1 {
		return 0, fmt.Errorf("declared first-attempt failure")
	}
	return int(attempt), nil
}
func Child(ctx workflow.Context) (string, error) {
	if e := workflow.Sleep(ctx, 100*time.Millisecond); e != nil {
		return "", e
	}
	return "child-completed", nil
}
func DurableWorkflow(ctx workflow.Context, in Input) (Result, error) {
	phase := "starting"
	if e := workflow.SetQueryHandler(ctx, "phase", func() (string, error) { return phase, nil }); e != nil {
		return Result{}, e
	}
	if in.Stage == 1 {
		phase = "continued"
		if e := workflow.Sleep(ctx, 100*time.Millisecond); e != nil {
			return Result{}, e
		}
		in.Result.Continued = true
		return in.Result, nil
	}
	if e := workflow.UpsertTypedSearchAttributes(ctx, temporal.NewSearchAttributeKeyKeyword("XenonProof").ValueSet("durable")); e != nil {
		return Result{}, e
	}
	result := Result{}
	if e := workflow.SetUpdateHandler(ctx, "set-proof-value", func(_ workflow.Context, value string) (string, error) { result.Update = value; return value, nil }); e != nil {
		return Result{}, e
	}
	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 5 * time.Second, RetryPolicy: &temporal.RetryPolicy{InitialInterval: 100 * time.Millisecond, MaximumAttempts: 2}})
	if e := workflow.ExecuteActivity(activityCtx, RetryActivity).Get(ctx, &result.Attempt); e != nil {
		return Result{}, e
	}
	if e := workflow.ExecuteChildWorkflow(ctx, Child).Get(ctx, &result.Child); e != nil {
		return Result{}, e
	}
	if e := workflow.Sleep(ctx, 100*time.Millisecond); e != nil {
		return Result{}, e
	}
	phase = "await-control"
	workflow.GetSignalChannel(ctx, "proof-signal").Receive(ctx, &result.Signal)
	if e := workflow.Await(ctx, func() bool { return result.Update != "" }); e != nil {
		return Result{}, e
	}
	return Result{}, workflow.NewContinueAsNewError(ctx, DurableWorkflow, Input{Stage: 1, Result: result})
}
