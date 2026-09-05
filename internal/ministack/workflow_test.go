package ministack

import (
	"errors"
	"testing"
	"time"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// This verifies workload construction only; the server history oracle runs against
// real Temporal in the controller and cannot be replaced by this SDK test suite.
func TestWorkloadConstruction(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(Child)
	env.RegisterActivity(RetryActivity)
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflowNoRejection("set-proof-value", "update", t, "update-ack")
		env.SignalWorkflow("proof-signal", "signal-ack")
	}, 2*time.Second)
	env.ExecuteWorkflow(DurableWorkflow, Input{})
	var continued *workflow.ContinueAsNewError
	if !errors.As(env.GetWorkflowError(), &continued) {
		t.Fatal("expected continue-as-new", env.GetWorkflowError())
	}
	var next Input
	if e := converter.GetDefaultDataConverter().FromPayloads(continued.Input, &next); e != nil {
		t.Fatal(e)
	}
	final := suite.NewTestWorkflowEnvironment()
	final.ExecuteWorkflow(DurableWorkflow, next)
	if e := final.GetWorkflowError(); e != nil {
		t.Fatal(e)
	}
	var result Result
	if e := final.GetWorkflowResult(&result); e != nil {
		t.Fatal(e)
	}
	if result != (Result{Attempt: 2, Child: "child-completed", Signal: "signal-ack", Update: "update-ack", Continued: true}) {
		t.Fatal(result)
	}
}
