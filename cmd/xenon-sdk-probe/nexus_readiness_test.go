package main

import (
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/server/common"
	"testing"
)

func TestNexusReadinessShardCoverage(t *testing.T) {
	ns := "11111111-1111-1111-1111-111111111111"
	ids, err := readinessWorkflowIDs(ns, "saved-control")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int32]bool{}
	for _, id := range ids {
		seen[common.WorkflowIDToHistoryShard(ns, id, 4)] = true
	}
	if len(ids) != 4 || len(seen) != 4 {
		t.Fatal(ids, seen)
	}
}
func TestNexusReadinessWorkflowEcho(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	s, err := readinessNexusService()
	if err != nil {
		t.Fatal(err)
	}
	env.RegisterNexusService(s)
	env.ExecuteWorkflow(nexusReadinessWorkflow, "saved-nonce")
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatal(env.GetWorkflowError())
	}
	var output string
	if err = env.GetWorkflowResult(&output); err != nil || output != "saved-nonce" {
		t.Fatal(output, err)
	}
}

func TestReadinessWorkerPollerScope(t *testing.T) {
	for _, nexusOnly := range []bool{false, true} {
		o := readinessWorkerOptions(nexusOnly)
		if !o.LocalActivityWorkerOnly || o.DisableWorkflowWorker != nexusOnly {
			t.Fatal(o)
		}
	}
}
