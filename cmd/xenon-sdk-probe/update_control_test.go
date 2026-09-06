package main

import (
	"context"
	"go.temporal.io/sdk/client"
	"reflect"
	"testing"
)

type updateFixture struct {
	options []client.UpdateWorkflowOptions
}

func (f *updateFixture) UpdateWorkflow(_ context.Context, o client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	f.options = append(f.options, o)
	return updateHandle{}, nil
}

type updateHandle struct{}

func (updateHandle) WorkflowID() string { return "workflow" }
func (updateHandle) RunID() string      { return "run" }
func (updateHandle) UpdateID() string   { return "workflow-update" }
func (updateHandle) Get(_ context.Context, value interface{}) error {
	*value.(*string) = "update-ack"
	return nil
}
func TestProofUpdateIdentityReused(t *testing.T) {
	f := new(updateFixture)
	for i := 0; i < 2; i++ {
		if e := proofUpdate(context.Background(), f, "workflow"); e != nil {
			t.Fatal(e)
		}
	}
	if len(f.options) != 2 || !reflect.DeepEqual(f.options[0], f.options[1]) {
		t.Fatal("different update identity")
	}
	o := f.options[0]
	if o.UpdateID != "workflow-update" || o.UpdateName != "set-proof-value" || o.WaitForStage != client.WorkflowUpdateStageCompleted || !reflect.DeepEqual(o.Args, []interface{}{"update-ack"}) {
		t.Fatal(o)
	}
}
