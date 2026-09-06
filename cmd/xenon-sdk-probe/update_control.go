package main

import (
	"context"
	"fmt"
	"go.temporal.io/sdk/client"
)

type proofUpdater interface {
	UpdateWorkflow(context.Context, client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error)
}

// Both update-only and later control use this exact idempotent update identity.
// Signaling remains in control; this function cannot send a signal.
func proofUpdate(ctx context.Context, c proofUpdater, id string) error {
	handle, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: id, UpdateID: id + "-update", UpdateName: "set-proof-value", Args: []interface{}{"update-ack"}, WaitForStage: client.WorkflowUpdateStageCompleted})
	if err != nil {
		return err
	}
	var value string
	if err = handle.Get(ctx, &value); err != nil {
		return err
	}
	if value != "update-ack" {
		return fmt.Errorf("wrong update result %q", value)
	}
	return nil
}
