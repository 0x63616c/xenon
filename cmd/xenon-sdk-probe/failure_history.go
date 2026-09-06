package main

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

type historyObserver interface {
	GetWorkflowExecutionHistory(context.Context, *workflowservice.GetWorkflowExecutionHistoryRequest, ...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryResponse, error)
}

// Diagnostic pages are emitted as they arrive, so a later timeout preserves the
// partial history. This never decides workload success or follows a new run.
func captureFailureHistory(ctx context.Context, client historyObserver, namespace, workflow, run string, emit func(any) error) error {
	if namespace == "" || workflow == "" || run == "" {
		return fmt.Errorf("failure history requires exact namespace/workflow/run")
	}
	request := &workflowservice.GetWorkflowExecutionHistoryRequest{Namespace: namespace, Execution: &commonpb.WorkflowExecution{WorkflowId: workflow, RunId: run}, MaximumPageSize: 100}
	bytes := 0
	for page := 0; page < 16; page++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		response, err := client.GetWorkflowExecutionHistory(ctx, request)
		if err != nil {
			return err
		}
		if response == nil || response.History == nil {
			return fmt.Errorf("missing diagnostic history")
		}
		raw, err := protojson.Marshal(response.History)
		if err != nil {
			return err
		}
		bytes += len(raw)
		if bytes > 4<<20 {
			return fmt.Errorf("diagnostic history exceeds 4 MiB")
		}
		if err := emit(map[string]any{"event": "failure-history-page", "workflow_id": workflow, "run_id": run, "page": page, "history_json": string(raw), "complete": len(response.NextPageToken) == 0}); err != nil {
			return err
		}
		if len(response.NextPageToken) == 0 {
			return ctx.Err()
		}
		request.NextPageToken = response.NextPageToken
	}
	return fmt.Errorf("diagnostic history exceeds 16 pages")
}
