package main

import (
	"context"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"time"
)

type readinessHistoryClient interface {
	GetWorkflowExecutionHistory(context.Context, *workflowservice.GetWorkflowExecutionHistoryRequest, ...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryResponse, error)
}

// Bounded diagnostic sample, never a complete-history or readiness oracle.
// At most 1000 events, no inputs/results/memo/failure payloads are emitted.
func readinessHistory(ctx context.Context, api readinessHistoryClient, namespace, id, run string) map[string]any {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	record := map[string]any{"event": "nexus_readiness_history", "workflow_id": id, "run_id": run, "sample_complete": false}
	r, err := api.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{Namespace: namespace, Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: run}, MaximumPageSize: 1000})
	if err != nil {
		record["rpc_status"] = status.Code(err).String()
		return record
	}
	if r == nil || r.History == nil {
		record["error"] = "missing_history"
		return record
	}
	counts := map[string]int{}
	for _, event := range r.History.Events {
		if event == nil {
			record["error"] = "nil_event"
			return record
		}
		counts[event.EventType.String()]++
		record["last_event_id"] = event.EventId
		record["last_event_type"] = event.EventType.String()
		if a := event.GetWorkflowTaskFailedEventAttributes(); a != nil {
			record["workflow_task_failure_cause"] = a.Cause.String()
		}
	}
	record["event_counts"] = counts
	record["sample_complete"] = len(r.NextPageToken) == 0
	return record
}
