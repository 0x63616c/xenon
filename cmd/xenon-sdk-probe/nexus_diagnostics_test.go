package main

import (
	"bytes"
	"context"
	"encoding/json"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
	"testing"
)

type diagnosticHistory struct{ nilResponse bool }

func (d diagnosticHistory) GetWorkflowExecutionHistory(ctx context.Context, r *workflowservice.GetWorkflowExecutionHistoryRequest, _ ...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	if d.nilResponse {
		return nil, nil
	}
	return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED}, {EventId: 2, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED}}}}, nil
}
func TestNexusHistoryDiagnostic(t *testing.T) {
	r := readinessHistory(context.Background(), diagnosticHistory{}, "ns", "workflow", "run")
	if r["last_event_id"] != int64(2) || r["last_event_type"] != "WorkflowTaskScheduled" || r["sample_complete"] != true {
		t.Fatal(r)
	}
	raw, _ := json.Marshal(r)
	if bytes.Contains(raw, []byte("attributes")) {
		t.Fatal("payload fields leaked")
	}
	r = readinessHistory(context.Background(), diagnosticHistory{true}, "ns", "workflow", "run")
	if r["sample_complete"] != false || r["error"] != "missing_history" {
		t.Fatal(r)
	}
}
