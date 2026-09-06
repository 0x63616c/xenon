package main

import (
	"context"
	"errors"
	"testing"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
)

type historyCall func(context.Context, *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error)

func (call historyCall) GetWorkflowExecutionHistory(ctx context.Context, request *workflowservice.GetWorkflowExecutionHistoryRequest, _ ...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	return call(ctx, request)
}

func TestFailureHistoryKeepsPartialPagesAndExactRun(t *testing.T) {
	calls, pages := 0, 0
	cause := errors.New("backend unavailable")
	client := historyCall(func(ctx context.Context, r *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
		calls++
		if r.Execution.WorkflowId != "workflow" || r.Execution.RunId != "run" || r.WaitNewEvent || r.MaximumPageSize != 100 {
			t.Fatal(r)
		}
		if calls == 2 {
			if string(r.NextPageToken) != "next" {
				t.Fatal("lost pagination")
			}
			return nil, cause
		}
		return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{}, NextPageToken: []byte("next")}, nil
	})
	err := captureFailureHistory(context.Background(), client, "namespace", "workflow", "run", func(v any) error {
		pages++
		if v.(map[string]any)["complete"] != false {
			t.Fatal("partial page marked complete")
		}
		return nil
	})
	if !errors.Is(err, cause) || pages != 1 || calls != 2 {
		t.Fatal(err, pages, calls)
	}
}

func TestFailureHistoryBoundsAndCancellation(t *testing.T) {
	calls := 0
	client := historyCall(func(context.Context, *workflowservice.GetWorkflowExecutionHistoryRequest) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
		calls++
		return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{}, NextPageToken: []byte("loop")}, nil
	})
	if err := captureFailureHistory(context.Background(), client, "n", "w", "r", func(any) error { return nil }); err == nil || calls != 16 {
		t.Fatal(err, calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := captureFailureHistory(ctx, client, "n", "w", "r", func(any) error { return nil }); !errors.Is(err, context.Canceled) || calls != 16 {
		t.Fatal(err, calls)
	}
}
