package main

import (
	"context"
	"errors"
	enums "go.temporal.io/api/enums/v1"
	history "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"testing"
)

func TestCompleteHistoryPaginationControls(t *testing.T) {
	for _, mode := range []string{"complete", "omitted", "cycle", "unavailable", "nil"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			h, e := readCompleteHistory(context.Background(), func(ctx context.Context, token []byte) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
				calls++
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("missing request deadline")
				}
				if calls == 1 {
					if len(token) != 0 {
						t.Fatal(token)
					}
					return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &history.History{Events: []*history.HistoryEvent{{EventId: 1, EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, Attributes: &history.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &history.WorkflowExecutionStartedEventAttributes{}}}}}, NextPageToken: []byte("next")}, nil
				}
				if string(token) != "next" {
					t.Fatal(token)
				}
				if mode == "unavailable" {
					return nil, errors.New("unavailable")
				}
				if mode == "nil" {
					return nil, nil
				}
				response := &workflowservice.GetWorkflowExecutionHistoryResponse{History: &history.History{Events: []*history.HistoryEvent{{EventId: 2}, {EventId: 3}}}}
				if mode == "omitted" {
					response.History.Events = response.History.Events[1:]
				}
				if mode == "cycle" {
					response.NextPageToken = []byte("next")
				}
				return response, nil
			})
			if calls != 2 {
				t.Fatal(calls)
			}
			if mode == "complete" {
				if e != nil || len(h.Events) != 3 {
					t.Fatal(h, e)
				}
				if _, e = historyShape(h); e != nil {
					t.Fatal(e)
				}
			} else if mode == "omitted" {
				if e != nil {
					t.Fatal(e)
				}
				if _, e = historyShape(h); e == nil {
					t.Fatal("missing page event accepted")
				}
			} else if e == nil {
				t.Fatal("incomplete pagination accepted")
			}
		})
	}
}
