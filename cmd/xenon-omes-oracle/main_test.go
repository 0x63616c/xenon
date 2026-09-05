package main

import (
	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"testing"
)

func completed() *historypb.History {
	return &historypb.History{Events: []*historypb.HistoryEvent{
		{EventId: 1, EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{}}},
		{EventId: 2, EventType: enums.EVENT_TYPE_ACTIVITY_TASK_COMPLETED},
		{EventId: 3, EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{}}},
	}}
}

func TestHistoryAuditRejectsIncompleteAndMismatchedEvidence(t *testing.T) {
	h := completed()
	counts, next, err := inspect(h, enums.WORKFLOW_EXECUTION_STATUS_COMPLETED)
	if err != nil || next != "" || counts[enums.EVENT_TYPE_ACTIVITY_TASK_COMPLETED.String()] != 1 {
		t.Fatalf("valid history: %v", err)
	}
	h.Events[1].EventId = 1
	if _, _, err = inspect(h, enums.WORKFLOW_EXECUTION_STATUS_COMPLETED); err == nil {
		t.Fatal("duplicate event accepted")
	}
	h = completed()
	h.Events = h.Events[:2]
	if _, _, err = inspect(h, enums.WORKFLOW_EXECUTION_STATUS_COMPLETED); err == nil {
		t.Fatal("truncated history accepted")
	}
	h = completed()
	if _, _, err = inspect(h, enums.WORKFLOW_EXECUTION_STATUS_RUNNING); err == nil {
		t.Fatal("running row accepted as closed")
	}
	h.Events[2] = &historypb.HistoryEvent{EventId: 3, EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{NewExecutionRunId: "second"}}}
	if _, next, err = inspect(h, enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW); err != nil || next != "second" {
		t.Fatalf("successor: %q %v", next, err)
	}
	if _, _, err = inspect(h, enums.WORKFLOW_EXECUTION_STATUS_COMPLETED); err == nil {
		t.Fatal("visibility terminal mismatch accepted")
	}
}

func TestRunGraphRequiresEverySuccessorAndRejectsCycles(t *testing.T) {
	runs := []runAudit{{WorkflowID: "w", RunID: "first", NextRun: "second"}, {WorkflowID: "w", RunID: "second", PreviousRun: "first"}}
	if err := chains(runs); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]runAudit{
		runs[:1],
		runs[1:], // A final run still points at the omitted predecessor.
		{runs[0], {WorkflowID: "w", RunID: "second"}},
		{runs[0], runs[0], runs[1]},
		{runs[0], {WorkflowID: "other", RunID: "second"}},
		{{WorkflowID: "w", RunID: "first", PreviousRun: "second", NextRun: "second"}, {WorkflowID: "w", RunID: "second", PreviousRun: "first", NextRun: "first"}},
		{runs[0], runs[1], {WorkflowID: "w", RunID: "third", NextRun: "second"}},
	} {
		if err := chains(bad); err == nil {
			t.Fatalf("invalid graph accepted: %+v", bad)
		}
	}
}
