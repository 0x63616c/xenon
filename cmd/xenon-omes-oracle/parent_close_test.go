package main

import (
	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	"os"
	"testing"
)

func TestGeneratedParentCloseTerminationRequiresExactPolicyEvidence(t *testing.T) {
	for _, mutation := range []string{"valid", "root", "policy", "link", "reason", "identity", "parent", "parent-running", "parent-history", "time", "initiated", "first-run", "child-identity", "parent-history-identity"} {
		t.Run(mutation, func(t *testing.T) {
			histories := map[string]*historypb.History{}
			byRun := map[string]runAudit{}
			statuses := map[string]enums.WorkflowExecutionStatus{}
			parentID := "01a08e53-ed39-7adb-bc5a-0a2695129904"
			childID := "01a08e54-0759-7d3f-bd45-d103ad6b3b6f"
			for _, item := range []struct {
				name, id string
				status   enums.WorkflowExecutionStatus
			}{{"parent", parentID, enums.WORKFLOW_EXECUTION_STATUS_COMPLETED}, {"child", childID, enums.WORKFLOW_EXECUTION_STATUS_TERMINATED}} {
				raw, err := os.ReadFile("testdata/parent-close/" + item.name + ".json")
				if err != nil {
					t.Fatal(err)
				}
				h := new(historypb.History)
				if err := protojson.Unmarshal(raw, h); err != nil {
					t.Fatal(err)
				}
				histories[item.id] = h
				byRun[item.id] = runAudit{WorkflowID: h.Events[0].GetWorkflowExecutionStartedEventAttributes().WorkflowId, RunID: item.id}
				statuses[item.id] = item.status
			}
			ph, ch := histories[parentID], histories[childID]
			start := ch.Events[0].GetWorkflowExecutionStartedEventAttributes()
			terminal := ch.Events[len(ch.Events)-1].GetWorkflowExecutionTerminatedEventAttributes()
			switch mutation {
			case "child-identity":
				start.WorkflowId = "wrong"
			case "parent-history-identity":
				ph.Events[0].GetWorkflowExecutionStartedEventAttributes().WorkflowId = "wrong"
			case "root":
				start.ParentWorkflowExecution = nil
			case "policy":
				ph.Events[58].GetStartChildWorkflowExecutionInitiatedEventAttributes().ParentClosePolicy = enums.PARENT_CLOSE_POLICY_ABANDON
			case "link":
				for _, e := range ph.Events {
					if a := e.GetChildWorkflowExecutionStartedEventAttributes(); a != nil && a.InitiatedEventId == 59 {
						a.WorkflowExecution.RunId = "wrong"
					}
				}
			case "reason":
				terminal.Reason = "operator request"
			case "identity":
				terminal.Identity = "operator"
			case "parent":
				start.ParentWorkflowExecution.WorkflowId = "wrong"
			case "parent-running":
				statuses[parentID] = enums.WORKFLOW_EXECUTION_STATUS_RUNNING
			case "parent-history":
				delete(histories, parentID)
			case "time":
				ch.Events[len(ch.Events)-1].EventTime = timestamppb.New(ph.Events[0].EventTime.AsTime())
			case "initiated":
				start.ParentInitiatedEventId = 2
			case "first-run":
				start.FirstExecutionRunId = "wrong"
			}
			counts, next, err := inspectGenerated(byRun[childID], histories, byRun, statuses, map[string]bool{})
			if mutation == "valid" {
				if err != nil || next != "" || counts[enums.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED.String()] != 1 {
					t.Fatal(counts, next, err)
				}
			} else if err == nil {
				t.Fatal("invalid parent-close evidence accepted")
			}
		})
	}
}
