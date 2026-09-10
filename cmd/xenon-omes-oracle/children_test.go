package main

import (
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
)

func childFixture() ([]runAudit, map[string]*historypb.History) {
	parent, child := completed(), completed()
	parent.Events = append(parent.Events, &historypb.HistoryEvent{Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "child", RunId: "child-run"}}}})
	child.Events[0].GetWorkflowExecutionStartedEventAttributes().ParentWorkflowExecution = &commonpb.WorkflowExecution{WorkflowId: "parent", RunId: "parent-run"}
	return []runAudit{{WorkflowID: "parent", RunID: "parent-run"}, {WorkflowID: "child", RunID: "child-run"}}, map[string]*historypb.History{"parent-run": parent, "child-run": child}
}

func TestChildGraphRejectsOmissionsAndWrongLinks(t *testing.T) {
	for _, mutation := range []string{"none", "omitted-child", "omitted-parent", "wrong-parent", "missing-start", "duplicate-start"} {
		t.Run(mutation, func(t *testing.T) {
			runs, histories := childFixture()
			switch mutation {
			case "omitted-child":
				runs = runs[:1]
				delete(histories, "child-run")
			case "omitted-parent":
				runs = runs[1:]
				delete(histories, "parent-run")
			case "wrong-parent":
				histories["child-run"].Events[0].GetWorkflowExecutionStartedEventAttributes().ParentWorkflowExecution.RunId = "other"
			case "missing-start":
				histories["parent-run"].Events = histories["parent-run"].Events[:3]
			case "duplicate-start":
				h := histories["parent-run"]
				h.Events = append(h.Events, h.Events[3])
			}
			err := childLinks(runs, histories)
			if (err == nil) != (mutation == "none") {
				t.Fatalf("graph result: %v", err)
			}
		})
	}
}

func TestChildGraphAllowsCompleteContinuationChain(t *testing.T) {
	runs, histories := childFixture()
	next := completed()
	next.Events[0].GetWorkflowExecutionStartedEventAttributes().ParentWorkflowExecution = &commonpb.WorkflowExecution{WorkflowId: "parent", RunId: "parent-run"}
	runs[1].NextRun = "next-run"
	runs = append(runs, runAudit{WorkflowID: "child", RunID: "next-run", PreviousRun: "child-run"})
	histories["next-run"] = next
	if err := childLinks(runs, histories); err != nil {
		t.Fatal(err)
	}
	next.Events[0].GetWorkflowExecutionStartedEventAttributes().ParentWorkflowExecution = nil
	if err := childLinks(runs, histories); err == nil {
		t.Fatal("continued child lost parent without rejection")
	}
}

func TestChildGraphRejectsCyclicParentage(t *testing.T) {
	runs, histories := childFixture()
	histories["parent-run"].Events[0].GetWorkflowExecutionStartedEventAttributes().ParentWorkflowExecution = &commonpb.WorkflowExecution{WorkflowId: "child", RunId: "child-run"}
	h := histories["child-run"]
	h.Events = append(h.Events, &historypb.HistoryEvent{Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "parent", RunId: "parent-run"}}}})
	if err := childLinks(runs, histories); err == nil {
		t.Fatal("cyclic parentage accepted")
	}
}
