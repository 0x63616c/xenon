package main

import (
	"fmt"

	historypb "go.temporal.io/api/history/v1"
)

// childLinks checks both directions independently of visibility list/count.
// A child omitted from both is still required by its parent's saved history.
// Expected root counts and workload semantics remain separate assertions.
func childLinks(runs []runAudit, histories map[string]*historypb.History) error {
	byRun := make(map[string]runAudit, len(runs))
	for _, run := range runs {
		byRun[run.RunID] = run
	}
	linked := map[string]string{}
	for _, parent := range runs {
		h := histories[parent.RunID]
		if h == nil || len(h.Events) == 0 {
			return fmt.Errorf("missing history for child graph")
		}
		for _, event := range h.Events {
			child := event.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution()
			if child == nil {
				continue
			}
			actual, ok := byRun[child.RunId]
			if !ok || actual.WorkflowID != child.WorkflowId || child.RunId == parent.RunID {
				return fmt.Errorf("missing or inconsistent child execution")
			}
			ch := histories[child.RunId]
			if ch == nil || len(ch.Events) == 0 {
				return fmt.Errorf("missing child history")
			}
			owner := ch.Events[0].GetWorkflowExecutionStartedEventAttributes().GetParentWorkflowExecution()
			if owner == nil || owner.RunId != parent.RunID || owner.WorkflowId != parent.WorkflowID {
				return fmt.Errorf("child history has wrong parent")
			}
			if linked[child.RunId] != "" {
				return fmt.Errorf("duplicate child start")
			}
			linked[child.RunId] = parent.RunID
		}
	}
	for _, run := range runs {
		h := histories[run.RunID]
		parent := h.Events[0].GetWorkflowExecutionStartedEventAttributes().GetParentWorkflowExecution()
		if run.PreviousRun != "" {
			previous := histories[run.PreviousRun]
			if previous == nil || len(previous.Events) == 0 {
				return fmt.Errorf("missing child predecessor history")
			}
			prior := previous.Events[0].GetWorkflowExecutionStartedEventAttributes().GetParentWorkflowExecution()
			if (parent == nil) != (prior == nil) || parent.GetWorkflowId() != prior.GetWorkflowId() || parent.GetRunId() != prior.GetRunId() {
				return fmt.Errorf("continued execution changed parent")
			}
		}
		if parent == nil {
			continue
		}
		owner, ok := byRun[parent.RunId]
		if !ok || owner.WorkflowID != parent.WorkflowId {
			return fmt.Errorf("missing or inconsistent parent execution")
		}
		// A continued child keeps its original parent; the parent links the
		// first run, while chains() separately validates the full successor chain.
		first := run
		seen := map[string]bool{}
		for first.PreviousRun != "" {
			if seen[first.RunID] {
				return fmt.Errorf("cyclic child continuation")
			}
			seen[first.RunID] = true
			var ok bool
			first, ok = byRun[first.PreviousRun]
			if !ok {
				return fmt.Errorf("missing child predecessor")
			}
		}
		if linked[first.RunID] != parent.RunId {
			return fmt.Errorf("child lacks parent start event")
		}
		seenParents := map[string]bool{run.RunID: true}
		for ancestor := parent.RunId; ancestor != ""; {
			if seenParents[ancestor] {
				return fmt.Errorf("cyclic child ancestry")
			}
			seenParents[ancestor] = true
			ah := histories[ancestor]
			if ah == nil || len(ah.Events) == 0 {
				return fmt.Errorf("missing ancestor history")
			}
			ancestor = ah.Events[0].GetWorkflowExecutionStartedEventAttributes().GetParentWorkflowExecution().GetRunId()
		}
	}
	return nil
}
