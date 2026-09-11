package main

import (
	"fmt"
	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
)

// A generated child may intentionally outlive its parent. The recorded policy,
// exact reciprocal link, valid parent closure and server termination must all agree.
func inspectGenerated(run runAudit, histories map[string]*historypb.History, byRun map[string]runAudit, statuses map[string]enums.WorkflowExecutionStatus, visiting map[string]bool) (map[string]int, string, error) {
	h := histories[run.RunID]
	if statuses[run.RunID] != enums.WORKFLOW_EXECUTION_STATUS_TERMINATED {
		return inspect(h, statuses[run.RunID])
	}
	counts, err := historyShape(h)
	if err != nil {
		return nil, "", err
	}
	if visiting[run.RunID] || len(visiting) >= 1000 {
		return nil, "", fmt.Errorf("cyclic or excessive parent-close ancestry")
	}
	visiting[run.RunID] = true
	defer delete(visiting, run.RunID)
	start := h.Events[0].GetWorkflowExecutionStartedEventAttributes()
	last := h.Events[len(h.Events)-1]
	termination := last.GetWorkflowExecutionTerminatedEventAttributes()
	if last.EventType != enums.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED || termination == nil || termination.Reason != "by parent close policy" || termination.Identity != "history-service" {
		return nil, "", fmt.Errorf("termination is not recorded parent-close policy")
	}
	if start.WorkflowId != run.WorkflowID {
		return nil, "", fmt.Errorf("parent-close child workflow identity mismatch")
	}
	parent := start.GetParentWorkflowExecution()
	if parent == nil || parent.RunId == "" || parent.WorkflowId == "" {
		return nil, "", fmt.Errorf("terminated root is not a parent-close child")
	}
	parentRun, ok := byRun[parent.RunId]
	if !ok || parentRun.WorkflowID != parent.WorkflowId {
		return nil, "", fmt.Errorf("parent-close parent identity mismatch")
	}
	if _, _, err := inspectGenerated(parentRun, histories, byRun, statuses, visiting); err != nil {
		return nil, "", fmt.Errorf("parent-close parent invalid: %w", err)
	}
	ph := histories[parent.RunId]
	if ph.Events[0].GetWorkflowExecutionStartedEventAttributes().WorkflowId != parentRun.WorkflowID {
		return nil, "", fmt.Errorf("parent-close parent history identity mismatch")
	}
	parentEnd := ph.Events[len(ph.Events)-1]
	if parentEnd.EventTime == nil || last.EventTime == nil || parentEnd.EventTime.CheckValid() != nil || last.EventTime.CheckValid() != nil || last.EventTime.AsTime().Before(parentEnd.EventTime.AsTime()) {
		return nil, "", fmt.Errorf("parent-close termination precedes valid parent closure")
	}
	firstRun := start.FirstExecutionRunId
	if firstRun == "" {
		firstRun = run.RunID
	}
	policy, linked := 0, 0
	for _, event := range ph.Events {
		if event.EventId == start.ParentInitiatedEventId {
			a := event.GetStartChildWorkflowExecutionInitiatedEventAttributes()
			if a != nil && a.WorkflowId == run.WorkflowID && a.ParentClosePolicy == enums.PARENT_CLOSE_POLICY_TERMINATE {
				policy++
			}
		}
		a := event.GetChildWorkflowExecutionStartedEventAttributes()
		if a != nil && a.InitiatedEventId == start.ParentInitiatedEventId && a.GetWorkflowExecution().GetWorkflowId() == run.WorkflowID && a.GetWorkflowExecution().GetRunId() == firstRun {
			linked++
		}
	}
	if policy != 1 || linked != 1 {
		return nil, "", fmt.Errorf("parent-close child lacks exact terminate policy and start link")
	}
	return counts, "", nil
}
