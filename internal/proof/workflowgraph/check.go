package workflowgraph

import (
	"fmt"
	"strings"

	enums "go.temporal.io/api/enums/v1"
	history "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/proto"
)

type Execution struct {
	WorkflowID, RunID string
	History           *history.History
}

// Check consumes complete histories, never visibility-derived expected counts.
// It binds opaque server/Omes IDs to the input's logical graph and rejects extra
// executions as well as omitted effects that never appeared in any history.
func Check(g *Graph, rootPrefix string, runs []Execution) error {
	fail := func(id string) error { return fmt.Errorf("expected_graph/%s", id) }
	if g == nil || len(g.Nodes) == 0 || len(runs) != len(g.Nodes) {
		return fail("execution_inventory")
	}
	byID := map[string]Execution{}
	root := ""
	for _, r := range runs {
		if r.RunID == "" || r.WorkflowID == "" || byID[r.RunID].RunID != "" || r.History == nil || len(r.History.Events) < 2 {
			return fail("history_shape")
		}
		for i, e := range r.History.Events {
			if e == nil || e.EventId != int64(i+1) {
				return fail("history_shape")
			}
		}
		first := r.History.Events[0]
		s := first.GetWorkflowExecutionStartedEventAttributes()
		if first.EventType != enums.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED || s == nil {
			return fail("history_shape")
		}
		if s.ParentWorkflowExecution == nil && s.ContinuedExecutionRunId == "" {
			if root != "" || rootPrefix == "" || !strings.HasPrefix(r.WorkflowID, rootPrefix) || !strings.HasSuffix(r.WorkflowID, "-1") {
				return fail("root_identity")
			}
			root = r.RunID
		}
		byID[r.RunID] = r
	}
	if root == "" {
		return fail("root_identity")
	}
	bound := map[int]string{}
	used := map[string]bool{}
	var check func(int, string) error
	check = func(index int, runID string) error {
		if index < 0 || index >= len(g.Nodes) || used[runID] {
			return fail("execution_identity")
		}
		r, ok := byID[runID]
		if !ok {
			return fail("omitted_execution")
		}
		used[runID] = true
		bound[index] = runID
		n := g.Nodes[index]
		s := r.History.Events[0].GetWorkflowExecutionStartedEventAttributes()
		if n.WorkflowID != "" && r.WorkflowID != n.WorkflowID {
			return fail("execution_identity")
		}
		if len(s.GetInput().GetPayloads()) != 1 || !matchesInput(s.Input.Payloads[0], n.Input, index == 0) {
			return fail("input_mismatch")
		}
		if n.Parent < 0 {
			if s.ParentWorkflowExecution != nil {
				return fail("parent_identity")
			}
		} else {
			p := byID[bound[n.Parent]]
			if s.GetParentWorkflowExecution().GetRunId() != p.RunID || s.GetParentWorkflowExecution().GetWorkflowId() != p.WorkflowID {
				return fail("parent_identity")
			}
		}
		if n.Previous < 0 {
			if s.ContinuedExecutionRunId != "" {
				return fail("continuation_identity")
			}
		} else {
			p := byID[bound[n.Previous]]
			if s.ContinuedExecutionRunId != p.RunID || r.WorkflowID != p.WorkflowID {
				return fail("continuation_identity")
			}
		}
		children := map[string]string{}
		for _, e := range r.History.Events {
			if a := e.GetChildWorkflowExecutionStartedEventAttributes(); a != nil {
				child := a.GetWorkflowExecution()
				if child.GetWorkflowId() == "" || child.GetRunId() == "" || children[child.WorkflowId] != "" {
					return fail("child_identity")
				}
				children[child.WorkflowId] = child.RunId
			}
			if e.GetNexusOperationScheduledEventAttributes() != nil || e.GetActivityTaskScheduledEventAttributes() != nil || e.GetWorkflowExecutionSignaledEventAttributes() != nil {
				return fail("unsupported_effect")
			}
		}
		if len(children) != len(n.Children) {
			return fail("child_inventory")
		}
		for _, child := range n.Children {
			if e := check(child, children[g.Nodes[child].WorkflowID]); e != nil {
				return e
			}
		}
		if err := checkAwaitedChildren(g, n, r.History, bound); err != nil {
			return err
		}
		last := r.History.Events[len(r.History.Events)-1]
		if n.Next >= 0 {
			a := last.GetWorkflowExecutionContinuedAsNewEventAttributes()
			if last.EventType != enums.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW || a == nil || a.NewExecutionRunId == "" {
				return fail("omitted_continuation")
			}
			if len(a.GetInput().GetPayloads()) != 1 || !matchesInput(a.Input.Payloads[0], g.Nodes[n.Next].Input, false) {
				return fail("continuation_input")
			}
			return check(n.Next, a.NewExecutionRunId)
		}
		a := last.GetWorkflowExecutionCompletedEventAttributes()
		if last.EventType != enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED || a == nil {
			return fail("terminal_status")
		}
		if len(a.GetResult().GetPayloads()) != 1 || !proto.Equal(unwrap(a.Result.Payloads[0]), n.Result) {
			return fail("corrupt_result")
		}
		return nil
	}
	if e := check(0, root); e != nil {
		return e
	}
	if len(used) != len(runs) {
		return fail("execution_inventory")
	}
	return nil
}
