package workflowgraph

import (
	"fmt"

	enums "go.temporal.io/api/enums/v1"
	history "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/proto"
)

// checkAwaitedChildren checks parent-side command/completion causality. Merely
// observing that a child eventually completed cannot prove its parent awaited it.
func checkAwaitedChildren(g *Graph, n Node, h *history.History, bound map[int]string) error {
	fail := func(id string) error { return fmt.Errorf("expected_graph/%s", id) }
	initiated := map[string]*history.HistoryEvent{}
	started := map[string]*history.HistoryEvent{}
	completed := map[string]*history.HistoryEvent{}
	terminal := int64(len(h.Events) + 1)
	for _, e := range h.Events {
		if e.EventType == enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED || e.EventType == enums.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW {
			if e.EventId < terminal {
				terminal = e.EventId
			}
		}
		if a := e.GetStartChildWorkflowExecutionInitiatedEventAttributes(); a != nil {
			if e.EventType != enums.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED || a.WorkflowId == "" || initiated[a.WorkflowId] != nil {
				return fail("child_initiation")
			}
			initiated[a.WorkflowId] = e
		}
		if a := e.GetChildWorkflowExecutionStartedEventAttributes(); a != nil {
			id := a.GetWorkflowExecution().GetWorkflowId()
			if e.EventType != enums.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED || id == "" || started[id] != nil {
				return fail("child_identity")
			}
			started[id] = e
		}
		if a := e.GetChildWorkflowExecutionCompletedEventAttributes(); a != nil {
			id := a.GetWorkflowExecution().GetWorkflowId()
			if e.EventType != enums.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED || id == "" || completed[id] != nil {
				return fail("child_completion")
			}
			completed[id] = e
		}
	}
	if len(initiated) != len(n.Children) {
		return fail("child_initiation")
	}
	if len(completed) != len(n.Children) {
		return fail("child_completion")
	}
	previous := int64(0)
	for _, child := range n.Children {
		id := g.Nodes[child].WorkflowID
		init, start, complete := initiated[id], started[id], completed[id]
		if init == nil {
			return fail("child_initiation")
		}
		if start == nil || complete == nil {
			return fail("child_completion")
		}
		ia := init.GetStartChildWorkflowExecutionInitiatedEventAttributes()
		sa := start.GetChildWorkflowExecutionStartedEventAttributes()
		ca := complete.GetChildWorkflowExecutionCompletedEventAttributes()
		if init.EventId <= previous || start.EventId <= init.EventId || complete.EventId <= start.EventId || complete.EventId >= terminal {
			return fail("serial_child_order")
		}
		if sa.InitiatedEventId != init.EventId || ca.InitiatedEventId != init.EventId || ca.StartedEventId != start.EventId {
			return fail("child_completion_link")
		}
		if len(ia.GetInput().GetPayloads()) != 1 || !matchesInput(ia.Input.Payloads[0], g.Nodes[child].Input, false) {
			return fail("child_initiation_input")
		}
		// Temporal links completion to the final run of a continued child, while its
		// StartedEventId still refers to the original start in the parent history.
		final := child
		for g.Nodes[final].Next >= 0 {
			final = g.Nodes[final].Next
		}
		if ca.GetWorkflowExecution().GetRunId() != bound[final] {
			return fail("child_completion_link")
		}
		if len(ca.GetResult().GetPayloads()) != 1 || !proto.Equal(unwrap(ca.Result.Payloads[0]), g.Nodes[final].Result) {
			return fail("child_completion_result")
		}
		previous = complete.EventId
	}
	return nil
}
