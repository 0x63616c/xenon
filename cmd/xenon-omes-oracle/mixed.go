package main

import (
	"encoding/json"
	"fmt"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/proto"
	"regexp"
	"strings"
)

// mixedSemantics checks the frozen 40 x 4, CAN every 2 profile. Counts apply to
// successful persisted actions, not intermediate retry failures (Temporal elides
// those while retrying and records the eventual ActivityTaskStarted.Attempt).
func mixedSemantics(runs []runAudit, histories map[string]*historypb.History) error {
	if len(runs) != 240 || len(histories) != 240 {
		return fmt.Errorf("mixed requires 80 parent runs and 160 child runs")
	}
	byRun := map[string]runAudit{}
	roots := map[string]int{}
	successors := map[string]int{}
	executionID := ""
	rootPattern := regexp.MustCompile(`^w-xenon-full-mixed-([0-9a-f]{16})-[0-9]+$`)
	children := map[string]bool{}
	for _, r := range runs {
		byRun[r.RunID] = r
		if !strings.Contains(r.WorkflowID, "/child-") {
			m := rootPattern.FindStringSubmatch(r.WorkflowID)
			if m == nil {
				return fmt.Errorf("invalid mixed root identity")
			}
			if executionID != "" && executionID != m[1] {
				return fmt.Errorf("mixed execution IDs differ")
			}
			executionID = m[1]
			roots[r.WorkflowID]++
			if r.NextRun != "" {
				successors[r.WorkflowID]++
			}
		}
	}
	if len(roots) != 40 {
		return fmt.Errorf("mixed root count")
	}
	for wid, n := range roots {
		if n != 2 || successors[wid] != 1 {
			return fmt.Errorf("mixed root must have two runs")
		}
	}
	for _, r := range runs {
		h := histories[r.RunID]
		if h == nil || len(h.Events) < 2 {
			return fmt.Errorf("missing mixed history")
		}
		start := h.Events[0].GetWorkflowExecutionStartedEventAttributes()
		if start == nil {
			return fmt.Errorf("missing mixed start")
		}
		last := h.Events[len(h.Events)-1]
		if completed := last.GetWorkflowExecutionCompletedEventAttributes(); completed != nil {
			if completed.Result == nil || len(completed.Result.Payloads) != 1 || !proto.Equal(completed.Result.Payloads[0], &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("_passthrough")}}) {
				return fmt.Errorf("mixed result is not pinned empty passthrough payload")
			}
		}
		scheduled := map[int64]string{}
		attempts := map[int64]int32{}
		completed := map[int64]bool{}
		initiated := map[int64]string{}
		started := map[int64]*commonpb.WorkflowExecution{}
		ended := map[int64]*commonpb.WorkflowExecution{}
		accepted := map[int64]bool{}
		updates := map[int64]bool{}
		variants := map[string]int{}
		signals, timers := 0, 0
		for _, e := range h.Events {
			if a := e.GetActivityTaskScheduledEventAttributes(); a != nil {
				scheduled[e.EventId] = a.GetActivityType().GetName()
			}
			if a := e.GetActivityTaskStartedEventAttributes(); a != nil {
				attempts[a.ScheduledEventId] = a.Attempt
			}
			if a := e.GetActivityTaskCompletedEventAttributes(); a != nil {
				completed[a.ScheduledEventId] = true
			}
			if a := e.GetStartChildWorkflowExecutionInitiatedEventAttributes(); a != nil {
				initiated[e.EventId] = a.WorkflowId
			}
			if a := e.GetChildWorkflowExecutionStartedEventAttributes(); a != nil {
				started[a.InitiatedEventId] = a.WorkflowExecution
			}
			if a := e.GetChildWorkflowExecutionCompletedEventAttributes(); a != nil {
				ended[a.InitiatedEventId] = a.WorkflowExecution
			}
			if a := e.GetWorkflowExecutionSignaledEventAttributes(); a != nil && a.SignalName == "do_actions_signal" {
				signals++
			}
			if e.GetTimerFiredEventAttributes() != nil {
				timers++
			}
			if a := e.GetWorkflowExecutionUpdateAcceptedEventAttributes(); a != nil && a.GetAcceptedRequest().GetInput().GetName() == "do_actions_update" {
				variant, err := mixedUpdateVariant(a.AcceptedRequest.Input.Args)
				if err != nil {
					return err
				}
				variants[variant]++
				accepted[e.EventId] = true
			}
			if a := e.GetWorkflowExecutionUpdateCompletedEventAttributes(); a != nil {
				if a.GetOutcome().GetSuccess() == nil {
					return fmt.Errorf("mixed update failed")
				}
				updates[a.AcceptedEventId] = true
			}
		}
		if strings.Contains(r.WorkflowID, "/child-") {
			count := 0
			for id, name := range scheduled {
				if name == "payload" && completed[id] {
					count++
				}
			}
			if count != 3 {
				return fmt.Errorf("child lacks three completed payload activities")
			}
			if start.ParentWorkflowExecution == nil {
				return fmt.Errorf("child lacks parent identity")
			}
			continue
		}
		if signals < 2 || timers < 4 || len(accepted) < 6 {
			return fmt.Errorf("mixed run lacks signals, timers or accepted updates")
		}
		for _, variant := range []string{"timer", "remote", "local"} {
			if variants[variant] < 2 {
				return fmt.Errorf("missing mixed update variant %s", variant)
			}
		}
		for id := range accepted {
			if !updates[id] {
				return fmt.Errorf("accepted update not completed")
			}
		}
		for _, name := range []string{"retryable_error", "timeout", "heartbeat"} {
			count := 0
			for id, typ := range scheduled {
				if typ == name {
					if !completed[id] || attempts[id] < 2 {
						return fmt.Errorf("retry case %s did not complete after retry", name)
					}
					count++
				}
			}
			if count != 2 {
				return fmt.Errorf("retry case %s count", name)
			}
		}
		if len(initiated) != 2 || len(started) != 2 || len(ended) != 2 {
			return fmt.Errorf("parent lacks exact child lifecycle")
		}
		for id, wid := range initiated {
			s, e := started[id], ended[id]
			if s == nil || e == nil || !proto.Equal(s, e) || s.WorkflowId != wid {
				return fmt.Errorf("child start/completion identity mismatch")
			}
			child, ok := byRun[s.RunId]
			if !ok || child.WorkflowID != wid || children[s.RunId] {
				return fmt.Errorf("missing or duplicate child run")
			}
			ch := histories[s.RunId]
			if ch == nil || len(ch.Events) == 0 {
				return fmt.Errorf("missing child history")
			}
			cs := ch.Events[0].GetWorkflowExecutionStartedEventAttributes()
			if cs == nil || cs.ParentWorkflowExecution == nil || cs.ParentWorkflowExecution.WorkflowId != r.WorkflowID || cs.ParentWorkflowExecution.RunId != r.RunID {
				return fmt.Errorf("child reverse parent mismatch")
			}
			children[s.RunId] = true
		}
	}
	if len(children) != 160 {
		return fmt.Errorf("unreferenced mixed child")
	}
	return chains(runs)
}

// Omes uses its ProtoJSON converter for DoActionsUpdate (raw result payloads
// instead use the explicit _passthrough converter).
func mixedUpdateVariant(args *commonpb.Payloads) (string, error) {
	if args == nil || len(args.Payloads) != 1 || args.Payloads[0] == nil || string(args.Payloads[0].Metadata["encoding"]) != "json/protobuf" {
		return "", fmt.Errorf("unexpected update argument encoding")
	}
	var input struct {
		DoActions struct {
			Actions []struct {
				Timer        json.RawMessage `json:"timer"`
				ExecActivity *struct {
					Payload *struct {
						BytesToReturn int `json:"bytesToReturn"`
					} `json:"payload"`
					IsLocal json.RawMessage `json:"isLocal"`
				} `json:"execActivity"`
			} `json:"actions"`
		} `json:"doActions"`
	}
	if err := json.Unmarshal(args.Payloads[0].Data, &input); err != nil {
		return "", err
	}
	if len(input.DoActions.Actions) == 0 {
		return "empty-start", nil
	}
	if len(input.DoActions.Actions) != 1 {
		return "", fmt.Errorf("unexpected update actions")
	}
	a := input.DoActions.Actions[0]
	if len(a.Timer) > 0 {
		return "timer", nil
	}
	if a.ExecActivity == nil || a.ExecActivity.Payload == nil || a.ExecActivity.Payload.BytesToReturn != 256 {
		return "", fmt.Errorf("unexpected update payload action")
	}
	if len(a.ExecActivity.IsLocal) > 0 {
		return "local", nil
	}
	return "remote", nil
}
