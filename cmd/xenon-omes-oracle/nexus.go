package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
)

type nexusInput struct {
	ConflictPolicy    string            `json:"handlerWorkflowIdConflictPolicy"`
	Input             string            `json:"input"`
	BeforeActions     []json.RawMessage `json:"beforeActions"`
	HandlerWorkflowID string            `json:"handlerWorkflowId"`
	WaitForSignal     bool              `json:"waitForSignal"`
}

func parseNexusInput(p *commonpb.Payload) (nexusInput, error) {
	var input nexusInput
	if p == nil || string(p.Metadata["encoding"]) != "json/protobuf" {
		return input, fmt.Errorf("Nexus input encoding")
	}
	if err := json.Unmarshal(p.Data, &input); err != nil {
		return input, err
	}
	return input, nil
}
func stringResult(p *commonpb.Payload, want string) bool {
	if p == nil || string(p.Metadata["encoding"]) != "json/plain" {
		return false
	}
	var got string
	return json.Unmarshal(p.Data, &got) == nil && got == want
}
func nexusKind(input nexusInput) string {
	if input.ConflictPolicy == "WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING" && input.WaitForSignal && strings.HasPrefix(input.HandlerWorkflowID, "nexus-attach-handler-") && input.Input == "hello" && len(input.BeforeActions) == 0 {
		return "attach"
	}
	if !input.WaitForSignal && input.HandlerWorkflowID == "" && input.Input == "hello" && len(input.BeforeActions) == 0 {
		return "echo"
	}
	if !input.WaitForSignal && input.HandlerWorkflowID == "" && input.Input == "" && len(input.BeforeActions) == 1 {
		var action map[string]any
		if json.Unmarshal(input.BeforeActions[0], &action) != nil {
			return "unknown"
		}
		want := `{"actions":[{"awaitWorkflowState":{"key":"never","value":"resolves"}}]}`
		var expected map[string]any
		_ = json.Unmarshal([]byte(want), &expected)
		actual, _ := json.Marshal(action)
		frozen, _ := json.Marshal(expected)
		if string(actual) == string(frozen) {
			return "cancel"
		}
		return "unknown"
	}
	return "unknown"
}

// mixedAllSemantics captures the entire task queue, but the baseline 240 runs
// exclude Nexus handlers. No unknown visible run is silently discarded.
func mixedAllSemantics(runs []runAudit, histories map[string]*historypb.History, namespace string) error {
	var baseline []runAudit
	baseHist := map[string]*historypb.History{}
	handlers := map[string]runAudit{}
	for _, r := range runs {
		h := histories[r.RunID]
		if h == nil || len(h.Events) == 0 {
			return fmt.Errorf("missing history")
		}
		start := h.Events[0].GetWorkflowExecutionStartedEventAttributes()
		if start == nil {
			return fmt.Errorf("missing start")
		}
		if start.GetWorkflowType().GetName() == "NexusHandlerWorkflow" {
			if _, duplicate := handlers[r.WorkflowID]; duplicate {
				return fmt.Errorf("multiple Nexus runs per handler")
			}
			handlers[r.WorkflowID] = r
		} else {
			baseline = append(baseline, r)
			baseHist[r.RunID] = h
		}
	}
	if err := mixedSemantics(baseline, baseHist); err != nil {
		return err
	}
	if len(handlers) != 480 {
		return fmt.Errorf("pinned mixed profile requires 480 Nexus handlers, got %d", len(handlers))
	}
	referenced := map[string]int{}
	handlerOwner := map[string]string{}
	for _, r := range baseline {
		if strings.Contains(r.WorkflowID, "/child-") {
			continue
		}
		h := histories[r.RunID]
		schedules := map[int64]*historypb.NexusOperationScheduledEventAttributes{}
		starts := map[int64]*historypb.HistoryEvent{}
		ends := map[int64]*historypb.HistoryEvent{}
		cancels := map[int64]bool{}
		signalTargets := map[string]int{}
		for _, event := range h.Events {
			if a := event.GetNexusOperationScheduledEventAttributes(); a != nil {
				schedules[event.EventId] = a
			}
			if a := event.GetNexusOperationStartedEventAttributes(); a != nil {
				starts[a.ScheduledEventId] = event
			}
			if a := event.GetNexusOperationCompletedEventAttributes(); a != nil {
				ends[a.ScheduledEventId] = event
			}
			if a := event.GetNexusOperationCanceledEventAttributes(); a != nil {
				ends[a.ScheduledEventId] = event
			}
			if a := event.GetNexusOperationFailedEventAttributes(); a != nil {
				return fmt.Errorf("unexpected failed Nexus operation")
			}
			if a := event.GetNexusOperationTimedOutEventAttributes(); a != nil {
				return fmt.Errorf("unexpected timed out Nexus operation")
			}
			if a := event.GetNexusOperationCancelRequestedEventAttributes(); a != nil {
				cancels[a.ScheduledEventId] = true
			}
			if a := event.GetSignalExternalWorkflowExecutionInitiatedEventAttributes(); a != nil && a.SignalName == "unblock" {
				signalTargets[a.GetWorkflowExecution().GetWorkflowId()]++
			}
		}
		if len(schedules) != 12 || len(ends) != 12 {
			return fmt.Errorf("each parent run requires 12 finished Nexus operations")
		}
		kinds := map[string]int{}
		for id, a := range schedules {
			if a.Service != "kitchen-sink" || a.Endpoint != "test-nexus-endpoint-xenon-full-mixed" {
				return fmt.Errorf("unexpected Nexus service/endpoint")
			}
			input, err := parseNexusInput(a.Input)
			if err != nil {
				return err
			}
			kind := nexusKind(input)
			ended := ends[id]
			if ended == nil {
				return fmt.Errorf("Nexus operation lacks terminal event")
			}
			if a.Operation == "echo-sync" {
				if kind != "echo" || starts[id] != nil || !stringResult(ended.GetNexusOperationCompletedEventAttributes().GetResult(), "hello") {
					return fmt.Errorf("sync Nexus outcome")
				}
				kinds["sync"]++
				continue
			}
			if a.Operation != "echo-async" || kind == "unknown" {
				return fmt.Errorf("unknown Nexus operation/input")
			}
			startEvent := starts[id]
			if startEvent == nil {
				return fmt.Errorf("async Nexus missing start")
			}
			started := startEvent.GetNexusOperationStartedEventAttributes()
			if len(started.OperationToken) > 4096 {
				return fmt.Errorf("oversized Nexus token")
			}
			raw, err := base64.RawURLEncoding.DecodeString(started.OperationToken)
			if err != nil {
				return err
			}
			var token struct {
				Type       int    `json:"t"`
				Namespace  string `json:"ns"`
				WorkflowID string `json:"wid"`
				Version    int    `json:"v"`
			}
			if json.Unmarshal(raw, &token) != nil || token.Type != 1 || token.Version != 0 || token.Namespace != namespace {
				return fmt.Errorf("invalid pinned SDK Nexus token")
			}
			handler, ok := handlers[token.WorkflowID]
			if !ok {
				return fmt.Errorf("missing linked Nexus handler")
			}
			linked := false
			for _, link := range startEvent.Links {
				if w := link.GetWorkflowEvent(); w != nil && w.Namespace == namespace && w.WorkflowId == handler.WorkflowID && w.RunId == handler.RunID {
					linked = true
				}
			}
			if !linked {
				return fmt.Errorf("Nexus start lacks exact handler history link")
			}
			hh := histories[handler.RunID]
			hs := hh.Events[0].GetWorkflowExecutionStartedEventAttributes()
			if hs.Input == nil || len(hs.Input.Payloads) != 1 {
				return fmt.Errorf("handler input missing")
			}
			hi, err := parseNexusInput(hs.Input.Payloads[0])
			if err != nil {
				return err
			}
			if nexusKind(hi) != kind || hi.HandlerWorkflowID != input.HandlerWorkflowID {
				return fmt.Errorf("handler/operation input mismatch")
			}
			last := hh.Events[len(hh.Events)-1]
			if kind == "cancel" {
				if !cancels[id] || ended.GetNexusOperationCanceledEventAttributes() == nil || last.GetWorkflowExecutionCanceledEventAttributes() == nil {
					return fmt.Errorf("Nexus cancellation was not preserved")
				}
			} else {
				completion := last.GetWorkflowExecutionCompletedEventAttributes()
				if !stringResult(ended.GetNexusOperationCompletedEventAttributes().GetResult(), "hello") || completion == nil || completion.Result == nil || len(completion.Result.Payloads) != 1 || !stringResult(completion.Result.Payloads[0], "hello") {
					return fmt.Errorf("Nexus result mismatch")
				}
			}
			if kind == "attach" {
				if token.WorkflowID != input.HandlerWorkflowID || signalTargets[token.WorkflowID] != 1 {
					return fmt.Errorf("attach signal identity mismatch")
				}
				unblocked := 0
				for _, event := range hh.Events {
					if s := event.GetWorkflowExecutionSignaledEventAttributes(); s != nil && s.SignalName == "unblock" {
						unblocked++
					}
				}
				if unblocked != 1 {
					return fmt.Errorf("attach handler did not receive exact unblock")
				}
			}
			if prior := handlerOwner[handler.WorkflowID]; prior != "" && prior != r.RunID {
				return fmt.Errorf("handler shared across different parent runs")
			}
			handlerOwner[handler.WorkflowID] = r.RunID
			kinds[kind]++
			referenced[handler.WorkflowID]++
		}
		if kinds["sync"] != 2 || kinds["echo"] != 2 || kinds["cancel"] != 2 || kinds["attach"] != 6 {
			return fmt.Errorf("wrong pinned Nexus variants per parent run")
		}
	}
	for wid, r := range handlers {
		start := histories[r.RunID].Events[0].GetWorkflowExecutionStartedEventAttributes()
		if start.Input == nil || len(start.Input.Payloads) != 1 {
			return fmt.Errorf("handler input")
		}
		input, err := parseNexusInput(start.Input.Payloads[0])
		if err != nil {
			return err
		}
		want := 1
		if nexusKind(input) == "attach" {
			want = 3
		}
		if referenced[wid] != want {
			return fmt.Errorf("unreferenced or incorrectly shared handler")
		}
	}
	return nil
}

func inspectMixed(h *historypb.History, status enums.WorkflowExecutionStatus) (map[string]int, string, error) {
	if status != enums.WORKFLOW_EXECUTION_STATUS_CANCELED {
		return inspect(h, status)
	}
	if h == nil || len(h.Events) < 2 || h.Events[0].GetEventType() != enums.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED || h.Events[0].GetWorkflowExecutionStartedEventAttributes().GetWorkflowType().GetName() != "NexusHandlerWorkflow" || h.Events[len(h.Events)-1].GetWorkflowExecutionCanceledEventAttributes() == nil {
		return nil, "", fmt.Errorf("unexpected canceled workflow")
	}
	counts := map[string]int{}
	for i, e := range h.Events {
		if e == nil || e.EventId != int64(i+1) {
			return nil, "", fmt.Errorf("history event gap or duplicate")
		}
		counts[e.EventType.String()]++
	}
	return counts, "", nil
}
