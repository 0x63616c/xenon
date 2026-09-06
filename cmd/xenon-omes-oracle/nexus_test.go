package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func nexusFixture(t *testing.T) ([]runAudit, map[string]*historypb.History) {
	t.Helper()
	runs, hs := mixedFixture(t)
	add := func(h *historypb.History, kind string, attrs map[string]any) int64 {
		id := int64(len(h.Events) + 1)
		raw, err := json.Marshal(map[string]any{"eventId": id, kind + "EventAttributes": attrs})
		if err != nil {
			t.Fatal(err)
		}
		e := &historypb.HistoryEvent{}
		if err = protojson.Unmarshal(raw, e); err != nil {
			t.Fatal(err)
		}
		h.Events = append(h.Events, e)
		return id
	}
	payload := func(encoding string, value any) map[string]any {
		raw, _ := json.Marshal(value)
		return map[string]any{"metadata": map[string]string{"encoding": base64.StdEncoding.EncodeToString([]byte(encoding))}, "data": base64.StdEncoding.EncodeToString(raw)}
	}
	for _, r := range append([]runAudit(nil), runs...) {
		if len(r.WorkflowID) > 0 && hs[r.RunID].Events[0].GetWorkflowExecutionStartedEventAttributes().ParentWorkflowExecution != nil {
			continue
		}
		h := hs[r.RunID]
		terminal := h.Events[len(h.Events)-1]
		h.Events = h.Events[:len(h.Events)-1]
		for iter := 0; iter < 2; iter++ {
			for _, kind := range []string{"sync", "echo", "cancel", "attach"} {
				input := map[string]any{"input": "hello"}
				operation := "echo-async"
				count := 1
				wid := fmt.Sprintf("%s-%s-%d", kind, r.RunID, iter)
				rid := "run-" + wid
				if kind == "sync" {
					operation = "echo-sync"
				}
				if kind == "cancel" {
					input = map[string]any{"beforeActions": []any{map[string]any{"actions": []any{map[string]any{"awaitWorkflowState": map[string]any{"key": "never", "value": "resolves"}}}}}}
				}
				if kind == "attach" {
					wid = "nexus-attach-handler-" + wid
					input["handlerWorkflowIdConflictPolicy"] = "WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING"
					input["handlerWorkflowId"] = wid
					input["waitForSignal"] = true
					count = 3
					add(h, "signalExternalWorkflowExecutionInitiated", map[string]any{"signalName": "unblock", "workflowExecution": map[string]any{"workflowId": wid}})
				}
				if kind != "sync" {
					hh := &historypb.History{}
					hs[rid] = hh
					add(hh, "workflowExecutionStarted", map[string]any{"workflowType": map[string]any{"name": "NexusHandlerWorkflow"}, "input": map[string]any{"payloads": []any{payload("json/protobuf", input)}}})
					if kind == "attach" {
						add(hh, "workflowExecutionSignaled", map[string]any{"signalName": "unblock"})
					}
					if kind == "cancel" {
						add(hh, "workflowExecutionCanceled", map[string]any{})
					} else {
						add(hh, "workflowExecutionCompleted", map[string]any{"result": map[string]any{"payloads": []any{payload("json/plain", "hello")}}})
					}
					runs = append(runs, runAudit{WorkflowID: wid, RunID: rid})
				}
				for copy := 0; copy < count; copy++ {
					id := add(h, "nexusOperationScheduled", map[string]any{"service": "kitchen-sink", "endpoint": "test-nexus-endpoint-xenon-full-mixed", "operation": operation, "input": payload("json/protobuf", input)})
					if kind != "sync" {
						token, _ := json.Marshal(map[string]any{"t": 1, "ns": "test", "wid": wid})
						startID := add(h, "nexusOperationStarted", map[string]any{"scheduledEventId": id, "operationToken": base64.RawURLEncoding.EncodeToString(token)})
						h.Events[startID-1].Links = []*commonpb.Link{{Variant: &commonpb.Link_WorkflowEvent_{WorkflowEvent: &commonpb.Link_WorkflowEvent{Namespace: "test", WorkflowId: wid, RunId: rid}}}}
					}
					if kind == "cancel" {
						add(h, "nexusOperationCancelRequested", map[string]any{"scheduledEventId": id})
						add(h, "nexusOperationCanceled", map[string]any{"scheduledEventId": id})
					} else {
						add(h, "nexusOperationCompleted", map[string]any{"scheduledEventId": id, "result": payload("json/plain", "hello")})
					}
				}
			}
		}
		terminal.EventId = int64(len(h.Events) + 1)
		h.Events = append(h.Events, terminal)
	}
	return runs, hs
}
func TestMixedNexus(t *testing.T) {
	for _, control := range []string{"valid", "unknown_handler", "wrong_link", "wrong_result", "missing_cancel", "wrong_attach", "missing_operation"} {
		t.Run(control, func(t *testing.T) {
			runs, hs := nexusFixture(t)
			h := hs["root-0-0"]
			switch control {
			case "unknown_handler":
				runs = append(runs, runAudit{WorkflowID: "unexpected", RunID: "unexpected"})
				hs["unexpected"] = hs["run-echo-root-0-0-0"]
			case "wrong_link":
				for _, e := range h.Events {
					if e.GetNexusOperationStartedEventAttributes() != nil {
						e.Links = nil
						break
					}
				}
			case "wrong_result":
				for _, e := range h.Events {
					if a := e.GetNexusOperationCompletedEventAttributes(); a != nil {
						a.Result.Data = []byte(`"wrong"`)
						break
					}
				}
			case "missing_cancel":
				for _, e := range h.Events {
					if e.GetNexusOperationCancelRequestedEventAttributes() != nil {
						e.Attributes = nil
						break
					}
				}
			case "wrong_attach":
				hs["run-attach-root-0-0-0"].Events[1].GetWorkflowExecutionSignaledEventAttributes().SignalName = "wrong"
			case "missing_operation":
				for _, e := range h.Events {
					if e.GetNexusOperationCompletedEventAttributes() != nil {
						e.Attributes = nil
						break
					}
				}
			}
			err := mixedAllSemantics(runs, hs, "test")
			if (err == nil) != (control == "valid") {
				t.Fatalf("%s: %v", control, err)
			}
		})
	}
}

func TestMixedNexusCanceled(t *testing.T) {
	_, hs := nexusFixture(t)
	h := hs["run-cancel-root-0-0-0"]
	h.Events[0].EventType = enums.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
	h.Events[1].EventType = enums.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED
	if _, _, err := inspectMixed(h, enums.WORKFLOW_EXECUTION_STATUS_CANCELED); err != nil {
		t.Fatal(err)
	}
	h.Events[0].GetWorkflowExecutionStartedEventAttributes().WorkflowType.Name = "kitchenSink"
	if _, _, err := inspectMixed(h, enums.WORKFLOW_EXECUTION_STATUS_CANCELED); err == nil {
		t.Fatal("accepted canceled parent")
	}
}
