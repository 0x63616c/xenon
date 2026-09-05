package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"testing"
)

func mixedFixture(t *testing.T) ([]runAudit, map[string]*historypb.History) {
	t.Helper()
	var runs []runAudit
	histories := map[string]*historypb.History{}
	add := func(h *historypb.History, kind string, attrs map[string]any) int64 {
		id := int64(len(h.Events) + 1)
		name := kind + "EventAttributes"
		raw, err := json.Marshal(map[string]any{"eventId": id, name: attrs})
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
	result := map[string]any{"result": map[string]any{"payloads": []any{map[string]any{"metadata": map[string]string{"encoding": base64.StdEncoding.EncodeToString([]byte("_passthrough"))}}}}}
	for root := 0; root < 40; root++ {
		wid := fmt.Sprintf("w-xenon-full-mixed-0123456789abcdef-%d", root)
		for chunk := 0; chunk < 2; chunk++ {
			rid := fmt.Sprintf("root-%d-%d", root, chunk)
			h := &historypb.History{}
			histories[rid] = h
			r := runAudit{WorkflowID: wid, RunID: rid}
			if chunk == 0 {
				r.NextRun = fmt.Sprintf("root-%d-1", root)
			} else {
				r.PreviousRun = fmt.Sprintf("root-%d-0", root)
			}
			add(h, "workflowExecutionStarted", map[string]any{"continuedExecutionRunId": r.PreviousRun})
			for iter := 0; iter < 2; iter++ {
				childID := fmt.Sprintf("%s/child-%d", wid, chunk*2+iter+1)
				childRun := fmt.Sprintf("child-%d-%d-%d", root, chunk, iter)
				init := add(h, "startChildWorkflowExecutionInitiated", map[string]any{"workflowId": childID})
				execution := map[string]any{"workflowId": childID, "runId": childRun}
				add(h, "childWorkflowExecutionStarted", map[string]any{"initiatedEventId": init, "workflowExecution": execution})
				add(h, "childWorkflowExecutionCompleted", map[string]any{"initiatedEventId": init, "workflowExecution": execution})
				ch := &historypb.History{}
				histories[childRun] = ch
				add(ch, "workflowExecutionStarted", map[string]any{"parentWorkflowExecution": map[string]any{"workflowId": wid, "runId": rid}})
				for i := 0; i < 3; i++ {
					id := add(ch, "activityTaskScheduled", map[string]any{"activityType": map[string]any{"name": "payload"}})
					add(ch, "activityTaskCompleted", map[string]any{"scheduledEventId": id})
				}
				add(ch, "workflowExecutionCompleted", result)
				runs = append(runs, runAudit{WorkflowID: childID, RunID: childRun})
				add(h, "workflowExecutionSignaled", map[string]any{"signalName": "do_actions_signal"})
				for i := 0; i < 2; i++ {
					add(h, "timerFired", map[string]any{})
				}
				for _, name := range []string{"retryable_error", "timeout", "heartbeat"} {
					id := add(h, "activityTaskScheduled", map[string]any{"activityType": map[string]any{"name": name}})
					add(h, "activityTaskStarted", map[string]any{"scheduledEventId": id, "attempt": 2})
					add(h, "activityTaskCompleted", map[string]any{"scheduledEventId": id})
				}
				for _, action := range []string{`{"timer":{"duration":"0.1s"}}`, `{"execActivity":{"payload":{"bytesToReturn":256},"isRemote":{}}}`, `{"execActivity":{"payload":{"bytesToReturn":256},"isLocal":{}}}`} {
					data := []byte(`{"doActions":{"actions":[` + action + `]}}`)
					id := add(h, "workflowExecutionUpdateAccepted", map[string]any{"acceptedRequest": map[string]any{"input": map[string]any{"name": "do_actions_update", "args": map[string]any{"payloads": []any{map[string]any{"metadata": map[string]string{"encoding": base64.StdEncoding.EncodeToString([]byte("json/protobuf"))}, "data": base64.StdEncoding.EncodeToString(data)}}}}}})
					add(h, "workflowExecutionUpdateCompleted", map[string]any{"acceptedEventId": id, "outcome": map[string]any{"success": map[string]any{}}})
				}
			}
			if chunk == 0 {
				add(h, "workflowExecutionContinuedAsNew", map[string]any{"newExecutionRunId": r.NextRun})
			} else {
				add(h, "workflowExecutionCompleted", result)
			}
			runs = append(runs, r)
		}
	}
	return runs, histories
}
func TestMixedSemantics(t *testing.T) {
	for _, control := range []string{"valid", "missing-child", "wrong-parent", "missing-update", "missing-retry", "wrong-result", "missing-signal", "missing-can"} {
		t.Run(control, func(t *testing.T) {
			runs, hs := mixedFixture(t)
			h := hs["root-0-0"]
			switch control {
			case "missing-child":
				delete(hs, "child-0-0-0")
			case "wrong-parent":
				hs["child-0-0-0"].Events[0].GetWorkflowExecutionStartedEventAttributes().ParentWorkflowExecution.RunId = "wrong"
			case "missing-update":
				for _, e := range h.Events {
					if e.GetWorkflowExecutionUpdateCompletedEventAttributes() != nil {
						e.Attributes = nil
						break
					}
				}
			case "missing-retry":
				for _, e := range h.Events {
					if a := e.GetActivityTaskStartedEventAttributes(); a != nil {
						a.Attempt = 1
						break
					}
				}
			case "wrong-result":
				hs["child-0-0-0"].Events[7].GetWorkflowExecutionCompletedEventAttributes().Result.Payloads[0].Data = []byte("wrong")
			case "missing-signal":
				for _, e := range h.Events {
					if e.GetWorkflowExecutionSignaledEventAttributes() != nil {
						e.Attributes = nil
					}
				}
			case "missing-can":
				for i := range runs {
					if runs[i].RunID == "root-0-0" {
						runs[i].NextRun = ""
					}
				}
			}
			err := mixedSemantics(runs, hs)
			if (err == nil) != (control == "valid") {
				t.Fatalf("control %s: %v", control, err)
			}
		})
	}
}
