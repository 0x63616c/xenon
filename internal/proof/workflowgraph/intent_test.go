package workflowgraph

import (
	"strings"
	"testing"

	"encoding/json"
	common "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	history "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"os"
)

func wire(n protowire.Number, b []byte) []byte {
	out := protowire.AppendTag(nil, n, protowire.BytesType)
	return protowire.AppendBytes(out, b)
}
func marsh(m proto.Message) []byte {
	b, e := proto.Marshal(m)
	if e != nil {
		panic(e)
	}
	return b
}
func result(s string) *common.Payload {
	return &common.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(`"` + s + `"`)}
}
func argument(b []byte) *common.Payload {
	return &common.Payload{Metadata: map[string][]byte{"encoding": []byte("binary/protobuf"), "messageType": []byte("temporal.omes.kitchen_sink.WorkflowInput")}, Data: b}
}
func ret(s string) []byte { return wire(11, wire(1, marsh(result(s)))) }
func workflow(actions ...[]byte) []byte {
	var set []byte
	for _, a := range actions {
		set = append(set, wire(1, a)...)
	}
	return wire(1, set)
}
func can(b []byte) []byte { return wire(13, wire(3, marsh(argument(b)))) }
func child(id string, b []byte) []byte {
	return wire(3, append(wire(3, []byte(id)), wire(6, marsh(argument(b)))...))
}
func wrapped(p *common.Payload) *common.Payload {
	return &common.Payload{Metadata: map[string][]byte{"encoding": []byte("_passthrough")}, Data: marsh(p)}
}
func start(input []byte, parentID, parentRun, previous string) *history.HistoryEvent {
	a := &history.WorkflowExecutionStartedEventAttributes{Input: &common.Payloads{Payloads: []*common.Payload{wrapped(argument(input))}}, ContinuedExecutionRunId: previous}
	if parentRun != "" {
		a.ParentWorkflowExecution = &common.WorkflowExecution{WorkflowId: parentID, RunId: parentRun}
	}
	return &history.HistoryEvent{EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, Attributes: &history.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: a}}
}
func end(s string) *history.HistoryEvent {
	return &history.HistoryEvent{EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, Attributes: &history.HistoryEvent_WorkflowExecutionCompletedEventAttributes{WorkflowExecutionCompletedEventAttributes: &history.WorkflowExecutionCompletedEventAttributes{Result: &common.Payloads{Payloads: []*common.Payload{wrapped(result(s))}}}}}
}
func continued(next string, input []byte) *history.HistoryEvent {
	return &history.HistoryEvent{EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, Attributes: &history.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{WorkflowExecutionContinuedAsNewEventAttributes: &history.WorkflowExecutionContinuedAsNewEventAttributes{NewExecutionRunId: next, Input: &common.Payloads{Payloads: []*common.Payload{wrapped(argument(input))}}}}}
}
func h(events ...*history.HistoryEvent) *history.History {
	for i, e := range events {
		e.EventId = int64(i + 1)
	}
	return &history.History{Events: events}
}
func fixture() ([]byte, []Execution) {
	// Expected observations are hand-authored, never synthesized from Derive's graph.
	childLast := workflow(ret("child-value"))
	childFirst := workflow(can(childLast))
	rootLast := workflow(ret("root-value"))
	rootFirst := workflow(child("wf_child", childFirst), can(rootLast))
	childEvent := &history.HistoryEvent{EventType: enums.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED, Attributes: &history.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: &history.ChildWorkflowExecutionStartedEventAttributes{WorkflowExecution: &common.WorkflowExecution{WorkflowId: "wf_child", RunId: "run_child1"}, InitiatedEventId: 2}}}
	rootJSON, err := json.Marshal(map[string]any{"initialActions": []any{map[string]any{"actions": []any{map[string]any{"execChildWorkflow": map[string]any{"workflowId": "wf_child", "input": []any{map[string]any{"metadata": map[string][]byte{"encoding": []byte("binary/protobuf"), "messageType": []byte("temporal.omes.kitchen_sink.WorkflowInput")}, "data": childFirst}}}}, map[string]any{"continueAsNew": map[string]any{"arguments": []any{map[string]any{"metadata": map[string][]byte{"encoding": []byte("binary/protobuf"), "messageType": []byte("temporal.omes.kitchen_sink.WorkflowInput")}, "data": rootLast}}}}}}}})
	if err != nil {
		panic(err)
	}
	rootStart := start(rootFirst, "", "", "")
	rootStart.GetWorkflowExecutionStartedEventAttributes().Input.Payloads[0] = &common.Payload{Metadata: map[string][]byte{"encoding": []byte("json/protobuf"), "messageType": []byte("temporal.omes.kitchen_sink.WorkflowInput")}, Data: rootJSON}
	return wire(1, rootFirst), []Execution{
		{"w-case-opaque-1", "run_root1", h(rootStart, childInitiated("wf_child", childFirst), childEvent, childCompleted("wf_child", "run_child2", "child-value", 2, 3), continued("run_root2", rootLast))},
		{"wf_child", "run_child1", h(start(childFirst, "w-case-opaque-1", "run_root1", ""), continued("run_child2", childLast))},
		{"wf_child", "run_child2", h(start(childLast, "w-case-opaque-1", "run_root1", "run_child1"), end("child-value"))},
		{"w-case-opaque-1", "run_root2", h(start(rootLast, "", "", "run_root1"), end("root-value"))},
	}
}
func TestSerialIntentGraphAndExactResults(t *testing.T) {
	raw, runs := fixture()
	g, e := Derive(raw)
	if e != nil {
		t.Fatal(e)
	}
	if len(g.Nodes) != 4 || len(g.Nodes[0].Children) != 1 || g.Nodes[0].Next != 3 || g.Nodes[1].Next != 2 {
		t.Fatal(g)
	}
	for range 3 {
		if e = Check(g, "w-case-", runs); e != nil {
			t.Fatal(e)
		}
	}
}
func TestExpectedGraphNegativeControls(t *testing.T) {
	controls := []struct {
		name, want string
		mutate     func([]Execution) []Execution
	}{
		{"child omitted from both inventory and parent", "execution_inventory", func(r []Execution) []Execution {
			r[0].History = h(r[0].History.Events[0], r[0].History.Events[len(r[0].History.Events)-1])
			return []Execution{r[0], r[3]}
		}},
		{"child command omitted", "child_inventory", func(r []Execution) []Execution {
			r[0].History = h(r[0].History.Events[0], r[0].History.Events[len(r[0].History.Events)-1])
			return r
		}},
		{"child result corrupted", "corrupt_result", func(r []Execution) []Execution { r[2].History = h(r[2].History.Events[0], end("wrong")); return r }},
		{"root result corrupted", "corrupt_result", func(r []Execution) []Execution { r[3].History = h(r[3].History.Events[0], end("wrong")); return r }},
		{"continuation missing", "omitted_continuation", func(r []Execution) []Execution {
			r[0].History = h(append(r[0].History.Events[:4], end("root-value"))...)
			return r
		}},
		{"history event omitted", "history_shape", func(r []Execution) []Execution {
			r[0].History.Events = append(r[0].History.Events[:1], r[0].History.Events[2])
			return r
		}},
		{"wrong parent", "parent_identity", func(r []Execution) []Execution {
			r[1].History.Events[0].GetWorkflowExecutionStartedEventAttributes().ParentWorkflowExecution.RunId = "wrong"
			return r
		}},
		{"wrong continuation input", "continuation_input", func(r []Execution) []Execution {
			r[0].History.Events[4].GetWorkflowExecutionContinuedAsNewEventAttributes().Input.Payloads[0].Data = []byte("wrong")
			return r
		}},
	}
	for _, c := range controls {
		t.Run(c.name, func(t *testing.T) {
			raw, runs := fixture()
			g, e := Derive(raw)
			if e != nil {
				t.Fatal(e)
			}
			e = Check(g, "w-case-", c.mutate(runs))
			if e == nil || e.Error() != "expected_graph/"+c.want {
				t.Fatalf("want %s got %v", c.want, e)
			}
		})
	}
}
func TestUnsupportedIntentFailsClosed(t *testing.T) {
	valid := wire(1, workflow(ret("ok")))
	for name, raw := range map[string][]byte{
		"client signals":   append(append([]byte{}, valid...), wire(2, nil)...),
		"Nexus":            wire(1, workflow(wire(15, nil), ret("ok"))),
		"implicit return":  wire(1, workflow()),
		"return then work": wire(1, workflow(ret("ok"), ret("bad"))),
		"reused child ID":  wire(1, workflow(child("wf_child", workflow(ret("ok"))), child("wf_child", workflow(ret("ok"))), ret("ok"))),
		"concurrent":       wire(1, wire(1, append(wire(1, ret("ok")), 0x10, 1))),
		"unknown field":    wire(1, wire(100, nil)),
		"truncated":        {0x0a, 0xff},
	} {
		t.Run(name, func(t *testing.T) {
			if _, e := Derive(raw); e == nil {
				t.Fatal("unsupported intent accepted")
			}
		})
	}
	if _, e := Derive([]byte(strings.Repeat("x", (1<<20)+1))); e == nil {
		t.Fatal("oversize accepted")
	}
}

// This saved fixture is emitted by the pinned Omes generated protobuf package
// and its actual custom converter, not by the intent parser or test wire helpers.
func TestPinnedOmesProtobufAndConverterFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/omes_serial_payloads.json")
	if err != nil {
		t.Fatal(err)
	}
	var input struct {
		Input []byte `json:"input"`
		Omes  string `json:"omes_commit"`
	}
	if err = json.Unmarshal(raw, &input); err != nil {
		t.Fatal(err)
	}
	if input.Omes != "c6978ba39aa03551ce28974117e8d7ecf983d2b3" {
		t.Fatal(input.Omes)
	}
	var saved map[string]json.RawMessage
	if err = json.Unmarshal(raw, &saved); err != nil {
		t.Fatal(err)
	}
	p := func(key string) *common.Payload {
		t.Helper()
		v := new(common.Payload)
		if err := protojson.Unmarshal(saved[key], v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	_, runs := fixture()
	for i, key := range []string{"root_start", "child_start", "child_continued", "root_continued"} {
		runs[i].History.Events[0].GetWorkflowExecutionStartedEventAttributes().Input.Payloads[0] = p(key)
	}
	runs[0].History.Events[4].GetWorkflowExecutionContinuedAsNewEventAttributes().Input.Payloads[0] = p("root_continued")
	runs[0].History.Events[1].GetStartChildWorkflowExecutionInitiatedEventAttributes().Input.Payloads[0] = p("child_start")
	runs[0].History.Events[3].GetChildWorkflowExecutionCompletedEventAttributes().Result.Payloads[0] = p("child_result")
	runs[1].History.Events[1].GetWorkflowExecutionContinuedAsNewEventAttributes().Input.Payloads[0] = p("child_continued")
	runs[2].History.Events[1].GetWorkflowExecutionCompletedEventAttributes().Result.Payloads[0] = p("child_result")
	runs[3].History.Events[1].GetWorkflowExecutionCompletedEventAttributes().Result.Payloads[0] = p("root_result")
	graph, err := Derive(input.Input)
	if err != nil {
		t.Fatal(err)
	}
	if err = Check(graph, "w-case-", runs); err != nil {
		t.Fatal(err)
	}
	// Decoder format alone cannot replace exact semantic result comparison.
	runs[3].History.Events[1].GetWorkflowExecutionCompletedEventAttributes().Result.Payloads[0] = p("child_result")
	if err = Check(graph, "w-case-", runs); err == nil || err.Error() != "expected_graph/corrupt_result" {
		t.Fatal(err)
	}
}

func TestPinnedOmesFirstIterationIsOneBased(t *testing.T) {
	raw, runs := fixture()
	graph, err := Derive(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err = Check(graph, "w-case-", runs); err != nil {
		t.Fatal(err)
	}
	runs[0].WorkflowID = "w-case-opaque-0"
	if err = Check(graph, "w-case-", runs); err == nil || err.Error() != "expected_graph/root_identity" {
		t.Fatal(err)
	}
}
