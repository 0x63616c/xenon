package workflowgraph

import (
	"encoding/json"
	"testing"

	common "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	history "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/encoding/protowire"
)

func payloadWithMetadataOrder(p *common.Payload, keys ...string) []byte {
	var out []byte
	for _, key := range keys {
		entry := protowire.AppendTag(nil, 1, protowire.BytesType)
		entry = protowire.AppendString(entry, key)
		entry = protowire.AppendTag(entry, 2, protowire.BytesType)
		entry = protowire.AppendBytes(entry, p.Metadata[key])
		out = protowire.AppendTag(out, 1, protowire.BytesType)
		out = protowire.AppendBytes(out, entry)
	}
	out = protowire.AppendTag(out, 2, protowire.BytesType)
	return protowire.AppendBytes(out, p.Data)
}

func childWithPayload(id string, payload []byte) []byte {
	return wire(3, append(wire(3, []byte(id)), wire(6, payload)...))
}

func childInitiated(id string, input []byte) *history.HistoryEvent {
	return &history.HistoryEvent{EventType: enums.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED, Attributes: &history.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{StartChildWorkflowExecutionInitiatedEventAttributes: &history.StartChildWorkflowExecutionInitiatedEventAttributes{WorkflowId: id, Input: &common.Payloads{Payloads: []*common.Payload{wrapped(argument(input))}}}}}
}
func childStarted(id, run string, initiated int64) *history.HistoryEvent {
	return &history.HistoryEvent{EventType: enums.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED, Attributes: &history.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: &history.ChildWorkflowExecutionStartedEventAttributes{WorkflowExecution: &common.WorkflowExecution{WorkflowId: id, RunId: run}, InitiatedEventId: initiated}}}
}
func childCompleted(id, run, value string, initiated, started int64) *history.HistoryEvent {
	return &history.HistoryEvent{EventType: enums.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED, Attributes: &history.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{ChildWorkflowExecutionCompletedEventAttributes: &history.ChildWorkflowExecutionCompletedEventAttributes{WorkflowExecution: &common.WorkflowExecution{WorkflowId: id, RunId: run}, InitiatedEventId: initiated, StartedEventId: started, Result: &common.Payloads{Payloads: []*common.Payload{wrapped(result(value))}}}}}
}

func serialChildrenFixture() ([]byte, []Execution) {
	first, second := workflow(ret("first")), workflow(ret("second"))
	root := workflow(child("wf_first", first), child("wf_second", second), ret("root"))
	// This independent expected observation encodes the input's two named actions,
	// without invoking Derive or using its returned graph to construct histories.
	childAction := func(id string, input []byte) any {
		return map[string]any{"execChildWorkflow": map[string]any{"workflowId": id, "input": []any{map[string]any{"metadata": map[string][]byte{"encoding": []byte("binary/protobuf"), "messageType": []byte("temporal.omes.kitchen_sink.WorkflowInput")}, "data": input}}}}
	}
	jsonInput, e := json.Marshal(map[string]any{"initialActions": []any{map[string]any{"actions": []any{childAction("wf_first", first), childAction("wf_second", second), map[string]any{"returnResult": map[string]any{"returnThis": map[string]any{"metadata": map[string][]byte{"encoding": []byte("json/plain")}, "data": []byte(`"root"`)}}}}}}})
	if e != nil {
		panic(e)
	}
	rootStart := start(root, "", "", "")
	rootStart.GetWorkflowExecutionStartedEventAttributes().Input.Payloads[0] = &common.Payload{Metadata: map[string][]byte{"encoding": []byte("json/protobuf"), "messageType": []byte("temporal.omes.kitchen_sink.WorkflowInput")}, Data: jsonInput}
	return wire(1, root), []Execution{
		{"w-case-opaque-1", "root", h(rootStart, childInitiated("wf_first", first), childStarted("wf_first", "first", 2), childCompleted("wf_first", "first", "first", 2, 3), childInitiated("wf_second", second), childStarted("wf_second", "second", 5), childCompleted("wf_second", "second", "second", 5, 6), end("root"))},
		{"wf_first", "first", h(start(first, "w-case-opaque-1", "root", ""), end("first"))},
		{"wf_second", "second", h(start(second, "w-case-opaque-1", "root", ""), end("second"))},
	}
}
func TestAwaitedSerialChildrenCompletionAndOrder(t *testing.T) {
	raw, runs := serialChildrenFixture()
	g, e := Derive(raw)
	if e != nil {
		t.Fatal(e)
	}
	if e = Check(g, "w-case-", runs); e != nil {
		t.Fatal(e)
	}
	controls := []struct {
		name, want string
		mutate     func([]Execution)
	}{
		{"skipped first await", "child_completion", func(r []Execution) { r[0].History = h(append(r[0].History.Events[:3], r[0].History.Events[4:]...)...) }},
		{"second issued before first completed", "serial_child_order", func(r []Execution) {
			events := r[0].History.Events
			events[3], events[4] = events[4], events[3]
			r[0].History = h(events...)
			events[5].GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId = 4
			events[6].GetChildWorkflowExecutionCompletedEventAttributes().InitiatedEventId = 4
		}},
		{"reversed fixed child order", "serial_child_order", func(r []Execution) {
			v := r[0].History.Events
			r[0].History = h(v[0], v[4], v[5], v[6], v[1], v[2], v[3], v[7])
			v = r[0].History.Events
			v[2].GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId = 2
			v[3].GetChildWorkflowExecutionCompletedEventAttributes().InitiatedEventId = 2
			v[3].GetChildWorkflowExecutionCompletedEventAttributes().StartedEventId = 3
			v[5].GetChildWorkflowExecutionStartedEventAttributes().InitiatedEventId = 5
			v[6].GetChildWorkflowExecutionCompletedEventAttributes().InitiatedEventId = 5
			v[6].GetChildWorkflowExecutionCompletedEventAttributes().StartedEventId = 6
		}},
		{"wrong completed run", "child_completion_link", func(r []Execution) {
			r[0].History.Events[3].GetChildWorkflowExecutionCompletedEventAttributes().WorkflowExecution.RunId = "wrong"
		}},
		{"wrong completion start link", "child_completion_link", func(r []Execution) {
			r[0].History.Events[3].GetChildWorkflowExecutionCompletedEventAttributes().StartedEventId = 2
		}},
		{"wrong parent copy of result", "child_completion_result", func(r []Execution) {
			r[0].History.Events[3].GetChildWorkflowExecutionCompletedEventAttributes().Result.Payloads[0] = wrapped(result("wrong"))
		}},
		{"parent completes before child", "serial_child_order", func(r []Execution) {
			v := r[0].History.Events
			r[0].History = h(v[0], v[1], v[2], v[3], v[4], v[5], end("root"), v[6], end("root"))
		}},
	}
	for _, c := range controls {
		t.Run(c.name, func(t *testing.T) {
			_, r := serialChildrenFixture()
			c.mutate(r)
			e := Check(g, "w-case-", r)
			if e == nil || e.Error() != "expected_graph/"+c.want {
				t.Fatalf("want %s got %v", c.want, e)
			}
		})
	}
}
func TestContinuedChildRequiresFinalParentCompletion(t *testing.T) {
	raw, runs := fixture()
	g, e := Derive(raw)
	if e != nil {
		t.Fatal(e)
	}
	runs[0].History.Events[3].GetChildWorkflowExecutionCompletedEventAttributes().WorkflowExecution.RunId = "run_child1"
	if e = Check(g, "w-case-", runs); e == nil || e.Error() != "expected_graph/child_completion_link" {
		t.Fatal(e)
	}
	_, runs = fixture()
	runs[0].History = h(append(runs[0].History.Events[:3], runs[0].History.Events[4:]...)...)
	if e = Check(g, "w-case-", runs); e == nil || e.Error() != "expected_graph/child_completion" {
		t.Fatal(e)
	}
}

func TestWorkflowInputComparisonIgnoresProtobufMapWireOrder(t *testing.T) {
	leaf := workflow(ret("leaf"))
	argument := argument(leaf)
	canonical := workflow(child("wf_leaf", leaf), ret("parent"))
	reordered := workflow(childWithPayload("wf_leaf", payloadWithMetadataOrder(argument, "messageType", "encoding")), ret("parent"))
	if string(canonical) == string(reordered) {
		t.Fatal("fixture did not change protobuf wire order")
	}
	actual := wrapped(&common.Payload{Metadata: argument.Metadata, Data: reordered})
	want := &common.Payload{Metadata: argument.Metadata, Data: canonical}
	if !matchesInput(actual, want, false) {
		t.Fatal("semantic WorkflowInput comparison depended on protobuf map wire order")
	}
	changed := workflow(child("wf_other", leaf), ret("parent"))
	if matchesInput(wrapped(&common.Payload{Metadata: argument.Metadata, Data: changed}), want, false) {
		t.Fatal("semantic WorkflowInput comparison accepted changed child identity")
	}
}
