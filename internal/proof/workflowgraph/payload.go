package workflowgraph

import (
	"encoding/json"
	"reflect"

	common "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// Omes uses _passthrough to wrap Payload arguments/results, while its initial
// WorkflowInput is encoded by the SDK's json/protobuf converter. Compare inner
// payload semantics, never protobuf map wire order or JSON whitespace.
func unwrap(p *common.Payload) *common.Payload {
	if p == nil || len(p.Metadata) != 1 || string(p.Metadata["encoding"]) != "_passthrough" || len(p.ProtoReflect().GetUnknown()) != 0 {
		return nil
	}
	inner := new(common.Payload)
	if proto.Unmarshal(p.Data, inner) != nil {
		return nil
	}
	return inner
}
func payloadValue(raw []byte) any {
	p, e := payload(raw)
	if e != nil {
		return nil
	}
	b, e := protojson.Marshal(p)
	if e != nil {
		return nil
	}
	var v any
	if json.Unmarshal(b, &v) != nil {
		return nil
	}
	return v
}

// workflowValue renders only the already-validated narrow grammar to protobuf
// JSON's documented field names. It is not a second execution engine.
func workflowValue(raw []byte) map[string]any {
	f, _ := fields(raw, 1)
	out := map[string]any{}
	var sets []any
	var set func([]byte) map[string]any
	set = func(raw []byte) map[string]any {
		f, _ := fields(raw, 1)
		out := map[string]any{}
		var actions []any
		for _, raw := range f[1] {
			a, _ := fields(raw, 3, 11, 13, 14)
			v := map[string]any{}
			for kind, values := range a {
				switch kind {
				case 3:
					f, _ := fields(values[0], 3, 6)
					v["execChildWorkflow"] = map[string]any{"workflowId": string(f[3][0]), "input": []any{payloadValue(f[6][0])}}
				case 11:
					f, _ := fields(values[0], 1)
					v["returnResult"] = map[string]any{"returnThis": payloadValue(f[1][0])}
				case 13:
					f, _ := fields(values[0], 3)
					v["continueAsNew"] = map[string]any{"arguments": []any{payloadValue(f[3][0])}}
				case 14:
					v["nestedActionSet"] = set(values[0])
				}
			}
			actions = append(actions, v)
		}
		if len(actions) > 0 {
			out["actions"] = actions
		}
		return out
	}
	for _, raw := range f[1] {
		sets = append(sets, set(raw))
	}
	if len(sets) > 0 {
		out["initialActions"] = sets
	}
	return out
}
func matchesInput(actual, want *common.Payload, initialRoot bool) bool {
	if !initialRoot {
		return equalWorkflowInput(unwrap(actual), want)
	}
	if actual == nil || len(actual.Metadata) != 2 || string(actual.Metadata["encoding"]) != "json/protobuf" || string(actual.Metadata["messageType"]) != "temporal.omes.kitchen_sink.WorkflowInput" || len(actual.ProtoReflect().GetUnknown()) != 0 {
		return false
	}
	var value any
	if json.Unmarshal(actual.Data, &value) != nil {
		return false
	}
	return reflect.DeepEqual(value, workflowValue(want.Data))
}

func equalWorkflowInput(actual, want *common.Payload) bool {
	valid := func(p *common.Payload) bool {
		if p == nil || len(p.Metadata) != 2 || string(p.Metadata["encoding"]) != "binary/protobuf" || string(p.Metadata["messageType"]) != "temporal.omes.kitchen_sink.WorkflowInput" || len(p.ProtoReflect().GetUnknown()) != 0 {
			return false
		}
		raw := protowire.AppendTag(nil, 1, protowire.BytesType)
		raw = protowire.AppendBytes(raw, p.Data)
		_, err := Derive(raw)
		return err == nil
	}
	return valid(actual) && valid(want) && reflect.DeepEqual(workflowValue(actual.Data), workflowValue(want.Data))
}
