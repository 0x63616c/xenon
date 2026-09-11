// Package workflowgraph derives a deliberately narrow Omes execution graph from
// saved TestInput bytes, independently of worker decisions and observed history.
package workflowgraph

import (
	"errors"
	"fmt"

	common "go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const Contract = "omes-serial-intent-v1"

// Node identities are logical: Temporal assigns run IDs, and Omes assigns the
// root workflow ID. Children have exact input-declared IDs; continuation edges
// retain the preceding workflow ID. Runtime IDs are bound only during checking.
type Node struct {
	WorkflowID       string
	Parent, Previous int
	Input            *common.Payload
	Result           *common.Payload
	Children         []int
	Next             int
}
type Graph struct{ Nodes []Node }

// fields decodes only length-delimited fields from the pinned protobuf grammar.
// Unknown fields, duplicate singular fields and alternate wire forms fail closed.
func fields(raw []byte, allowed ...protowire.Number) (map[protowire.Number][][]byte, error) {
	out := map[protowire.Number][][]byte{}
	for len(raw) > 0 {
		n, t, k := protowire.ConsumeTag(raw)
		if k < 0 || t != protowire.BytesType {
			return nil, errors.New("unsupported intent field/wire type")
		}
		raw = raw[k:]
		ok := false
		for _, a := range allowed {
			if n == a {
				ok = true
			}
		}
		if !ok {
			return nil, fmt.Errorf("unsupported intent field %d", n)
		}
		b, k := protowire.ConsumeBytes(raw)
		if k < 0 {
			return nil, errors.New("invalid intent protobuf")
		}
		raw = raw[k:]
		out[n] = append(out[n], b)
	}
	return out, nil
}
func one(f map[protowire.Number][][]byte, n protowire.Number) ([]byte, error) {
	if len(f[n]) != 1 {
		return nil, fmt.Errorf("intent requires exactly one field %d", n)
	}
	return f[n][0], nil
}
func payload(raw []byte) (*common.Payload, error) {
	p := new(common.Payload)
	if e := proto.Unmarshal(raw, p); e != nil {
		return nil, e
	}
	if len(p.ProtoReflect().GetUnknown()) != 0 {
		return nil, errors.New("unsupported payload fields")
	}
	return p, nil
}
func workflowPayload(raw []byte) (*common.Payload, error) {
	p, e := payload(raw)
	if e != nil {
		return nil, e
	}
	if len(p.Metadata) != 2 || string(p.Metadata["encoding"]) != "binary/protobuf" || string(p.Metadata["messageType"]) != "temporal.omes.kitchen_sink.WorkflowInput" {
		return nil, errors.New("intent requires WorkflowInput protobuf payload")
	}
	return p, nil
}

// Derive accepts serial initial action sets with awaited fixed-ID children,
// nested serial sets, explicit non-nil return payloads and Continue-As-New.
// Signals, updates, concurrent sets, Nexus, implicit returns and every other
// action/option are unsupported. No network, tools or worker code are invoked.
func Derive(input []byte) (*Graph, error) {
	if len(input) == 0 || len(input) > 1<<20 {
		return nil, errors.New("intent input bound")
	}
	f, e := fields(input, 1)
	if e != nil {
		return nil, e
	}
	wf, e := one(f, 1)
	if e != nil {
		return nil, e
	}
	g := &Graph{}
	ids := map[string]bool{}
	actions := 0
	var visit func([]byte, string, int, int, int) (int, error)
	visit = func(raw []byte, id string, parent, previous, depth int) (int, error) {
		if depth > 32 || len(g.Nodes) >= 128 {
			return 0, errors.New("intent graph bound")
		}
		f, e := fields(raw, 1)
		if e != nil {
			return 0, e
		}
		index := len(g.Nodes)
		g.Nodes = append(g.Nodes, Node{WorkflowID: id, Parent: parent, Previous: previous, Next: -1, Input: &common.Payload{Metadata: map[string][]byte{"encoding": []byte("binary/protobuf"), "messageType": []byte("temporal.omes.kitchen_sink.WorkflowInput")}, Data: raw}})
		terminal := false
		var set func([]byte, int) error
		set = func(raw []byte, d int) error {
			if d > 32 {
				return errors.New("intent action depth bound")
			}
			f, e := fields(raw, 1)
			if e != nil {
				return e
			}
			for _, a := range f[1] {
				if terminal {
					return errors.New("unsupported action after terminal intent")
				}
				actions++
				if actions > 2048 {
					return errors.New("intent action bound")
				}
				f, e := fields(a, 3, 11, 13, 14)
				if e != nil {
					return e
				}
				if len(f) != 1 {
					return errors.New("intent action requires one variant")
				}
				var kind protowire.Number
				for n := range f {
					kind = n
				}
				body, e := one(f, kind)
				if e != nil {
					return e
				}
				switch kind {
				case 14:
					if e = set(body, d+1); e != nil {
						return e
					}
				case 11:
					f, e = fields(body, 1)
					if e != nil {
						return e
					}
					b, e := one(f, 1)
					if e != nil {
						return e
					}
					p, e := payload(b)
					if e != nil {
						return e
					}
					g.Nodes[index].Result = p
					terminal = true
				case 13:
					f, e = fields(body, 3)
					if e != nil {
						return e
					}
					b, e := one(f, 3)
					if e != nil {
						return e
					}
					p, e := workflowPayload(b)
					if e != nil {
						return e
					}
					next, e := visit(p.Data, id, parent, index, depth+1)
					if e != nil {
						return e
					}
					g.Nodes[index].Next = next
					terminal = true
				case 3:
					f, e = fields(body, 3, 6)
					if e != nil {
						return e
					}
					b, e := one(f, 3)
					if e != nil {
						return e
					}
					childID := string(b)
					if childID == "" || len(childID) > 255 || ids[childID] {
						return errors.New("intent child ID empty, repeated or too long")
					}
					ids[childID] = true
					b, e = one(f, 6)
					if e != nil {
						return e
					}
					p, e := workflowPayload(b)
					if e != nil {
						return e
					}
					child, e := visit(p.Data, childID, index, -1, depth+1)
					if e != nil {
						return e
					}
					g.Nodes[index].Children = append(g.Nodes[index].Children, child)
				}
			}
			return nil
		}
		for _, a := range f[1] {
			if e = set(a, depth); e != nil {
				return 0, e
			}
		}
		if !terminal {
			return 0, errors.New("intent requires explicit terminal result or continuation")
		}
		return index, nil
	}
	_, e = visit(wf, "", -1, -1, 0)
	if e != nil {
		return nil, e
	}
	return g, nil
}
