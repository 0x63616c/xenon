// Package visibility contains the shared, storage-independent visibility contract.
package visibility

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"math"
	"strconv"
	"strings"
	"time"
)

const PartitionCount = 4
const ResponseBudget = 3 * 1024 * 1024

var MinimumTime = time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC)
var MaxTime = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)

func Partition(namespace, run string) (string, error) {
	var n uuid.UUID
	var e error
	if namespace != "" {
		n, e = uuid.Parse(namespace)
	}
	if e != nil {
		return "", e
	}
	var r uuid.UUID
	if run != "" {
		r, e = uuid.Parse(run)
	}
	if e != nil {
		return "", e
	}
	h := sha256.Sum256(append(n[:], r[:]...))
	return fmt.Sprintf("vis-v1-%d", binary.BigEndian.Uint64(h[:8])%PartitionCount), nil
}
func Time(t time.Time) *wire.VisibilityTime {
	if t.IsZero() {
		t = MinimumTime
	}
	t = t.UTC().Truncate(time.Microsecond)
	return &wire.VisibilityTime{Seconds: t.Unix(), Nanos: int32(t.Nanosecond())}
}
func physicalTime(t *wire.VisibilityTime) time.Time {
	if t == nil {
		return time.Time{}
	}
	return time.Unix(t.Seconds, int64(t.Nanos)).UTC()
}
func GoTime(t *wire.VisibilityTime) time.Time {
	v := physicalTime(t)
	if v.Equal(MinimumTime) {
		return time.Time{}.UTC()
	}
	return v
}

func String(s string) *wire.QueryValue {
	return &wire.QueryValue{Scalar: &wire.QueryValue_StringValue{StringValue: s}}
}
func Int(i int64) *wire.QueryValue {
	return &wire.QueryValue{Scalar: &wire.QueryValue_IntValue{IntValue: i}}
}
func Bool(b bool) *wire.QueryValue {
	return &wire.QueryValue{Scalar: &wire.QueryValue_BoolValue{BoolValue: b}}
}
func Value(t enumspb.IndexedValueType, v any) (*wire.VisibilityAttribute, error) {
	a := &wire.VisibilityAttribute{ValueType: int32(t)}
	if v == nil {
		a.Missing = true
		return a, nil
	}
	switch t {
	case enumspb.INDEXED_VALUE_TYPE_KEYWORD, enumspb.INDEXED_VALUE_TYPE_TEXT:
		x, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("expected string, got %T", v)
		}
		a.Values = []*wire.QueryValue{String(x)}
	case enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST:
		switch x := v.(type) {
		case []string:
			for _, s := range x {
				a.Values = append(a.Values, String(s))
			}
		case []any:
			for _, s := range x {
				z, ok := s.(string)
				if !ok {
					return nil, fmt.Errorf("expected string list")
				}
				a.Values = append(a.Values, String(z))
			}
		default:
			return nil, fmt.Errorf("expected string list, got %T", v)
		}
	case enumspb.INDEXED_VALUE_TYPE_INT:
		x, ok := v.(int64)
		if !ok {
			return nil, fmt.Errorf("expected int64, got %T", v)
		}
		a.Values = []*wire.QueryValue{Int(x)}
	case enumspb.INDEXED_VALUE_TYPE_BOOL:
		x, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("expected bool, got %T", v)
		}
		a.Values = []*wire.QueryValue{Bool(x)}
	case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		x, ok := v.(float64)
		if !ok || math.IsNaN(x) || math.IsInf(x, 0) {
			return nil, fmt.Errorf("expected finite double")
		}
		a.Values = []*wire.QueryValue{{Scalar: &wire.QueryValue_DoubleValue{DoubleValue: x}}}
	case enumspb.INDEXED_VALUE_TYPE_DATETIME:
		x, ok := v.(time.Time)
		if !ok {
			return nil, fmt.Errorf("expected time.Time, got %T", v)
		}
		a.Values = []*wire.QueryValue{String(x.UTC().Truncate(time.Microsecond).Format("2006-01-02 15:04:05.999999"))}
	default:
		return nil, fmt.Errorf("unknown search attribute type %d", t)
	}
	return a, nil
}
func Scalar(a *wire.VisibilityAttribute) any {
	if a == nil || a.Missing || len(a.Values) == 0 {
		return nil
	}
	v := a.Values[0]
	switch x := v.Scalar.(type) {
	case *wire.QueryValue_StringValue:
		return x.StringValue
	case *wire.QueryValue_IntValue:
		return x.IntValue
	case *wire.QueryValue_DoubleValue:
		return x.DoubleValue
	case *wire.QueryValue_BoolValue:
		return x.BoolValue
	}
	return nil
}
func Field(d *wire.VisibilityDocument, name string) *wire.VisibilityAttribute {
	var v any
	var typ enumspb.IndexedValueType
	typ = enumspb.INDEXED_VALUE_TYPE_KEYWORD
	switch name {
	case "NamespaceId":
		v = d.NamespaceId
	case "WorkflowId":
		v = d.WorkflowId
	case "RunId":
		v = d.RunId
	case "WorkflowType":
		v = d.WorkflowType
	case "TaskQueue":
		v = d.TaskQueue
	case "ExecutionStatus":
		v = enumspb.WorkflowExecutionStatus(d.Status).String()
	case "ParentWorkflowId":
		if d.ParentWorkflowId != nil {
			v = *d.ParentWorkflowId
		}
	case "ParentRunId":
		if d.ParentRunId != nil {
			v = *d.ParentRunId
		}
	case "RootWorkflowId":
		v = d.RootWorkflowId
	case "RootRunId":
		v = d.RootRunId
	case "StartTime":
		typ = enumspb.INDEXED_VALUE_TYPE_DATETIME
		v = physicalTime(d.StartTime)
	case "ExecutionTime":
		typ = enumspb.INDEXED_VALUE_TYPE_DATETIME
		v = physicalTime(d.ExecutionTime)
	case "CloseTime":
		typ = enumspb.INDEXED_VALUE_TYPE_DATETIME
		if d.CloseTime != nil {
			v = physicalTime(d.CloseTime)
		}
	case "HistoryLength":
		typ = enumspb.INDEXED_VALUE_TYPE_INT
		if d.CloseTime != nil {
			v = d.HistoryLength
		}
	case "HistorySizeBytes":
		typ = enumspb.INDEXED_VALUE_TYPE_INT
		if d.CloseTime != nil {
			v = d.HistorySizeBytes
		}
	case "ExecutionDuration":
		typ = enumspb.INDEXED_VALUE_TYPE_INT
		if d.CloseTime != nil {
			v = d.ExecutionDuration
		}
	case "StateTransitionCount":
		typ = enumspb.INDEXED_VALUE_TYPE_INT
		if d.CloseTime != nil {
			v = d.StateTransitionCount
		}
	default:
		return d.Attributes[name]
	}
	a, _ := Value(typ, v)
	return a
}

// SortKey orders open/close descending, start descending, then UUID ascending.
func SortKey(d *wire.VisibilityDocument) string {
	close := MaxTime
	if d.CloseTime != nil {
		close = physicalTime(d.CloseTime)
	}
	stamp := func(t time.Time) string {
		return fmt.Sprintf("%016x%08x", ^(uint64(t.Unix()) ^ (uint64(1) << 63)), ^uint32(t.Nanosecond()))
	}
	return stamp(close) + "/" + stamp(physicalTime(d.StartTime)) + "/" + d.RunId
}
func CanonicalUUID(s string) (string, error) {
	if s == "" {
		return "", nil
	}
	id, e := uuid.Parse(s)
	if e != nil {
		return "", e
	}
	return id.String(), nil
}
func AttributeText(a *wire.VisibilityAttribute) string {
	if a == nil || a.Missing {
		return "null"
	}
	var b strings.Builder
	for _, v := range a.Values {
		switch x := v.Scalar.(type) {
		case *wire.QueryValue_StringValue:
			b.WriteString(strconv.Quote(x.StringValue))
		case *wire.QueryValue_IntValue:
			b.WriteString(strconv.FormatInt(x.IntValue, 10))
		case *wire.QueryValue_DoubleValue:
			b.WriteString(strconv.FormatFloat(x.DoubleValue, 'g', -1, 64))
		case *wire.QueryValue_BoolValue:
			b.WriteString(strconv.FormatBool(x.BoolValue))
		}
		b.WriteByte(0)
	}
	return b.String()
}

// ValidateAttribute rejects malformed typed wire values before any native write.
func ValidateAttribute(a *wire.VisibilityAttribute) error {
	if a == nil || a.ValueType < 1 || a.ValueType > 7 {
		return fmt.Errorf("invalid attribute type")
	}
	if a.Missing {
		if len(a.Values) != 0 {
			return fmt.Errorf("missing attribute has values")
		}
		return nil
	}
	typ := enumspb.IndexedValueType(a.ValueType)
	if typ != enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST && len(a.Values) != 1 {
		return fmt.Errorf("scalar attribute arity")
	}
	for _, v := range a.Values {
		if v == nil {
			return fmt.Errorf("nil attribute value")
		}
		switch typ {
		case enumspb.INDEXED_VALUE_TYPE_KEYWORD, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, enumspb.INDEXED_VALUE_TYPE_TEXT, enumspb.INDEXED_VALUE_TYPE_DATETIME:
			x, ok := v.Scalar.(*wire.QueryValue_StringValue)
			if !ok {
				return fmt.Errorf("invalid string attribute")
			}
			if strings.IndexByte(x.StringValue, 0) >= 0 {
				return fmt.Errorf("NUL in text attribute")
			}
			if typ == enumspb.INDEXED_VALUE_TYPE_DATETIME {
				if _, e := time.Parse("2006-01-02 15:04:05.999999", x.StringValue); e != nil {
					return e
				}
			}
			if typ == enumspb.INDEXED_VALUE_TYPE_TEXT {
				if _, e := TextMatch(x.StringValue, "probe"); e != nil {
					return e
				}
			}
		case enumspb.INDEXED_VALUE_TYPE_INT:
			if _, ok := v.Scalar.(*wire.QueryValue_IntValue); !ok {
				return fmt.Errorf("invalid integer attribute")
			}
		case enumspb.INDEXED_VALUE_TYPE_BOOL:
			if _, ok := v.Scalar.(*wire.QueryValue_BoolValue); !ok {
				return fmt.Errorf("invalid boolean attribute")
			}
		case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
			x, ok := v.Scalar.(*wire.QueryValue_DoubleValue)
			if !ok {
				return fmt.Errorf("invalid double attribute")
			}
			if _, e := decimalStored(x.DoubleValue); e != nil {
				return e
			}
		}
	}
	return nil
}

func NormalizedScalar(a *wire.VisibilityAttribute) (any, error) {
	if a == nil || a.Missing {
		return nil, nil
	}
	if enumspb.IndexedValueType(a.ValueType) == enumspb.INDEXED_VALUE_TYPE_DOUBLE {
		r, e := decimalStored(a.Values[0].GetDoubleValue())
		if e != nil {
			return nil, e
		}
		v, _ := r.Float64()
		return v, nil
	}
	if enumspb.IndexedValueType(a.ValueType) == enumspb.INDEXED_VALUE_TYPE_DATETIME {
		return time.Parse("2006-01-02 15:04:05.999999", a.Values[0].GetStringValue())
	}
	return Scalar(a), nil
}
