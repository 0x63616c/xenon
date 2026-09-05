package visibility

import (
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"math"
	"testing"
	"time"
)

func TestVisibilityTypedEvaluation(t *testing.T) {
	attr := func(typ enumspb.IndexedValueType, v any) *wire.VisibilityAttribute {
		a, e := Value(typ, v)
		if e != nil {
			t.Fatal(e)
		}
		return a
	}
	d := &wire.VisibilityDocument{Attributes: map[string]*wire.VisibilityAttribute{"Int01": attr(enumspb.INDEXED_VALUE_TYPE_INT, int64(math.MaxInt64)), "KeywordList01": attr(enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, []string{"a", "b"}), "Double01": attr(enumspb.INDEXED_VALUE_TYPE_DOUBLE, 1.234565)}}
	predicate := func(field string, typ enumspb.IndexedValueType, op string, vs ...*wire.QueryValue) *wire.QueryExpr {
		return &wire.QueryExpr{Operator: op, Column: &wire.QueryColumn{Field: field, ValueType: int32(typ)}, Values: vs}
	}
	cases := []struct {
		q   *wire.QueryExpr
		yes bool
	}{
		{predicate("Int01", enumspb.INDEXED_VALUE_TYPE_INT, "=", Int(math.MaxInt64)), true},
		{predicate("Int01", enumspb.INDEXED_VALUE_TYPE_INT, "=", Int(math.MaxInt64-1)), false},
		{predicate("KeywordList01", enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, "in", String("b"), String("z")), true},
		{predicate("KeywordList01", enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, "not in", String("b")), false},
		{&wire.QueryExpr{Operator: "not", Children: []*wire.QueryExpr{predicate("Keyword01", enumspb.INDEXED_VALUE_TYPE_KEYWORD, "=", String("x"))}}, false},
		{predicate("Keyword01", enumspb.INDEXED_VALUE_TYPE_KEYWORD, "is null"), true},
		{predicate("Double01", enumspb.INDEXED_VALUE_TYPE_DOUBLE, "=", &wire.QueryValue{Scalar: &wire.QueryValue_DoubleValue{DoubleValue: 1.23457}}), true},
		{predicate("Double01", enumspb.INDEXED_VALUE_TYPE_DOUBLE, "=", &wire.QueryValue{Scalar: &wire.QueryValue_DoubleValue{DoubleValue: 1.234565}}), false},
	}
	for i, c := range cases {
		got, e := Match(d, c.q)
		if e != nil || got != c.yes {
			t.Fatalf("case%d got%v err%v", i, got, e)
		}
	}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	open := &wire.VisibilityDocument{StartTime: Time(start), RunId: "b"}
	closed := &wire.VisibilityDocument{StartTime: Time(start.Add(time.Hour)), CloseTime: Time(start.Add(time.Hour)), RunId: "a"}
	if SortKey(open) >= SortKey(closed) {
		t.Fatal("open sentinel order")
	}
}

func TestVisibilitySystemTimeSentinel(t *testing.T) {
	zero := Time(time.Time{})
	if !physicalTime(zero).Equal(MinimumTime) || !GoTime(zero).IsZero() {
		t.Fatal("system zero sentinel")
	}
	d := &wire.VisibilityDocument{StartTime: zero}
	q := &wire.QueryExpr{Operator: ">", Column: &wire.QueryColumn{Field: "StartTime", ValueType: int32(enumspb.INDEXED_VALUE_TYPE_DATETIME)}, Values: []*wire.QueryValue{String("0500-01-01 00:00:00")}}
	if match, e := Match(d, q); e != nil || !match {
		t.Fatal("query used logical zero instead of physical sentinel", match, e)
	}
	custom, e := Value(enumspb.INDEXED_VALUE_TYPE_DATETIME, time.Time{})
	if e != nil || custom.Values[0].GetStringValue() != "0001-01-01 00:00:00" {
		t.Fatal("custom datetime incorrectly inherited system sentinel", custom, e)
	}
}
