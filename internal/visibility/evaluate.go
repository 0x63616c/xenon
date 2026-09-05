package visibility

import (
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// truth retains SQL UNKNOWN through NOT/AND/OR. Only true selects a row.
type truth int

const (
	unknown     truth = -1
	falsehood   truth = 0
	affirmative truth = 1
)

func negate(t truth) truth {
	if t == unknown {
		return t
	}
	return 1 - t
}
func Match(d *wire.VisibilityDocument, e *wire.QueryExpr) (bool, error) {
	t, err := evaluate(d, e)
	return t == affirmative, err
}
func evaluate(d *wire.VisibilityDocument, e *wire.QueryExpr) (truth, error) {
	if e == nil {
		return affirmative, nil
	}
	op := strings.ToLower(e.Operator)
	if op == "not" {
		if len(e.Children) != 1 {
			return unknown, fmt.Errorf("NOT arity")
		}
		t, err := evaluate(d, e.Children[0])
		return negate(t), err
	}
	if op == "and" || op == "or" {
		if len(e.Children) == 0 {
			return unknown, fmt.Errorf("empty logical expression")
		}
		result := affirmative
		if op == "or" {
			result = falsehood
		}
		for _, child := range e.Children {
			t, err := evaluate(d, child)
			if err != nil {
				return unknown, err
			}
			if op == "and" {
				if t == falsehood {
					result = falsehood
				} else if t == unknown && result != falsehood {
					result = unknown
				}
			} else {
				if t == affirmative {
					result = affirmative
				} else if t == unknown && result != affirmative {
					result = unknown
				}
			}
		}
		return result, nil
	}
	if e.Column == nil {
		return unknown, fmt.Errorf("missing query column")
	}
	a := Field(d, e.Column.Field)
	missing := a == nil || a.Missing
	if op == "is null" {
		if missing {
			return affirmative, nil
		}
		return falsehood, nil
	}
	if op == "is not null" {
		if missing {
			return falsehood, nil
		}
		return affirmative, nil
	}
	if missing {
		return unknown, nil
	}
	if a.ValueType != e.Column.ValueType {
		return unknown, fmt.Errorf("search attribute type mismatch for %s", e.Column.Field)
	}
	if len(e.Values) == 0 {
		return unknown, fmt.Errorf("missing comparison value")
	}
	neg := op == "!=" || op == "not in" || op == "not between" || op == "not starts_with" || op == "not starts with"
	base := op
	switch op {
	case "!=":
		base = "="
	case "not in":
		base = "in"
	case "not between":
		base = "between"
	case "not starts_with", "not starts with":
		base = "starts_with"
	}
	matched := false
	if enumspb.IndexedValueType(a.ValueType) == enumspb.INDEXED_VALUE_TYPE_TEXT {
		if base != "=" || len(e.Values) != 1 {
			return unknown, fmt.Errorf("invalid Text comparison")
		}
		var err error
		matched, err = TextMatch(a.Values[0].GetStringValue(), e.Values[0].GetStringValue())
		if err != nil {
			return unknown, err
		}
	} else if enumspb.IndexedValueType(a.ValueType) == enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST {
		if base != "=" && base != "in" {
			return unknown, fmt.Errorf("invalid KeywordList comparison")
		}
		for _, v := range a.Values {
			for _, q := range e.Values {
				if v.GetStringValue() == q.GetStringValue() {
					matched = true
				}
			}
		}
	} else {
		if len(a.Values) != 1 {
			return unknown, fmt.Errorf("invalid scalar document")
		}
		compare := func(v *wire.QueryValue) (int, error) {
			return Compare(a.Values[0], v, enumspb.IndexedValueType(a.ValueType))
		}
		switch base {
		case "in":
			for _, v := range e.Values {
				c, err := compare(v)
				if err != nil {
					return unknown, err
				}
				matched = matched || c == 0
			}
		case "between":
			if len(e.Values) != 2 {
				return unknown, fmt.Errorf("BETWEEN arity")
			}
			l, err := compare(e.Values[0])
			if err != nil {
				return unknown, err
			}
			h, err := compare(e.Values[1])
			if err != nil {
				return unknown, err
			}
			matched = l >= 0 && h <= 0
		case "starts_with", "starts with":
			matched = strings.HasPrefix(a.Values[0].GetStringValue(), e.Values[0].GetStringValue())
		default:
			if len(e.Values) != 1 {
				return unknown, fmt.Errorf("comparison arity")
			}
			c, err := compare(e.Values[0])
			if err != nil {
				return unknown, err
			}
			switch base {
			case "=":
				matched = c == 0
			case "<":
				matched = c < 0
			case "<=":
				matched = c <= 0
			case ">":
				matched = c > 0
			case ">=":
				matched = c >= 0
			default:
				return unknown, fmt.Errorf("unsupported operator %q", op)
			}
		}
	}
	if neg {
		matched = !matched
	}
	if matched {
		return affirmative, nil
	}
	return falsehood, nil
}
func Compare(a, b *wire.QueryValue, t enumspb.IndexedValueType) (int, error) {
	switch t {
	case enumspb.INDEXED_VALUE_TYPE_INT:
		x, ok := a.Scalar.(*wire.QueryValue_IntValue)
		if !ok {
			return 0, fmt.Errorf("invalid integer")
		}
		y, ok := b.Scalar.(*wire.QueryValue_IntValue)
		if !ok {
			return 0, fmt.Errorf("invalid integer operand")
		}
		if x.IntValue < y.IntValue {
			return -1, nil
		}
		if x.IntValue > y.IntValue {
			return 1, nil
		}
		return 0, nil
	case enumspb.INDEXED_VALUE_TYPE_DOUBLE:
		// PostgreSQL DECIMAL(20,5) stores rounded decimal text; the comparison operand
		// remains full precision. Use rational decimal arithmetic, never binary epsilon.
		x, ok := a.Scalar.(*wire.QueryValue_DoubleValue)
		if !ok {
			return 0, fmt.Errorf("invalid double")
		}
		left, err := decimalStored(x.DoubleValue)
		if err != nil {
			return 0, err
		}
		var text string
		switch y := b.Scalar.(type) {
		case *wire.QueryValue_DoubleValue:
			text = strconv.FormatFloat(y.DoubleValue, 'f', -1, 64)
		case *wire.QueryValue_IntValue:
			text = strconv.FormatInt(y.IntValue, 10)
		default:
			return 0, fmt.Errorf("invalid numeric operand")
		}
		right, ok := new(big.Rat).SetString(text)
		if !ok {
			return 0, fmt.Errorf("invalid numeric operand")
		}
		return left.Cmp(right), nil
	case enumspb.INDEXED_VALUE_TYPE_BOOL:
		x, ok := a.Scalar.(*wire.QueryValue_BoolValue)
		if !ok {
			return 0, fmt.Errorf("invalid bool")
		}
		y, ok := b.Scalar.(*wire.QueryValue_BoolValue)
		if !ok {
			return 0, fmt.Errorf("invalid bool operand")
		}
		if x.BoolValue == y.BoolValue {
			return 0, nil
		}
		if x.BoolValue {
			return 1, nil
		}
		return -1, nil
	case enumspb.INDEXED_VALUE_TYPE_DATETIME:
		x, err := time.Parse("2006-01-02 15:04:05.999999", a.GetStringValue())
		if err != nil {
			return 0, err
		}
		y, err := time.Parse("2006-01-02 15:04:05.999999", b.GetStringValue())
		if err != nil {
			return 0, err
		}
		return x.Compare(y), nil
	default:
		x, ok := a.Scalar.(*wire.QueryValue_StringValue)
		if !ok {
			return 0, fmt.Errorf("invalid string")
		}
		y, ok := b.Scalar.(*wire.QueryValue_StringValue)
		if !ok {
			return 0, fmt.Errorf("invalid string operand")
		}
		return strings.Compare(x.StringValue, y.StringValue), nil
	}
}
func decimalStored(x float64) (*big.Rat, error) {
	r, ok := new(big.Rat).SetString(strconv.FormatFloat(x, 'f', -1, 64))
	if !ok {
		return nil, fmt.Errorf("invalid decimal")
	}
	scale := big.NewInt(100000)
	n := new(big.Int).Mul(r.Num(), scale)
	q, rem := new(big.Int), new(big.Int)
	q.QuoRem(n, r.Denom(), rem)
	if new(big.Int).Lsh(new(big.Int).Abs(rem), 1).Cmp(r.Denom()) >= 0 {
		if n.Sign() < 0 {
			q.Sub(q, big.NewInt(1))
		} else {
			q.Add(q, big.NewInt(1))
		}
	}
	if new(big.Int).Abs(new(big.Int).Set(q)).Cmp(new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)) >= 0 {
		return nil, fmt.Errorf("DECIMAL(20,5) overflow")
	}
	return new(big.Rat).SetFrac(q, scale), nil
}

func ValidateExpression(e *wire.QueryExpr, depth int) error {
	if e == nil {
		return nil
	}
	if depth > 100 {
		return fmt.Errorf("query expression depth exceeded")
	}
	op := strings.ToLower(e.Operator)
	if op == "and" || op == "or" || op == "not" {
		if len(e.Children) == 0 || (op == "not" && len(e.Children) != 1) || e.Column != nil || len(e.Values) > 0 {
			return fmt.Errorf("invalid logical expression")
		}
		for _, c := range e.Children {
			if c == nil {
				return fmt.Errorf("nil logical child")
			}
			if err := ValidateExpression(c, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if e.Column == nil || len(e.Children) > 0 || e.Column.ValueType < 1 || e.Column.ValueType > 7 || len(e.Column.Field) > 256 {
		return fmt.Errorf("invalid query column")
	}
	count := len(e.Values)
	switch op {
	case "is null", "is not null":
		if count != 0 {
			return fmt.Errorf("IS arity")
		}
		return nil
	case "between", "not between":
		if count != 2 {
			return fmt.Errorf("BETWEEN arity")
		}
	case "in", "not in":
		if count < 1 {
			return fmt.Errorf("IN arity")
		}
	case "=", "!=", "<", "<=", ">", ">=", "starts_with", "not starts_with", "starts with", "not starts with":
		if count != 1 {
			return fmt.Errorf("comparison arity")
		}
	default:
		return fmt.Errorf("unknown operator %q", op)
	}
	typ := enumspb.IndexedValueType(e.Column.ValueType)
	for _, v := range e.Values {
		if v == nil {
			return fmt.Errorf("nil query value")
		}
		if typ == enumspb.INDEXED_VALUE_TYPE_DOUBLE {
			switch v.Scalar.(type) {
			case *wire.QueryValue_DoubleValue, *wire.QueryValue_IntValue:
			default:
				return fmt.Errorf("invalid numeric query value")
			}
			continue
		}
		a := &wire.VisibilityAttribute{ValueType: int32(typ), Values: []*wire.QueryValue{v}}
		if typ == enumspb.INDEXED_VALUE_TYPE_TEXT {
			if _, e := TextMatch("", v.GetStringValue()); e != nil {
				return e
			}
			a.ValueType = int32(enumspb.INDEXED_VALUE_TYPE_KEYWORD)
		}
		if e := ValidateAttribute(a); e != nil {
			return e
		}
	}
	return nil
}
