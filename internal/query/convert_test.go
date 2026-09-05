package query

import (
	"testing"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/protobuf/proto"
)

func configuration() Config {
	return Config{NamespaceID: "10000000-0000-0000-0000-000000000001", Types: searchattribute.NewNameTypeMap(map[string]enums.IndexedValueType{"MyInt": enums.INDEXED_VALUE_TYPE_INT, "MyKeyword": enums.INDEXED_VALUE_TYPE_KEYWORD, "MyText": enums.INDEXED_VALUE_TYPE_TEXT, "MyList": enums.INDEXED_VALUE_TYPE_KEYWORD_LIST, "MyBool": enums.INDEXED_VALUE_TYPE_BOOL}), SchemaVersion: 1}
}
func walk(e *wire.QueryExpr, f func(*wire.QueryExpr)) {
	if e == nil {
		return
	}
	f(e)
	for _, c := range e.Children {
		walk(c, f)
	}
}
func TestPinnedConverterTypedRoundTrip(t *testing.T) {
	q, err := Compile("MyInt = 9007199254740993 AND (MyKeyword IN ('a', 'b') OR MyBool = true)", configuration())
	if err != nil {
		t.Fatal(err)
	}
	b, err := proto.Marshal(q)
	if err != nil {
		t.Fatal(err)
	}
	got := new(wire.VisibilityQuery)
	if err = proto.Unmarshal(b, got); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(q, got) {
		t.Fatal("wire changed query")
	}
	foundInt, foundDivision := false, false
	walk(got.Predicate, func(e *wire.QueryExpr) {
		if e.Column == nil {
			return
		}
		if e.Column.Field == "MyInt" {
			foundInt = len(e.Values) == 1 && e.Values[0].GetIntValue() == 9007199254740993
		}
		if e.Column.Field == "TemporalNamespaceDivision" {
			foundDivision = e.Operator == "is null"
		}
	})
	if !foundInt || !foundDivision {
		t.Fatalf("int precision or default division lost: %v", got)
	}
	first, err := Digest(q)
	if err != nil {
		t.Fatal(err)
	}
	q.SchemaVersion++
	second, err := Digest(q)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("schema changes must invalidate digest")
	}
}
func TestPinnedConverterRejectsInvalidInput(t *testing.T) {
	for _, text := range []string{"MyText = ''", "MyText = '   '", "Unknown = 1", "MyInt = 'wrong'", "MyList > 'a'", "MyText IN ('a')", "WorkflowId =", "ORDER BY StartTime DESC"} {
		t.Run(text, func(t *testing.T) {
			if _, err := Compile(text, configuration()); err == nil {
				t.Fatal("accepted invalid/unsupported input")
			}
		})
	}
	cfg := configuration()
	cfg.NamespaceID = "not-a-uuid"
	if _, err := Compile("", cfg); err == nil {
		t.Fatal("accepted invalid namespace")
	}
}
func TestChasmDivisionAndRange(t *testing.T) {
	cfg := configuration()
	cfg.ArchetypeID = chasm.ArchetypeID(7)
	q, err := Compile("MyInt BETWEEN -5 AND 10", cfg)
	if err != nil {
		t.Fatal(err)
	}
	division, interval := false, false
	walk(q.Predicate, func(e *wire.QueryExpr) {
		if e.Column == nil {
			return
		}
		if e.Column.Field == "TemporalNamespaceDivision" {
			division = e.Operator == "=" && len(e.Values) == 1 && e.Values[0].GetStringValue() == "7"
		}
		if e.Column.Field == "MyInt" {
			interval = e.Operator == "between" && len(e.Values) == 2 && e.Values[0].GetIntValue() == -5 && e.Values[1].GetIntValue() == 10
		}
	})
	if !division || !interval {
		t.Fatalf("CHASM division/range lost: %v", q)
	}
}

type aliasMapper struct{ identityMapper }

func (aliasMapper) GetFieldName(alias, _ string) (string, error) {
	if alias == "Amount" {
		return "MyInt", nil
	}
	return alias, nil
}
func TestAliasGroupingAndNamespaceBinding(t *testing.T) {
	cfg := configuration()
	cfg.Mapper = aliasMapper{}
	q, err := Compile("Amount > 12 GROUP BY ExecutionStatus", cfg)
	if err != nil {
		t.Fatal(err)
	}
	alias := false
	walk(q.Predicate, func(e *wire.QueryExpr) {
		if e.Column != nil && e.Column.Field == "MyInt" {
			alias = e.Column.Alias == "Amount"
		}
	})
	if !alias || len(q.GroupBy) != 1 || q.GroupBy[0].Field != "ExecutionStatus" {
		t.Fatalf("alias/group lost: %v", q)
	}
	if _, err = Compile("GROUP BY MyInt", cfg); err == nil {
		t.Fatal("accepted invalid grouping")
	}
	first, _ := Digest(q)
	cfg.NamespaceID = "20000000-0000-0000-0000-000000000001"
	other, err := Compile("Amount > 12 GROUP BY ExecutionStatus", cfg)
	if err != nil {
		t.Fatal(err)
	}
	second, _ := Digest(other)
	if first == second {
		t.Fatal("namespace missing from digest")
	}
}
