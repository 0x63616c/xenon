package visibility

import (
	"encoding/json"
	"os"
	"testing"
)

type oracleCase struct {
	Vector, Query string
	Match         bool
	Error         bool
}

func TestTextPostgreSQLOracle(t *testing.T) {
	b, e := os.ReadFile("../../proof/visibility/oracle-cases.json")
	if e != nil {
		t.Fatal(e)
	}
	var cases []oracleCase
	if e = json.Unmarshal(b, &cases); e != nil {
		t.Fatal(e)
	}
	if len(cases) < 500 {
		t.Fatal("truncated oracle corpus")
	}
	for i, c := range cases {
		got, e := TextMatch(c.Vector, c.Query)
		if (e != nil) != c.Error || (!c.Error && got != c.Match) {
			t.Errorf("case %d vector=%q query=%q: got %v/%v want %v/error=%v", i, c.Vector, c.Query, got, e, c.Match, c.Error)
		}
	}
}
