package identity

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

type brokenReader struct{ err error }

func (r brokenReader) Read([]byte) (int, error) { return 0, r.err }

type badSource string

func (s badSource) NewID(string) (string, error) { return string(s), nil }

func TestEncodingVectorsAndTypeBoundaries(t *testing.T) {
	for _, tc := range []struct {
		raw   []byte
		value string
	}{
		{make([]byte, 16), "0000000000000000000000"},
		{append(make([]byte, 15), 1), "0000000000000000000001"},
		{bytes.Repeat([]byte{255}, 16), "7n42DGM5Tflk9n8mt7Fhc7"},
	} {
		for _, prefix := range []string{"clu", "nod", "inc", "prt", "op", "trn", "dev"} {
			got, err := (Generator{bytes.NewReader(tc.raw)}).NewID(prefix)
			if err != nil || got != prefix+"_"+tc.value {
				t.Fatalf("vector %s: %q %v", prefix, got, err)
			}
			if err = validate(got, prefix); err != nil {
				t.Fatal(err)
			}
			for _, other := range []string{"clu", "nod", "inc", "prt", "op", "trn", "dev"} {
				if other != prefix && validate(got, other) == nil {
					t.Fatalf("accepted cross-type %s as %s", got, other)
				}
			}
		}
	}
	for _, bad := range []string{"", "trn_000000000000000000000", "trn_00000000000000000000000", "trn_7n42DGM5Tflk9n8mt7Fhc8", "trn_zzzzzzzzzzzzzzzzzzzzzz", "trn_000000000000000000000/", "TRN_0000000000000000000000", "trn_000000000000000000000é", "00000000-0000-4000-8000-000000000001"} {
		if !errors.Is(TransitionID(bad).Validate(), ErrInvalid) {
			t.Fatalf("accepted %q", bad)
		}
	}
}
func TestTypedConstructorsAndEntropyFailures(t *testing.T) {
	s := Generator{bytes.NewReader(make([]byte, 16*7))}
	d, e := NewDevRunID(s)
	if e != nil || d.Validate() != nil {
		t.Fatal(d, e)
	}
	c, e := NewClusterID(s)
	if e != nil || c.Validate() != nil {
		t.Fatal(c, e)
	}
	n, e := NewNodeID(s)
	if e != nil || n.Validate() != nil {
		t.Fatal(n, e)
	}
	i, e := NewIncarnationID(s)
	if e != nil || i.Validate() != nil {
		t.Fatal(i, e)
	}
	p, e := NewPartitionID(s)
	if e != nil || p.Validate() != nil {
		t.Fatal(p, e)
	}
	o, e := NewOperationID(s)
	if e != nil || o.Validate() != nil {
		t.Fatal(o, e)
	}
	tr, e := NewTransitionID(s)
	if e != nil || tr.Validate() != nil {
		t.Fatal(tr, e)
	}
	for _, size := range []int{0, 1, 15} {
		got, err := (Generator{strings.NewReader(strings.Repeat("a", size))}).NewID("trn")
		want := io.ErrUnexpectedEOF
		if size == 0 {
			want = io.EOF
		}
		if got != "" || !errors.Is(err, want) {
			t.Fatalf("short entropy %d: %q %v", size, got, err)
		}
	}
	boom := errors.New("entropy unavailable")
	if got, err := NewTransitionID(Generator{brokenReader{boom}}); got != "" || !errors.Is(err, boom) {
		t.Fatal(got, err)
	}
	if _, err := NewTransitionID(nil); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := NewTransitionID(badSource("op_0000000000000000000000")); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := (Generator{brokenReader{boom}}).NewID("unknown"); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}
