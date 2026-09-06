package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/0x63616c/xenon/internal/identity"
)

const transition identity.TransitionID = "trn_0000000000000000000001"
const otherTransition identity.TransitionID = "trn_0000000000000000000002"

func write(t *testing.T, key Key, version Version, id identity.TransitionID, body string) Write {
	t.Helper()
	w, e := NewWrite(key, version, id, []byte(body))
	if e != nil {
		t.Fatal(e)
	}
	return w
}
func record(t *testing.T, key Key, expected Version, w Write, version Version) Record {
	t.Helper()
	b, e := Encode(key, expected, w)
	if e != nil {
		t.Fatal(e)
	}
	return Record{b, version}
}
func TestDigestBindingAndOwnedBytes(t *testing.T) {
	body := []byte("body")
	w, e := NewWrite("control", "v1", transition, body)
	if e != nil {
		t.Fatal(e)
	}
	body[0] = 'X'
	if string(w.Body) != "body" {
		t.Fatal("borrowed input")
	}
	for _, tc := range []struct {
		k Key
		v Version
		b string
	}{{"other", "v1", "body"}, {"control", "v2", "body"}, {"control", "", "body"}, {"control", "v1", "changed"}} {
		changed := w
		changed.Body = []byte(tc.b)
		var invalid *Invalid
		if !errors.As(ValidateWrite(tc.k, tc.v, changed), &invalid) {
			t.Fatal("accepted unbound write", tc)
		}
	}
	// Concatenation ambiguities must not collide.
	a := write(t, "a", "bc", transition, "d")
	b := write(t, "ab", "c", transition, "d")
	if a.Digest == b.Digest {
		t.Fatal("ambiguous field framing")
	}
	r := record(t, "control", "v1", w, "v2")
	clone := r.Clone()
	clone.Body[0] = 'X'
	if r.Body[0] != '{' {
		t.Fatal("borrowed record")
	}
	decoded, e := Decode("control", r)
	if e != nil {
		t.Fatal(e)
	}
	decoded.Body[0] = 'Y'
	again, e := Decode("control", r)
	if e != nil || string(again.Body) != "body" {
		t.Fatal("borrowed decoded body", e)
	}
	// Backend versions are byte-opaque, including non-UTF8 and punctuation.
	opaque := Version(string([]byte{255, 0, '"', '/'}))
	ow := write(t, "control", opaque, transition, "x")
	oe, e := Decode("control", record(t, "control", opaque, ow, "next"))
	if e != nil || Version(oe.Expected) != opaque {
		t.Fatal("version changed", e)
	}
}
func TestStrictEnvelopeAndKeys(t *testing.T) {
	w := write(t, "control", "", transition, "payload")
	r := record(t, "control", "", w, "v1")
	mutations := []func([]byte) []byte{
		func(b []byte) []byte { return append(b, ' ') },
		func(b []byte) []byte { return append(b, []byte("{}")...) },
		func(b []byte) []byte { return bytes.Replace(b, []byte(`"format":1`), []byte(`"format":2`), 1) },
		func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"format":1`), []byte(`"format":1,"format":1`), 1)
		},
		func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"format":1`), []byte(`"format":1,"extra":0`), 1)
		},
		func(b []byte) []byte { return bytes.Replace(b, []byte(`"format":1`), []byte(`"format":1.0`), 1) },
		func(b []byte) []byte { return bytes.Replace(b, []byte("control"), []byte("other"), 1) },
		func(b []byte) []byte {
			return bytes.Replace(b, []byte(transition), []byte("trn_zzzzzzzzzzzzzzzzzzzzzz"), 1)
		},
		func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"expected":""`), []byte(`"expected":"djE="`), 1)
		},
		func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"body":"cGF5bG9hZA=="`), []byte(`"body":"eA=="`), 1)
		},
		func(b []byte) []byte { return bytes.Replace(b, []byte(`"digest":"`), []byte(`"digest":"0`), 1) },
	}
	for i, mutate := range mutations {
		var corrupt *Corrupt
		if _, err := Decode("control", Record{mutate(bytes.Clone(r.Body)), r.Version}); !errors.As(err, &corrupt) {
			t.Fatalf("mutation %d accepted: %v", i, err)
		}
	}
	for _, bad := range []Key{"", "/x", "x/", "x//y", ".", "x/../y", "x/./y", "x\\y", "x\x00y", Key(string([]byte{255})), Key(strings.Repeat("a", 1025))} {
		var invalid *Invalid
		if !errors.As(ValidateKey(bad), &invalid) {
			t.Fatalf("accepted key %q", bad)
		}
	}
	var corrupt *Corrupt
	if _, e := Decode("control", Record{r.Body, ""}); !errors.As(e, &corrupt) {
		t.Fatal(e)
	}
	// Empty payloads have one encoding, regardless of nil versus empty input.
	nilW, _ := NewWrite("control", "", transition, nil)
	emptyW, _ := NewWrite("control", "", transition, []byte{})
	nilB, _ := Encode("control", "", nilW)
	emptyB, _ := Encode("control", "", emptyW)
	if !bytes.Equal(nilB, emptyB) {
		t.Fatal("empty payload alias")
	}
}
func TestAmbiguousAggregateReconciliation(t *testing.T) {
	w := write(t, "control", "old", transition, "value")
	old := record(t, "control", "before", write(t, "control", "before", otherTransition, "prior"), "old")
	matching := record(t, "control", "old", w, "new")
	later := record(t, "control", "new", write(t, "control", "new", otherTransition, "later"), "later")
	reused := record(t, "control", "old", write(t, "control", "old", transition, "different"), "different")
	for _, tc := range []struct {
		name    string
		r       Record
		readErr error
		want    Resolution
		kind    string
	}{
		{"matched", matching, nil, Published, ""}, {"original remains", old, nil, RetrySameWrite, ""},
		{"later cannot disambiguate", later, nil, Unresolved, "unknown"},
		{"reused identity", reused, nil, Unresolved, "invalid"},
		{"read unavailable", Record{}, &Unavailable{"control", context.Canceled}, Unresolved, "unknown"},
		{"missing replacement", Record{}, &NotFound{"control"}, Unresolved, "unknown"},
		{"corrupt read", Record{[]byte("{}"), "x"}, nil, Unresolved, "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, e := Reconcile("control", "old", w, tc.r, tc.readErr)
			if got != tc.want {
				t.Fatal(got, e)
			}
			var u *UnknownOutcome
			var i *Invalid
			if tc.kind == "unknown" && !errors.As(e, &u) || tc.kind == "invalid" && !errors.As(e, &i) || tc.kind == "" && e != nil {
				t.Fatal(e)
			}
		})
	}
	create := write(t, "control", "", transition, "value")
	if got, e := Reconcile("control", "", create, Record{}, fmt.Errorf("read: %w", &NotFound{"control"})); got != RetrySameWrite || e != nil {
		t.Fatal(got, e)
	}
	if got, e := Reconcile("control", "", create, Record{}, &NotFound{"other"}); got != Unresolved || e == nil {
		t.Fatal(got, e)
	}
	// An automatic retry's conflict is still ambiguous after a lost response. The
	// outer UnknownOutcome classification takes precedence over wrapped causes.
	cause := errors.Join(context.Canceled, &Conflict{"control"})
	u := &UnknownOutcome{"control", transition, cause}
	var found *UnknownOutcome
	if !errors.As(fmt.Errorf("publish: %w", u), &found) || !errors.Is(u, context.Canceled) {
		t.Fatal("lost classification/cause")
	}
	for _, err := range []error{&Unavailable{"control", context.Canceled}, &Corrupt{"control", context.Canceled}} {
		if !errors.Is(err, context.Canceled) {
			t.Fatal("lost cause")
		}
	}
}

func ExampleEncode() {
	// Store implementations persist encoded bytes and return them in Record.Body.
	// Callers prepare payload bytes once and decode Read/Replace results once.
	w, _ := NewWrite("control", "", transition, []byte(`{"revision":1}`))
	encoded, _ := Encode("control", "", w)
	e, _ := Decode("control", Record{Body: encoded, Version: "opaque backend version"})
	var payload struct {
		Revision int `json:"revision"`
	}
	_ = json.Unmarshal(e.Body, &payload)
	fmt.Println(payload.Revision)
	// Output: 1
}

func TestMethodConditions(t *testing.T) {
	create := write(t, "control", "", transition, "body")
	replace := write(t, "control", "observed", transition, "body")
	if ValidateCreate("control", create) != nil || ValidateReplace("control", "observed", replace) != nil {
		t.Fatal("valid conditions rejected")
	}
	for _, err := range []error{ValidateReplace("control", "", create), ValidateCreate("control", replace), ValidateReplace("control", "observed", create)} {
		var invalid *Invalid
		if !errors.As(err, &invalid) {
			t.Fatal("invalid method condition accepted", err)
		}
	}
}
