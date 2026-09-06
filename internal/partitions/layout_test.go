package partitions

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/registry"
)

func mismatchedLayout(t *testing.T, r registry.Record, legacy bool) registry.Record {
	t.Helper()
	control := snap(t, r).Control()
	if legacy {
		control.Format = 1
		control.Layout = nil
	} else {
		control.Layout.Partitions[0].LogicalName = "different-logical-domain"
	}
	body, err := json.Marshal(control)
	if err != nil {
		t.Fatal(err)
	}
	w, err := registry.NewWrite(testKey, "", tid(90), body)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := registry.Encode(testKey, "", w)
	if err != nil {
		t.Fatal(err)
	}
	return registry.Record{Version: r.Version, Body: encoded}
}
func TestLayoutRejectsPartitionActivationAndAmbiguousRetry(t *testing.T) {
	for _, legacy := range []bool{true, false} {
		s, err := NewState(testConfig())
		if err != nil {
			t.Fatal(err)
		}
		s, es := step(s, Event{Kind: Poll})
		read := only(t, es, ReadControl)
		wrong := mismatchedLayout(t, fixture(t), legacy)
		s, es = step(s, Event{Kind: ReadCompleted, Effect: read.ID, Record: wrong, Transition: tid(91)})
		if len(es) != 0 || !errors.Is(s.LastError, cluster.ErrLayoutMismatch) {
			t.Fatal("layout activated native writer", es, s.LastError)
		}
		s, p, _ := reservation(t)
		s, es = step(s, Event{Kind: PublishCompleted, Effect: p.ID, Err: &registry.UnknownOutcome{Key: testKey, Transition: p.Write.Transition}})
		read = only(t, es, ReadControl)
		s, es = step(s, Event{Kind: ReadCompleted, Effect: read.ID, Record: wrong, Transition: tid(92)})
		if len(es) != 0 || s.LastUnknown == nil || !errors.Is(s.LastError, cluster.ErrLayoutMismatch) {
			t.Fatal("layout authorized retry or erased ambiguity")
		}
	}
	config := testConfig()
	config.ExpectedLayoutDigest = [32]byte{}
	if _, err := NewState(config); err == nil {
		t.Fatal("zero pin accepted")
	}
}
func TestLayoutMismatchRetainsLateNativeOpenUntilClose(t *testing.T) {
	s, open, r := opening(t)
	s, es := step(s, Event{Kind: Poll})
	read := only(t, es, ReadControl)
	s, es = step(s, Event{Kind: ReadCompleted, Effect: read.ID, Record: mismatchedLayout(t, r, false), Transition: tid(93)})
	if len(es) != 0 || s.opening != open.ID {
		t.Fatal("layout rejection lost pending native open")
	}
	s, es = step(s, Event{Kind: OpenCompleted, Effect: open.ID, HasWriter: true})
	close := only(t, es, CloseEngine)
	if close.Handle != open.ID || s.Phase == Ready {
		t.Fatal("late wrong-layout writer activated")
	}
	s, es = step(s, Event{Kind: CloseCompleted, Effect: close.ID})
	only(t, es, ReadControl)
	if s.Handle() != 0 {
		t.Fatal("late handle did not retire")
	}
}
