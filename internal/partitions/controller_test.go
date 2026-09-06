package partitions

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
	"testing"
)

const testInc identity.IncarnationID = "inc_0000000000000000000001"
const otherInc identity.IncarnationID = "inc_0000000000000000000002"
const testPart identity.PartitionID = "prt_0000000000000000000001"
const testKey registry.Key = "cluster/control"

func tid(n int) identity.TransitionID { return identity.TransitionID(fmt.Sprintf("trn_%022d", n)) }
func testOwner(i identity.IncarnationID) cluster.Owner {
	return cluster.Owner{Node: "nod_0000000000000000000001", Incarnation: i, Address: "localhost:8080"}
}
func testLayout() cluster.Layout {
	return cluster.Layout{Version: 1, Placement: cluster.DefaultPlacementConfig(), Partitions: []cluster.PhysicalPartition{{LogicalName: "partition-a", ID: testPart, Path: "data/a"}}}
}
func testConfig() ControllerConfig {
	digest, _ := testLayout().Digest()
	return ControllerConfig{ExpectedLayoutDigest: digest, Key: testKey, Partition: testPart, Incarnation: testInc, MaxControlBytes: 1 << 20}
}
func encoded(t *testing.T, v registry.Version, w registry.Write) registry.Record {
	t.Helper()
	b, e := registry.Encode(testKey, v, w)
	if e != nil {
		t.Fatal(e)
	}
	return registry.Record{Body: b, Version: registry.Version(w.Transition)}
}
func fixture(t *testing.T) registry.Record {
	t.Helper()
	layout := testLayout()
	w, e := cluster.BootstrapWrite(testKey, tid(1), cluster.Control{Format: cluster.ControlFormat, Layout: &layout, Cluster: "clu_0000000000000000000001", Coordinator: cluster.Coordinator{Incarnation: testInc, Generation: 1}, AssignmentRevision: 1, Partitions: map[identity.PartitionID]cluster.PartitionControl{testPart: {Path: "data/a", Desired: testOwner(testInc), AssignmentRevision: 1}}}, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	return encoded(t, "", w)
}
func snap(t *testing.T, r registry.Record) cluster.Snapshot {
	t.Helper()
	s, e := cluster.DecodeControl(testKey, r, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func step(s State, e Event) (State, []Effect) { e.Incarnation = testInc; return Step(s, e) }
func only(t *testing.T, es []Effect, k EffectKind) Effect {
	t.Helper()
	if len(es) != 1 || es[0].Kind != k {
		t.Fatalf("want effect %v got %+v", k, es)
	}
	return es[0]
}
func reservation(t *testing.T) (State, Effect, registry.Record) {
	t.Helper()
	s, e := NewState(testConfig())
	if e != nil {
		t.Fatal(e)
	}
	s, es := step(s, Event{Kind: Poll})
	r := only(t, es, ReadControl)
	base := fixture(t)
	s, es = step(s, Event{Kind: ReadCompleted, Effect: r.ID, Record: base, Transition: tid(2)})
	p := only(t, es, PublishControl)
	return s, p, encoded(t, p.Expected, p.Write)
}
func opening(t *testing.T) (State, Effect, registry.Record) {
	t.Helper()
	s, p, r := reservation(t)
	s, es := step(s, Event{Kind: PublishCompleted, Effect: p.ID, Record: r})
	return s, only(t, es, OpenEngine), r
}

func TestUnknownReservationSurvivesCoordinatorReplacement(t *testing.T) {
	s, p, r := reservation(t)
	s, es := step(s, Event{Kind: PublishCompleted, Effect: p.ID, Err: &registry.UnknownOutcome{Key: testKey, Transition: p.Write.Transition, Cause: context.Canceled}})
	read := only(t, es, ReadControl)
	// A different envelope retains the reservation, proving current authority.
	w, err := snap(t, r).ChangeCoordinator(tid(3), cluster.CoordinatorChange{Expected: r.Version, Incarnation: otherInc, Generation: 2})
	if err != nil {
		t.Fatal(err)
	}
	r = encoded(t, r.Version, w)
	s, es = step(s, Event{Kind: ReadCompleted, Effect: read.ID, Record: r, Transition: tid(4)})
	op := only(t, es, OpenEngine)
	if op.Open.Reservation != p.Write.Transition || s.LastUnknown == nil {
		t.Fatal("lost reservation or historical ambiguity")
	}
	for range 3 {
		s, es = step(s, Event{Kind: Poll})
		read = only(t, es, ReadControl)
		s, es = step(s, Event{Kind: ReadCompleted, Effect: read.ID, Record: r, Transition: tid(5)})
		if len(es) != 0 {
			t.Fatal("duplicate open", es)
		}
	}
	s, es = step(s, Event{Kind: OpenCompleted, Effect: op.ID, HasWriter: true})
	read = only(t, es, ReadControl)
	s, es = step(s, Event{Kind: ReadCompleted, Effect: read.ID, Record: r, Transition: tid(6)})
	ready := only(t, es, PublishControl)
	s, es = step(s, Event{Kind: PublishCompleted, Effect: ready.ID, Record: encoded(t, ready.Expected, ready.Write)})
	if s.Phase != Ready || len(es) != 0 {
		t.Fatal("did not activate", s.Phase, es)
	}
}
func TestUnknownRetriesOriginalConditionAndIdentity(t *testing.T) {
	s, p, _ := reservation(t)
	s, es := step(s, Event{Kind: PublishCompleted, Effect: p.ID, Err: &registry.UnknownOutcome{Key: testKey, Transition: p.Write.Transition, Cause: context.Canceled}})
	read := only(t, es, ReadControl)
	s, es = step(s, Event{Kind: ReadCompleted, Effect: read.ID, Record: fixture(t), Transition: tid(9)})
	if len(es) != 0 {
		t.Fatal(es)
	}
	_, es = step(s, Event{Kind: Poll})
	retry := only(t, es, PublishControl)
	if retry.Expected != p.Expected || retry.Write.Transition != p.Write.Transition || retry.Write.Digest != p.Write.Digest {
		t.Fatal("changed unknown retry")
	}
}
func TestLateOpenAfterAssignmentABAClosesBeforeReacquiring(t *testing.T) {
	s, op, r := opening(t)
	for n, i := range []identity.IncarnationID{otherInc, testInc} {
		w, e := snap(t, r).Assign(testInc, tid(10+n), map[identity.PartitionID]cluster.Owner{testPart: testOwner(i)})
		if e != nil {
			t.Fatal(e)
		}
		r = encoded(t, r.Version, w)
	}
	s, es := step(s, Event{Kind: Poll})
	read := only(t, es, ReadControl)
	s, es = step(s, Event{Kind: ReadCompleted, Effect: read.ID, Record: r, Transition: tid(12)})
	if len(es) != 0 || s.opening != op.ID {
		t.Fatal("released native exclusion")
	}
	s, es = step(s, Event{Kind: OpenCompleted, Effect: op.ID, HasWriter: true})
	cl := only(t, es, CloseEngine)
	if cl.Handle != op.ID {
		t.Fatal("closed wrong handle")
	}
	s, es = step(s, Event{Kind: OpenCompleted, Effect: op.ID, HasWriter: true})
	if len(es) != 0 {
		t.Fatal("duplicate completion")
	}
	s, es = step(s, Event{Kind: CloseCompleted, Effect: cl.ID})
	only(t, es, ReadControl)
	if s.handle != 0 {
		t.Fatal("handle retained after close")
	}
}
func TestFenceWhileReadPendingCannotPublishReady(t *testing.T) {
	s, op, r := opening(t)
	s, es := step(s, Event{Kind: OpenCompleted, Effect: op.ID, HasWriter: true})
	read := only(t, es, ReadControl)
	s, es = step(s, Event{Kind: Fenced, Handle: op.ID})
	only(t, es, CloseEngine)
	_, es = step(s, Event{Kind: ReadCompleted, Effect: read.ID, Record: r, Transition: tid(15)})
	if len(es) != 0 {
		t.Fatal("fenced read published", es)
	}
}
func TestStopRetainsOpenAndRejectsOldIncarnation(t *testing.T) {
	s, op, _ := opening(t)
	s, es := step(s, Event{Kind: Stop})
	if len(es) != 0 || s.opening != op.ID {
		t.Fatal("lost pending open")
	}
	s, es = Step(s, Event{Kind: OpenCompleted, Effect: op.ID, Incarnation: otherInc, HasWriter: true})
	if len(es) != 0 || s.opening != op.ID {
		t.Fatal("accepted old incarnation")
	}
	s, es = step(s, Event{Kind: OpenCompleted, Effect: op.ID, HasWriter: true, Err: context.Canceled})
	cl := only(t, es, CloseEngine)
	s, es = step(s, Event{Kind: CloseCompleted, Effect: cl.ID, Err: errors.New("close failed")})
	if len(es) != 0 || s.handle == 0 {
		t.Fatal("lost failed close")
	}
	s, es = step(s, Event{Kind: Poll})
	cl = only(t, es, CloseEngine)
	s, es = step(s, Event{Kind: CloseCompleted, Effect: cl.ID})
	if s.Phase != Stopped || len(s.Pending()) != 0 {
		t.Fatal("not stopped")
	}
}

func TestUnknownReadyReconcilesUnchangedAndNewerControl(t *testing.T) {
	for _, newer := range []bool{false, true} {
		t.Run(fmt.Sprintf("newer=%t", newer), func(t *testing.T) {
			s, op, r := opening(t)
			s, es := step(s, Event{Kind: OpenCompleted, Effect: op.ID, HasWriter: true})
			read := only(t, es, ReadControl)
			s, es = step(s, Event{Kind: ReadCompleted, Effect: read.ID, Record: r, Transition: tid(30)})
			original := only(t, es, PublishControl)
			s, es = step(s, Event{Kind: PublishCompleted, Effect: original.ID, Err: &registry.UnknownOutcome{Key: testKey, Transition: original.Write.Transition, Cause: context.Canceled}})
			read = only(t, es, ReadControl)
			if newer {
				w, err := snap(t, r).ChangeCoordinator(tid(31), cluster.CoordinatorChange{Expected: r.Version, Incarnation: otherInc, Generation: 2})
				if err != nil {
					t.Fatal(err)
				}
				r = encoded(t, r.Version, w)
			}
			s, es = step(s, Event{Kind: ReadCompleted, Effect: read.ID, Record: r, Transition: tid(32)})
			if !newer {
				if len(es) != 0 {
					t.Fatalf("read retried before Poll: %+v", es)
				}
				s, es = step(s, Event{Kind: Poll})
			}
			next := only(t, es, PublishControl)
			if !newer {
				if next.Expected != original.Expected || next.Write.Transition != original.Write.Transition || next.Write.Digest != original.Write.Digest || !bytes.Equal(next.Write.Body, original.Write.Body) {
					t.Fatal("unknown ready attempt changed")
				}
			} else if next.Expected != r.Version || next.Write.Transition != tid(32) {
				t.Fatal("newer control did not reconcile current readiness")
			}
			if s.Handle() != op.ID || s.Attempt() != op.Open {
				t.Fatal("reconciliation replaced native reservation")
			}
			s, es = step(s, Event{Kind: PublishCompleted, Effect: next.ID, Record: encoded(t, next.Expected, next.Write)})
			if s.Phase != Ready || s.Handle() != op.ID || len(es) != 0 {
				t.Fatal("retained writer did not become ready")
			}
		})
	}
}
