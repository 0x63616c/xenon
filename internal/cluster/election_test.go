package cluster

import (
	"math"
	"testing"
	"time"

	"github.com/0x63616c/xenon/internal/identity"
)

const leader identity.IncarnationID = "inc_0000000000000000000001"
const contender identity.IncarnationID = "inc_0000000000000000000002"

func initial() Authority {
	return Authority{Version: "v1", Incarnation: leader, Generation: 1, Renewal: 4}
}

func TestSuspicionRequiresElapsedLocalTimeAndFreshUnchangedLeader(t *testing.T) {
	a := initial()
	s, err := (Election{}).Observe(0, a)
	if err != nil {
		t.Fatal(err)
	}
	// Owner edits cannot indefinitely postpone failure detection.
	a.Version = "v2-owner-edit"
	s, err = s.Observe(Tick(9*time.Second), a)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.Propose(Tick(9*time.Second), 10*time.Second, contender, a); err != nil || ok {
		t.Fatalf("premature: %v %v", ok, err)
	}
	p, ok, err := s.Propose(Tick(10*time.Second), 10*time.Second, contender, a)
	if err != nil || !ok || p.Expected != a.Version || p.Generation != 2 || p.Renewal != 0 {
		t.Fatalf("proposal: %+v %v %v", p, ok, err)
	}
	// A renewal between timer firing and reread cancels takeover.
	a.Renewal++
	a.Version = "v3-renewal"
	if _, ok, err := s.Propose(Tick(10*time.Second), 10*time.Second, contender, a); err != nil || ok {
		t.Fatalf("ignored renewal: %v %v", ok, err)
	}
	s, err = s.Observe(Tick(10*time.Second), a)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Propose(Tick(19*time.Second), 10*time.Second, contender, a); ok {
		t.Fatal("renewal did not reset suspicion")
	}
	if _, ok, err := s.Propose(Tick(20*time.Second), 10*time.Second, contender, a); err != nil || !ok {
		t.Fatalf("no eventual takeover: %v %v", ok, err)
	}
}

func TestCoordinatorChangeNeverAdoptsNewVersionForOldLeader(t *testing.T) {
	a := initial()
	old, err := Renew(leader, a)
	if err != nil {
		t.Fatal(err)
	}
	a.Version, a.Incarnation, a.Generation = "v2", contender, 2
	if _, err := Renew(leader, a); err == nil {
		t.Fatal("former leader can renew new snapshot")
	}
	if old.Expected != "v1" || old.Incarnation != leader || old.Generation != 1 {
		t.Fatal("old proposal mutated")
	}
}

func TestElectionRejectsInvalidStateWithoutMutation(t *testing.T) {
	a := initial()
	s, _ := (Election{}).Observe(12, a)
	if got, err := s.Observe(11, a); err == nil || got != s {
		t.Fatal("clock regression mutated state")
	}
	bad := a
	bad.Incarnation = "legacy-uuid"
	if got, err := s.Observe(13, bad); err == nil || got != s {
		t.Fatal("invalid identity mutated state")
	}
	if _, ok, _ := s.Propose(100, time.Nanosecond, leader, a); ok {
		t.Fatal("leader attempts self takeover")
	}
	if _, ok, _ := (Election{}).Propose(100, time.Nanosecond, contender, a); ok {
		t.Fatal("restart reused suspicion")
	}
	a.Generation = math.MaxUint64
	s, _ = s.Observe(13, a)
	if _, _, err := s.Propose(100, time.Nanosecond, contender, a); err == nil {
		t.Fatal("generation wrapped")
	}
	a.Renewal = math.MaxUint64
	if _, err := Renew(leader, a); err == nil {
		t.Fatal("renewal wrapped")
	}
}
