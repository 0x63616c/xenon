package cluster

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
)

func controllerConfig(self identity.IncarnationID) ControllerConfig {
	return ControllerConfig{Key: controlKey, Incarnation: self, MaxControlBytes: controlLimit, RenewalInterval: 5 * time.Nanosecond, SuspectAfter: 20 * time.Nanosecond, Placement: DefaultPlacementConfig(), Slots: []identity.PartitionID{partitionA, partitionB}}
}
func controllerState(t *testing.T, self identity.IncarnationID) State {
	t.Helper()
	s, err := NewState(controllerConfig(self))
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func controlRecord(t *testing.T, c Control, version string, id int) registry.Record {
	t.Helper()
	w, err := BootstrapWrite(controlKey, transition(id), c, controlLimit)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := registry.Encode(controlKey, "", w)
	if err != nil {
		t.Fatal(err)
	}
	return registry.Record{Body: raw, Version: registry.Version(version)}
}
func effectRecord(t *testing.T, e Effect, version string) registry.Record {
	t.Helper()
	raw, err := registry.Encode(e.Key, e.Expected, e.Write)
	if err != nil {
		t.Fatal(err)
	}
	return registry.Record{Body: raw, Version: registry.Version(version)}
}
func pollController(t *testing.T, s State, at Tick, members ...Owner) (State, Effect) {
	t.Helper()
	next, effects := Step(s, Event{Kind: Poll, At: at, Incarnation: s.config.Incarnation, Members: members})
	if len(effects) != 1 || effects[0].Kind != ReadControl {
		t.Fatal("expected fresh read", effects, next.LastError)
	}
	return next, effects[0]
}
func completeRead(s State, e Effect, r registry.Record, id int) (State, []Effect) {
	return Step(s, Event{Kind: ReadCompleted, At: s.at, Incarnation: e.Incarnation, Effect: e.ID, Record: r, Transition: transition(id)})
}
func completePublish(s State, e Effect, r registry.Record, err error) (State, []Effect) {
	return Step(s, Event{Kind: PublishCompleted, At: s.at, Incarnation: e.Incarnation, Effect: e.ID, Record: r, Err: err})
}
func onlyPublication(t *testing.T, effects []Effect) Effect {
	t.Helper()
	if len(effects) != 1 || effects[0].Kind != PublishControl {
		t.Fatal("expected publication", effects)
	}
	return effects[0]
}
func TestControllerTakeoverRereadsRenewalAndAssignmentABA(t *testing.T) {
	s := controllerState(t, contender)
	c := fixtureControl()
	r := controlRecord(t, c, "v1", 1)
	s, read := pollController(t, s, 0)
	s, effects := completeRead(s, read, r, 2)
	if len(effects) != 0 {
		t.Fatal("premature takeover")
	}
	// A renewal during suspicion restarts observation, even after the old timeout.
	c.Coordinator.Renewal++
	renewed := controlRecord(t, c, "v2", 3)
	s, read = pollController(t, s, 21)
	s, effects = completeRead(s, read, renewed, 4)
	if len(effects) != 0 {
		t.Fatal("changed renewal ignored")
	}
	// Unrelated partition A->B->A edits do not restart leader suspicion, but the
	// takeover must preserve their revisions and use the exact fresh CAS version.
	c.AssignmentRevision = 3
	p := c.Partitions[partitionA]
	p.AssignmentRevision = 3
	c.Partitions[partitionA] = p
	aba := controlRecord(t, c, "v4", 5)
	s, read = pollController(t, s, 41)
	s, effects = completeRead(s, read, aba, 6)
	publication := onlyPublication(t, effects)
	if publication.Expected != "v4" {
		t.Fatal("stale takeover CAS", publication.Expected)
	}
	snap, err := DecodeControl(controlKey, effectRecord(t, publication, "v5"), controlLimit)
	if err != nil {
		t.Fatal(err)
	}
	got := snap.Control()
	if got.Coordinator.Incarnation != contender || got.Coordinator.Generation != 2 || !reflect.DeepEqual(got.Partitions, c.Partitions) {
		t.Fatal("takeover lost current assignment state", got)
	}
	// Concurrent renewal wins: this failed proposal is discarded, and the next
	// decision rereads the actual winner instead of changing its CAS condition.
	s, _ = completePublish(s, publication, registry.Record{}, &registry.Conflict{Key: controlKey})
	if s.Publication() != nil {
		t.Fatal("definite conflict retained as retry")
	}
	c.Coordinator.Renewal++
	s, read = pollController(t, s, 42)
	s, effects = completeRead(s, read, controlRecord(t, c, "v6", 7), 8)
	if len(effects) != 0 {
		t.Fatal("renewal winner did not reset suspicion")
	}
}
func TestControllerRenewThenPlanFromFreshSnapshot(t *testing.T) {
	s := controllerState(t, leader)
	r := controlRecord(t, fixtureControl(), "v1", 1)
	b := Owner{Node: "nod_0000000000000000000002", Incarnation: contender, Address: "b:8080"}
	s, read := pollController(t, s, 0, b)
	s, effects := completeRead(s, read, r, 2)
	renewal := onlyPublication(t, effects)
	renewed := effectRecord(t, renewal, "v2")
	s, effects = completePublish(s, renewal, renewed, nil)
	if len(effects) != 0 {
		t.Fatal("receipt reused as decision authority")
	}
	// Persisted active move from another assignment must prevent a second move.
	snap, _ := DecodeControl(controlKey, renewed, controlLimit)
	w, err := snap.Assign(leader, transition(3), map[identity.PartitionID]Owner{partitionB: b})
	if err != nil {
		t.Fatal(err)
	}
	active := Effect{Key: controlKey, Expected: "v2", Write: w}
	s, read = pollController(t, s, 1, b)
	s, effects = completeRead(s, read, effectRecord(t, active, "v3"), 4)
	if len(effects) != 0 {
		t.Fatal("fresh active move ignored")
	}
	// A live ready assignment releases the budget, permitting the cold first slot.
	c := snap.Control()
	p := c.Partitions[partitionB]
	p.Desired = b
	c.Partitions[partitionB] = p
	s, read = pollController(t, s, 2, b)
	s, effects = completeRead(s, read, controlRecord(t, c, "v4", 5), 6)
	move := onlyPublication(t, effects)
	if move.Expected != "v4" {
		t.Fatal("plan did not use fresh snapshot")
	}
	after, _ := DecodeControl(controlKey, effectRecord(t, move, "v5"), controlLimit)
	if after.Control().ActiveMove != partitionA {
		t.Fatal("did not select cold failed owner")
	}
	s, _ = completePublish(s, move, effectRecord(t, move, "v5"), nil)
	s, read = pollController(t, s, 5, b)
	s, effects = completeRead(s, read, effectRecord(t, move, "v5"), 7)
	next := onlyPublication(t, effects)
	nextSnap, _ := DecodeControl(controlKey, effectRecord(t, next, "v6"), controlLimit)
	if nextSnap.Control().Coordinator.Renewal != 2 {
		t.Fatal("placement starved renewal")
	}
}
func TestControllerUnknownRetainsConditionAndHistoricalOutcome(t *testing.T) {
	s := controllerState(t, leader)
	r := controlRecord(t, fixtureControl(), "v1", 1)
	s, read := pollController(t, s, 0)
	s, effects := completeRead(s, read, r, 2)
	attempt := onlyPublication(t, effects)
	unknown := &registry.UnknownOutcome{Key: controlKey, Transition: attempt.Write.Transition, Cause: errors.New("lost response")}
	s, _ = completePublish(s, attempt, registry.Record{}, unknown)
	s, read = pollController(t, s, 1)
	s, effects = completeRead(s, read, r, 3)
	retry := onlyPublication(t, effects)
	if retry.ID == attempt.ID || retry.Expected != attempt.Expected || retry.Write.Transition != attempt.Write.Transition || retry.Write.Digest != attempt.Write.Digest || !bytes.Equal(retry.Write.Body, attempt.Write.Body) {
		t.Fatal("ambiguous retry changed attempt")
	}
	s, _ = completePublish(s, retry, registry.Record{}, &registry.Conflict{Key: controlKey})
	if s.Publication() == nil || s.LastUnknown == nil {
		t.Fatal("retry conflict erased historical ambiguity")
	}
	c := fixtureControl()
	c.Coordinator = Coordinator{contender, 2, 0}
	s, read = pollController(t, s, 2)
	s, effects = completeRead(s, read, controlRecord(t, c, "v3", 4), 5)
	if len(effects) != 0 || s.Publication() != nil || s.LastUnknown == nil || s.LastUnknown.Effect.Expected != "v1" {
		t.Fatal("supersession relabeled/lost historical attempt")
	}
	// Exported copies cannot rewrite retained history or a future retry.
	view := s.LastUnknown.clone()
	view.Effect.Write.Body[0] ^= 1
	if bytes.Equal(view.Effect.Write.Body, s.LastUnknown.Effect.Write.Body) {
		t.Fatal("history alias")
	}
}
func TestControllerUnknownMatchingReadConfirmsWithoutOldAuthorityReuse(t *testing.T) {
	s := controllerState(t, leader)
	s, read := pollController(t, s, 0)
	s, effects := completeRead(s, read, controlRecord(t, fixtureControl(), "v1", 1), 2)
	attempt := onlyPublication(t, effects)
	s, _ = completePublish(s, attempt, registry.Record{}, &registry.UnknownOutcome{Key: controlKey, Transition: attempt.Write.Transition})
	b := Owner{Node: "nod_0000000000000000000002", Incarnation: contender, Address: "b:8080"}
	s, read = pollController(t, s, 100, b)
	s, effects = completeRead(s, read, effectRecord(t, attempt, "v2"), 3)
	// Matching readback accounts for renewal progress and grants a placement
	// opportunity from this fresh snapshot, even long after the initial attempt.
	if s.lastRenew != 100 || !s.renewed {
		t.Fatal("matching readback did not account for renewal time")
	}
	next := onlyPublication(t, effects)
	if next.Expected != "v2" || next.Write.Transition == attempt.Write.Transition {
		t.Fatal("confirmed read reused old attempt")
	}
}
func TestControllerIgnoresDuplicateWrongIncarnationAndDrainsLatePublication(t *testing.T) {
	s := controllerState(t, leader)
	s, read := pollController(t, s, 0)
	before := s.clone()
	wrong := Event{Kind: ReadCompleted, At: 100, Incarnation: contender, Effect: read.ID}
	next, effects := Step(s, wrong)
	if !reflect.DeepEqual(next, before) || len(effects) != 0 {
		t.Fatal("old incarnation changed state")
	}
	s, effects = completeRead(s, read, controlRecord(t, fixtureControl(), "v1", 1), 2)
	attempt := onlyPublication(t, effects)
	next, effects = completeRead(s, read, controlRecord(t, fixtureControl(), "v1", 1), 3)
	if !reflect.DeepEqual(next, s) || len(effects) != 0 {
		t.Fatal("duplicate completion changed state")
	}
	s, effects = Step(s, Event{Kind: Stop, Incarnation: leader})
	if s.Stopped() || len(effects) != 0 {
		t.Fatal("stopped before pending publication completed")
	}
	s, effects = completePublish(s, attempt, registry.Record{}, &registry.UnknownOutcome{Key: controlKey, Transition: attempt.Write.Transition})
	if !s.Stopped() || len(effects) != 0 || s.LastUnknown == nil {
		t.Fatal("late ambiguous publication lost on drain")
	}
	s, effects = Step(s, Event{Kind: Poll, At: 5, Incarnation: leader})
	if !s.Stopped() || len(effects) != 0 {
		t.Fatal("poll resurrected stopped service")
	}
}
func TestControllerBootstrapRequiresFreshNamespaceAndCannotRepairMissingKey(t *testing.T) {
	s := controllerState(t, leader)
	s, read := pollController(t, s, 0)
	missing := Event{Kind: ReadCompleted, Incarnation: leader, Effect: read.ID, Err: &registry.NotFound{Key: controlKey}, Transition: transition(1)}
	s, effects := Step(s, missing)
	if len(effects) != 0 {
		t.Fatal("implicit bootstrap")
	}
	c := controllerConfig(leader)
	initial := fixtureControl()
	c.Bootstrap = &initial
	if _, err := NewState(c); err == nil {
		t.Fatal("bootstrap without explicit fresh namespace")
	}
	c.FreshNamespace = true
	s, _ = NewState(c)
	s, read = pollController(t, s, 0)
	missing.Effect = read.ID
	s, effects = Step(s, missing)
	create := onlyPublication(t, effects)
	if create.Expected != "" {
		t.Fatal("bootstrap was not absent-key create")
	}
	s, _ = completePublish(s, create, effectRecord(t, create, "v1"), nil)
	s, read = pollController(t, s, 1)
	missing.Effect = read.ID
	missing.At = 1
	s, effects = Step(s, missing)
	if len(effects) != 0 {
		t.Fatal("recreated disappeared live namespace")
	}
}

func TestControllerStalePlanRejectedThenFreshLeaderObserved(t *testing.T) {
	s := controllerState(t, leader)
	b := Owner{Node: "nod_0000000000000000000002", Incarnation: contender, Address: "b:8080"}
	s, read := pollController(t, s, 0, b)
	s, effects := completeRead(s, read, controlRecord(t, fixtureControl(), "v1", 1), 2)
	renewal := onlyPublication(t, effects)
	r := effectRecord(t, renewal, "v2")
	s, _ = completePublish(s, renewal, r, nil)
	s, read = pollController(t, s, 1, b)
	s, effects = completeRead(s, read, r, 3)
	stalePlan := onlyPublication(t, effects)
	snap, _ := DecodeControl(controlKey, r, controlLimit)
	// The old coordinator has prepared its plan, then a replacement wins v2.
	w, err := snap.ChangeCoordinator(transition(4), CoordinatorChange{Expected: "v2", Incarnation: contender, Generation: 2})
	if err != nil {
		t.Fatal(err)
	}
	winner := effectRecord(t, Effect{Key: controlKey, Expected: "v2", Write: w}, "v3")
	s, _ = completePublish(s, stalePlan, registry.Record{}, &registry.Conflict{Key: controlKey})
	s, read = pollController(t, s, 2, b)
	s, effects = completeRead(s, read, winner, 5)
	if len(effects) != 0 || s.Publication() != nil {
		t.Fatal("stale plan rebased over replacement coordinator")
	}
}

func TestControllerMalformedReadRetainsAmbiguousAttemptAndInputOwnership(t *testing.T) {
	config := controllerConfig(leader)
	s, err := NewState(config)
	if err != nil {
		t.Fatal(err)
	}
	config.Slots[0] = partitionB
	if s.Config().Slots[0] != partitionA {
		t.Fatal("configuration alias")
	}
	s, read := pollController(t, s, 0)
	s, effects := completeRead(s, read, controlRecord(t, fixtureControl(), "v1", 1), 2)
	attempt := onlyPublication(t, effects)
	s, _ = completePublish(s, attempt, registry.Record{}, &registry.UnknownOutcome{Key: controlKey, Transition: attempt.Write.Transition})
	s, read = pollController(t, s, 1)
	s, effects = completeRead(s, read, registry.Record{Version: "v2", Body: []byte("bad")}, 3)
	if len(effects) != 0 || s.Publication() == nil || s.LastUnknown == nil {
		t.Fatal("malformed read discarded ambiguity")
	}
	b := Owner{Node: "nod_0000000000000000000002", Incarnation: contender, Address: "b:8080"}
	s, read = pollController(t, s, 2, b)
	s, effects = completeRead(s, read, effectRecord(t, attempt, "v2"), 4)
	if s.LastUnknown != nil {
		t.Fatal("exact matching publication still reported unknown")
	}
	next := onlyPublication(t, effects)
	view := s.Pending()
	view[0].Write.Body[0] ^= 1
	if bytes.Equal(view[0].Write.Body, s.Pending()[0].Write.Body) || !bytes.Equal(next.Write.Body, s.Pending()[0].Write.Body) {
		t.Fatal("pending bytes alias")
	}
	before := s.clone()
	s, effects = Step(s, Event{Kind: Poll, At: 1, Incarnation: leader})
	if len(effects) != 0 || s.at != before.at || s.pending.ID != before.pending.ID || !errors.Is(s.LastError, ErrInvalidController) {
		t.Fatal("regressing clock accepted")
	}
}

func TestControllerSlowPollsCannotStarvePlacementOrRenewal(t *testing.T) {
	s := controllerState(t, leader)
	current := controlRecord(t, fixtureControl(), "v1", 1)
	b := Owner{Node: "nod_0000000000000000000002", Incarnation: contender, Address: "b:8080"}
	wantRenewals := []uint64{1, 1, 2, 2}
	for cycle := 0; cycle < 4; cycle++ {
		var read Effect
		s, read = pollController(t, s, Tick(cycle*10), b)
		var effects []Effect
		s, effects = completeRead(s, read, current, 100+cycle)
		publication := onlyPublication(t, effects)
		proposed := effectRecord(t, publication, fmt.Sprintf("v%d", cycle+2))
		snap, err := DecodeControl(controlKey, proposed, controlLimit)
		if err != nil {
			t.Fatal(err)
		}
		c := snap.Control()
		if c.Coordinator.Renewal != wantRenewals[cycle] {
			t.Fatal("placement or renewal starved", cycle, c.Coordinator)
		}
		if cycle%2 == 1 && c.ActiveMove != partitionA {
			t.Fatal("slow poll did not plan assignment", cycle)
		}
		if cycle == 1 {
			// A failed plan still consumes its credit; next slow poll must renew.
			s, _ = completePublish(s, publication, registry.Record{}, &registry.Conflict{Key: controlKey})
		} else {
			current = proposed
			s, _ = completePublish(s, publication, current, nil)
		}
	}
}

func TestControllerUnknownRenewalRetainedTupleGrantsPlacement(t *testing.T) {
	s := controllerState(t, leader)
	current := controlRecord(t, fixtureControl(), "v1", 1)
	member := Owner{Node: "nod_0000000000000000000002", Incarnation: contender, Address: "b:8080"}
	s, read := pollController(t, s, 0, member)
	s, effects := completeRead(s, read, current, 200)
	for cycle := 0; cycle < 4; cycle++ {
		renewal := onlyPublication(t, effects)
		current = effectRecord(t, renewal, fmt.Sprintf("renew%d", cycle))
		applied, err := DecodeControl(controlKey, current, controlLimit)
		if err != nil {
			t.Fatal(err)
		}
		if applied.Control().Coordinator.Renewal != uint64(cycle+1) || applied.Control().ActiveMove != "" {
			t.Fatal("renewal did not progress", cycle)
		}
		s, _ = completePublish(s, renewal, registry.Record{}, &registry.UnknownOutcome{Key: controlKey, Transition: renewal.Write.Transition})
		// A different owner changes only its reservation before our reconciliation
		// read. The renewal envelope is gone, but its authority tuple remains.
		unrelated, err := applied.Reserve(leader, partitionB, 1, transition(300+cycle))
		if err != nil {
			t.Fatal(err)
		}
		body, err := registry.Encode(controlKey, current.Version, unrelated)
		if err != nil {
			t.Fatal(err)
		}
		current = registry.Record{Body: body, Version: registry.Version(fmt.Sprintf("owner%d", cycle))}
		s, read = pollController(t, s, Tick(cycle*20+10), member)
		s, effects = completeRead(s, read, current, 400+cycle)
		move := onlyPublication(t, effects)
		plan, err := DecodeControl(controlKey, effectRecord(t, move, "planned"), controlLimit)
		if err != nil {
			t.Fatal(err)
		}
		if plan.Control().ActiveMove != partitionA || plan.Control().Coordinator != applied.Control().Coordinator {
			t.Fatal("retained renewal starved placement", cycle)
		}
		if s.LastUnknown == nil || s.LastUnknown.Effect.Write.Transition != renewal.Write.Transition {
			t.Fatal("invented historical renewal success")
		}
		// A competing CAS defeats this placement. Its consumed credit must still
		// permit the next renewal rather than repeatedly submitting assignments.
		s, _ = completePublish(s, move, registry.Record{}, &registry.Conflict{Key: controlKey})
		s, read = pollController(t, s, Tick(cycle*20+20), member)
		s, effects = completeRead(s, read, current, 500+cycle)
	}
}

func TestControllerUnknownRenewalCannotCreditAnotherCoordinator(t *testing.T) {
	s := controllerState(t, leader)
	s, read := pollController(t, s, 0)
	s, effects := completeRead(s, read, controlRecord(t, fixtureControl(), "v1", 1), 600)
	renewal := onlyPublication(t, effects)
	current := effectRecord(t, renewal, "renewed")
	s, _ = completePublish(s, renewal, registry.Record{}, &registry.UnknownOutcome{Key: controlKey, Transition: renewal.Write.Transition})
	applied, err := DecodeControl(controlKey, current, controlLimit)
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := applied.ChangeCoordinator(transition(601), CoordinatorChange{Expected: current.Version, Incarnation: contender, Generation: 2})
	if err != nil {
		t.Fatal(err)
	}
	body, err := registry.Encode(controlKey, current.Version, replacement)
	if err != nil {
		t.Fatal(err)
	}
	current = registry.Record{Body: body, Version: "replacement"}
	s, read = pollController(t, s, 10)
	s, effects = completeRead(s, read, current, 602)
	if s.renewed || s.moveCredit || len(effects) != 0 {
		t.Fatal("another coordinator granted our renewal credit")
	}
	if s.LastUnknown == nil || s.LastUnknown.Effect.Write.Transition != renewal.Write.Transition {
		t.Fatal("lost historical ambiguity after replacement")
	}
}
