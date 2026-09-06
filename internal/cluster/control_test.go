package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
)

const partitionA identity.PartitionID = "prt_0000000000000000000001"
const partitionB identity.PartitionID = "prt_0000000000000000000002"
const controlKey registry.Key = "cluster/control"
const controlLimit = 1 << 20

func transition(n int) identity.TransitionID {
	return identity.TransitionID(fmt.Sprintf("trn_%022d", n))
}
func owner(inc identity.IncarnationID) Owner {
	return Owner{Node: "nod_0000000000000000000001", Incarnation: inc, Address: "localhost:8080"}
}
func fixtureLayout() Layout {
	return Layout{Version: 1, Placement: DefaultPlacementConfig(), Partitions: []PhysicalPartition{{LogicalName: "partition-a", ID: partitionA, Path: "data/a"}, {LogicalName: "partition-b", ID: partitionB, Path: "data/b"}}}
}
func fixtureControl() Control {
	layout := fixtureLayout()
	return Control{Format: ControlFormat, Layout: &layout, Cluster: "clu_0000000000000000000001", Coordinator: Coordinator{leader, 1, 0}, AssignmentRevision: 1, Partitions: map[identity.PartitionID]PartitionControl{
		partitionA: {Path: "data/a", Desired: owner(leader), AssignmentRevision: 1},
		partitionB: {Path: "data/b", Desired: owner(leader), AssignmentRevision: 1},
	}}
}
func publishFixture(t *testing.T, expected registry.Version, w registry.Write, version string) Snapshot {
	t.Helper()
	encoded, err := registry.Encode(controlKey, expected, w)
	if err != nil {
		t.Fatal(err)
	}
	s, err := DecodeControl(controlKey, registry.Record{Body: encoded, Version: registry.Version(version)}, controlLimit)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func snapshotFixture(t *testing.T) Snapshot {
	t.Helper()
	w, err := BootstrapWrite(controlKey, transition(1), fixtureControl(), controlLimit)
	if err != nil {
		t.Fatal(err)
	}
	return publishFixture(t, "", w, "v1")
}

func TestAssignmentABARejectsOldReadyAndPreservesOtherOwners(t *testing.T) {
	s := snapshotFixture(t)
	w, err := s.Reserve(leader, partitionA, 1, transition(2))
	if err != nil {
		t.Fatal(err)
	}
	s = publishFixture(t, s.version, w, "v2")
	w, err = s.Reserve(leader, partitionB, 1, transition(3))
	if err != nil {
		t.Fatal(err)
	}
	s = publishFixture(t, s.version, w, "v3")
	w, err = s.MarkReady(leader, partitionB, 1, 1, transition(3), transition(4))
	if err != nil {
		t.Fatal(err)
	}
	s = publishFixture(t, s.version, w, "v4")
	unchanged := s.Control().Partitions[partitionB]
	for i, inc := range []identity.IncarnationID{contender, leader} {
		w, err = s.Assign(leader, transition(5+i), map[identity.PartitionID]Owner{partitionA: owner(inc)})
		if err != nil {
			t.Fatal(err)
		}
		s = publishFixture(t, s.version, w, fmt.Sprint("plan", i))
	}
	if _, err := s.MarkReady(leader, partitionA, 1, 1, transition(2), transition(7)); !errors.Is(err, ErrStaleControl) {
		t.Fatalf("ABA activated old open: %v", err)
	}
	if s.Control().Partitions[partitionB] != unchanged {
		t.Fatal("unrelated assignment revoked healthy owner")
	}
	p := s.Control().Partitions[partitionA]
	if p.Path != "data/a" || p.AssignmentRevision != 3 || p.Generation != 1 || p.Ready || p.Reservation != "" {
		t.Fatalf("bad ABA state: %+v", p)
	}
	w, err = s.Reserve(leader, partitionA, 3, transition(8))
	if err != nil {
		t.Fatal(err)
	}
	s = publishFixture(t, s.version, w, "v8")
	w, err = s.MarkReady(leader, partitionA, 3, 2, transition(8), transition(9))
	if err != nil {
		t.Fatal(err)
	}
	s = publishFixture(t, s.version, w, "v9")
	if !s.Control().Partitions[partitionA].Ready {
		t.Fatal("current recovered writer cannot become ready")
	}
}

func TestControlAuthorizationAndSnapshotOwnership(t *testing.T) {
	s := snapshotFixture(t)
	copy := s.Control()
	delete(copy.Partitions, partitionA)
	if len(s.Control().Partitions) != 2 {
		t.Fatal("caller mutated snapshot")
	}
	if _, err := s.Assign(contender, transition(2), map[identity.PartitionID]Owner{partitionA: owner(contender)}); !errors.Is(err, ErrStaleControl) {
		t.Fatal(err)
	}
	if _, err := s.Reserve(contender, partitionA, 1, transition(2)); !errors.Is(err, ErrStaleControl) {
		t.Fatal(err)
	}
	w, err := s.ChangeCoordinator(transition(2), CoordinatorChange{Expected: s.version, Incarnation: contender, Generation: 2})
	if err != nil {
		t.Fatal(err)
	}
	next := publishFixture(t, s.version, w, "v2")
	if _, err := next.Assign(leader, transition(3), map[identity.PartitionID]Owner{partitionA: owner(contender)}); !errors.Is(err, ErrStaleControl) {
		t.Fatal("old leader acquired new CAS version")
	}
	if s.Authority().Incarnation != leader || next.Authority().Incarnation != contender {
		t.Fatal("proposal mutated source snapshot")
	}
	if next.Control().Partitions[partitionA] != s.Control().Partitions[partitionA] {
		t.Fatal("turnover changed assignment")
	}
	if _, err := next.ChangeCoordinator(transition(3), CoordinatorChange{Expected: s.version, Incarnation: leader, Generation: 3}); !errors.Is(err, ErrStaleControl) {
		t.Fatal("accepted foreign CAS condition")
	}
}

func TestControlCodecAndBoundedPublication(t *testing.T) {
	c := fixtureControl()
	if _, err := BootstrapWrite(controlKey, transition(1), c, 64); !errors.Is(err, ErrControlLimit) {
		t.Fatal("oversized record accepted")
	}
	c.Partitions[partitionB] = c.Partitions[partitionA]
	if _, err := BootstrapWrite(controlKey, transition(1), c, controlLimit); !errors.Is(err, ErrInvalidControl) {
		t.Fatal("aliased database paths accepted")
	}
	c = fixtureControl()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range [][]byte{append(raw, ' '), append([]byte(`{"format":1,`), raw[1:]...)} {
		w, err := registry.NewWrite(controlKey, "", transition(1), payload)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := registry.Encode(controlKey, "", w)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodeControl(controlKey, registry.Record{Body: encoded, Version: "v1"}, controlLimit); !errors.Is(err, ErrInvalidControl) {
			t.Fatalf("noncanonical/duplicate key accepted: %v", err)
		}
	}
}

func TestMoveBudgetSurvivesCoordinatorReplacement(t *testing.T) {
	s := snapshotFixture(t)
	w, err := s.Assign(leader, transition(2), map[identity.PartitionID]Owner{partitionA: owner(contender)})
	if err != nil {
		t.Fatal(err)
	}
	s = publishFixture(t, s.version, w, "v2")
	w, err = s.ChangeCoordinator(transition(3), CoordinatorChange{Expected: s.version, Incarnation: contender, Generation: 2})
	if err != nil {
		t.Fatal(err)
	}
	s = publishFixture(t, s.version, w, "v3")
	if s.Control().ActiveMove != partitionA {
		t.Fatal("turnover forgot pending move")
	}
	if _, err := s.Assign(contender, transition(4), map[identity.PartitionID]Owner{partitionB: owner(contender)}); !errors.Is(err, ErrControlLimit) {
		t.Fatal("new coordinator started second move")
	}
	w, err = s.Reserve(contender, partitionA, 2, transition(5))
	if err != nil {
		t.Fatal(err)
	}
	s = publishFixture(t, s.version, w, "v5")
	w, err = s.MarkReady(contender, partitionA, 2, 1, transition(5), transition(6))
	if err != nil {
		t.Fatal(err)
	}
	s = publishFixture(t, s.version, w, "v6")
	if s.Control().ActiveMove != "" {
		t.Fatal("completed move retains budget")
	}
	if _, err := s.Assign(contender, transition(7), map[identity.PartitionID]Owner{partitionB: owner(contender)}); err != nil {
		t.Fatalf("next move cannot progress: %v", err)
	}
}
