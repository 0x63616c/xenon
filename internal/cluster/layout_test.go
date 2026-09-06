package cluster

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
)

func TestLayoutPinsOrderedExplicitMapping(t *testing.T) {
	layout := fixtureLayout()
	digest, err := layout.Digest()
	if err != nil || digest == ([32]byte{}) {
		t.Fatal(digest, err)
	}
	p, ok := layout.Resolve("partition-a")
	if !ok || p.ID != partitionA || p.Path != "data/a" {
		t.Fatal("logical mapping changed")
	}
	for _, name := range []string{"order", "name", "path"} {
		t.Run(name, func(t *testing.T) {
			changed := layout.clone()
			switch name {
			case "order":
				changed.Partitions[0], changed.Partitions[1] = changed.Partitions[1], changed.Partitions[0]
			case "name":
				changed.Partitions[0].LogicalName = "another-name"
			case "path":
				changed.Partitions[0].Path = "data/another"
			}
			got, err := changed.Digest()
			if err != nil || got == digest {
				t.Fatal("layout change did not change pin", err)
			}
		})
	}
	for _, name := range []string{"version", "planner", "duplicate name", "duplicate id", "duplicate path", "overlapping path", "empty name", "empty layout"} {
		t.Run(name, func(t *testing.T) {
			changed := layout.clone()
			switch name {
			case "version":
				changed.Version++
			case "planner":
				changed.Placement.Replicas++
			case "duplicate name":
				changed.Partitions[1].LogicalName = changed.Partitions[0].LogicalName
			case "duplicate id":
				changed.Partitions[1].ID = changed.Partitions[0].ID
			case "duplicate path":
				changed.Partitions[1].Path = changed.Partitions[0].Path
			case "overlapping path":
				changed.Partitions[1].Path = changed.Partitions[0].Path + "/child"
			case "empty name":
				changed.Partitions[0].LogicalName = ""
			case "empty layout":
				changed.Partitions = nil
			}
			if _, err := changed.Digest(); !errors.Is(err, ErrLayoutMismatch) {
				t.Fatal("invalid layout accepted", err)
			}
		})
	}
}
func TestControlPreservesLayoutAcrossEveryMutation(t *testing.T) {
	s := snapshotFixture(t)
	expected, _ := fixtureLayout().Digest()
	check := func() {
		t.Helper()
		if err := s.ValidateLayout(expected); err != nil {
			t.Fatal(err)
		}
		copy := s.Control()
		copy.Layout.Partitions[0].Path = "mutated"
		if err := s.ValidateLayout(expected); err != nil {
			t.Fatal("layout aliased", err)
		}
	}
	check()
	w, err := s.ChangeCoordinator(transition(70), CoordinatorChange{Expected: s.Authority().Version, Incarnation: contender, Generation: 2})
	if err != nil {
		t.Fatal(err)
	}
	s = publishFixture(t, s.Authority().Version, w, "renew")
	check()
	w, err = s.Assign(contender, transition(71), map[identity.PartitionID]Owner{partitionA: owner(contender)})
	if err != nil {
		t.Fatal(err)
	}
	s = publishFixture(t, s.Authority().Version, w, "assign")
	check()
	w, err = s.Reserve(contender, partitionA, 2, transition(72))
	if err != nil {
		t.Fatal(err)
	}
	s = publishFixture(t, s.Authority().Version, w, "reserve")
	check()
	w, err = s.MarkReady(contender, partitionA, 2, 1, transition(72), transition(73))
	if err != nil {
		t.Fatal(err)
	}
	s = publishFixture(t, s.Authority().Version, w, "ready")
	check()
	c := s.Control()
	p := c.Partitions[partitionA]
	p.Path = "different/data"
	c.Partitions[partitionA] = p
	if _, err := BootstrapWrite(controlKey, transition(74), c, controlLimit); !errors.Is(err, ErrLayoutMismatch) {
		t.Fatal("physical/layout path mismatch accepted", err)
	}
}
func legacyLayoutRecord(t *testing.T) registry.Record {
	t.Helper()
	c := fixtureControl()
	c.Format = 1
	c.Layout = nil
	body, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	w, err := registry.NewWrite(controlKey, "", transition(80), body)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := registry.Encode(controlKey, "", w)
	if err != nil {
		t.Fatal(err)
	}
	return registry.Record{Version: "legacy", Body: encoded}
}
func TestLayoutLegacyInspectionCannotActivateOrMutate(t *testing.T) {
	r := legacyLayoutRecord(t)
	snapshot, err := DecodeControl(controlKey, r, controlLimit)
	if err != nil {
		t.Fatal("legacy inspection failed", err)
	}
	digest, _ := fixtureLayout().Digest()
	if !errors.Is(snapshot.ValidateLayout(digest), ErrLayoutMismatch) {
		t.Fatal("legacy activated")
	}
	if _, err := snapshot.Reserve(leader, partitionA, 1, transition(81)); !errors.Is(err, ErrLayoutMismatch) {
		t.Fatal("legacy mutated", err)
	}
	s := controllerState(t, leader)
	s, read := pollController(t, s, 0)
	s, effects := completeRead(s, read, r, 82)
	if len(effects) != 0 || !errors.Is(s.LastError, ErrLayoutMismatch) {
		t.Fatal("legacy controller activated", effects, s.LastError)
	}
	config := controllerConfig(leader)
	config.ExpectedLayoutDigest = [32]byte{}
	if _, err := NewState(config); err == nil {
		t.Fatal("zero layout pin accepted")
	}
}
func TestLayoutMismatchPrecedesAmbiguousRetry(t *testing.T) {
	s := controllerState(t, leader)
	s, read := pollController(t, s, 0)
	original := controlRecord(t, fixtureControl(), "initial", 90)
	s, effects := completeRead(s, read, original, 91)
	pending := onlyPublication(t, effects)
	s, _ = completePublish(s, pending, registry.Record{}, &registry.UnknownOutcome{Key: controlKey, Transition: pending.Write.Transition})
	changed := fixtureControl()
	changed.Layout.Partitions[0], changed.Layout.Partitions[1] = changed.Layout.Partitions[1], changed.Layout.Partitions[0]
	// Reuse the original version in this deliberately malformed backend observation
	// to ensure layout is checked BEFORE RetrySameWrite, not only fresh placement.
	wrong := controlRecord(t, changed, "initial", 92)
	s, read = pollController(t, s, 10)
	s, effects = completeRead(s, read, wrong, 93)
	if len(effects) != 0 || !errors.Is(s.LastError, ErrLayoutMismatch) || s.Publication() == nil || s.LastUnknown == nil {
		t.Fatal("mismatch authorized ambiguous retry", effects, s.LastError)
	}
	s, read = pollController(t, s, 20)
	s, effects = completeRead(s, read, original, 94)
	retry := onlyPublication(t, effects)
	if retry.Expected != pending.Expected || retry.Write.Digest != pending.Write.Digest || retry.Write.Transition != pending.Write.Transition {
		t.Fatal("valid reconciliation did not retain original write")
	}
}

func TestBootstrapLayoutIsPinnedAndOwned(t *testing.T) {
	config := controllerConfig(leader)
	initial := fixtureControl()
	config.FreshNamespace = true
	config.Bootstrap = &initial
	s, err := NewState(config)
	if err != nil {
		t.Fatal(err)
	}
	initial.Layout.Partitions[0].LogicalName = "caller-mutated"
	if s.Config().Bootstrap.Layout.Partitions[0].LogicalName != "partition-a" {
		t.Fatal("bootstrap layout alias")
	}
	exposed := s.Config()
	exposed.Bootstrap.Layout.Partitions[0].Path = "another/path"
	if s.Config().Bootstrap.Layout.Partitions[0].Path != "data/a" {
		t.Fatal("configuration view aliases layout")
	}
	if _, err := NewState(config); err == nil {
		t.Fatal("mismatched bootstrap accepted")
	}
}
