package cluster

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"reflect"
	"slices"
	"testing"

	"github.com/0x63616c/xenon/internal/identity"
)

func placementInputs() ([]identity.PartitionID, []identity.NodeID) {
	slots := make([]identity.PartitionID, 1024)
	for i := range slots {
		slots[i] = identity.PartitionID(fmt.Sprintf("prt_%022d", i+1))
	}
	nodes := make([]identity.NodeID, 100)
	for i := range nodes {
		nodes[i] = identity.NodeID(fmt.Sprintf("nod_%022d", i+1))
	}
	return slots, nodes
}

func TestPlacementDeterministicRestartAndChurn(t *testing.T) {
	slots, nodes := placementInputs()
	beforeSlots, beforeNodes := slices.Clone(slots), slices.Clone(nodes)
	config := DefaultPlacementConfig()
	first, err := PlanPlacement(config, slots, nodes)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	// Golden protects persisted slot mapping against accidental hash/library drift.
	if got := fmt.Sprintf("%x", sha256.Sum256(canonical)); got != "6c369ac31ceea53efa276f70fa7a4265a9f6f690a2145e437e99f2cd826cfb7b" {
		t.Fatal("version-1 placement changed", got)
	}
	// Recreate from persisted bytes and differently ordered membership, with no
	// retained ring/history. This also protects the caller-owned layout slices.
	raw, _ := json.Marshal(struct {
		Config PlacementConfig
		Slots  []identity.PartitionID
	}{config, slots})
	var restored struct {
		Config PlacementConfig
		Slots  []identity.PartitionID
	}
	if err = json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	reversed := slices.Clone(nodes)
	slices.Reverse(reversed)
	restarted, err := PlanPlacement(restored.Config, restored.Slots, reversed)
	if err != nil || !maps.Equal(first, restarted) || !slices.Equal(slots, beforeSlots) || !slices.Equal(nodes, beforeNodes) {
		t.Fatal("restart/permutation changed fixed slot identities", err)
	}
	joined := append(slices.Clone(nodes), identity.NodeID("nod_0000000000000000000101"))
	after, err := PlanPlacement(config, slots, joined)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[identity.NodeID]int{}
	moved := 0
	for _, id := range slots {
		if after[id] == "" || !slices.Contains(joined, after[id]) {
			t.Fatal("missing slot or unknown owner", id)
		}
		counts[after[id]]++
		if first[id] != after[id] {
			moved++
		}
	}
	bound := int(math.Ceil(float64(len(slots)) / float64(len(joined)) * 1.25))
	for node, count := range counts {
		if count > bound {
			t.Fatal("partition count bound exceeded", node, count, bound)
		}
	}
	// A broad regression guard, not a universal churn bound: round-robin rebuilds
	// would move most slots here. This fixture must exercise the added owner too.
	if moved == 0 || moved > len(slots)/10 || counts[joined[len(joined)-1]] == 0 {
		t.Fatal("unexpected join redistribution", moved)
	}
	restoredPlan, err := PlanPlacement(config, slots, nodes)
	if err != nil || !maps.Equal(first, restoredPlan) {
		t.Fatal("join/departure changed restored mapping", err)
	}
}

func TestPlacementRejectsUnknownLayoutsAndSchemes(t *testing.T) {
	slots, nodes := placementInputs()
	for _, change := range []func(*PlacementConfig){
		func(c *PlacementConfig) { c.Version++ },
		func(c *PlacementConfig) { c.Hash = "other" },
		func(c *PlacementConfig) { c.Replicas++ },
		func(c *PlacementConfig) { c.Load = 2 },
		func(c *PlacementConfig) { c.Load = math.NaN() },
	} {
		c := DefaultPlacementConfig()
		change(&c)
		if _, err := PlanPlacement(c, slots, nodes); !errors.Is(err, ErrInvalidPlacement) {
			t.Fatal("unknown persisted scheme accepted", c, err)
		}
	}
	for _, input := range []struct {
		slots []identity.PartitionID
		nodes []identity.NodeID
	}{
		{nil, nodes}, {slots, nil},
		{[]identity.PartitionID{slots[0], slots[0]}, nodes},
		{[]identity.PartitionID{"bad"}, nodes},
		{slots, []identity.NodeID{nodes[0], nodes[0]}},
		{slots, []identity.NodeID{"bad"}},
	} {
		if _, err := PlanPlacement(DefaultPlacementConfig(), input.slots, input.nodes); !errors.Is(err, ErrInvalidPlacement) {
			t.Fatal("invalid input accepted", err)
		}
	}
}

func TestPlacementMoveLifecycleAndStablePaths(t *testing.T) {
	slots := []identity.PartitionID{partitionA, partitionB}
	current := fixtureControl() // Both unready: bootstrap does not block recovery.
	unchanged := current.clone()
	a := owner(leader)
	b := Owner{Node: "nod_0000000000000000000002", Incarnation: contender, Address: "other:8080"}
	planned := map[identity.PartitionID]identity.NodeID{partitionA: b.Node, partitionB: b.Node}
	move, ok, err := SelectPlacementMove(slots, current, planned, []Owner{b, a})
	if err != nil || !ok || move != (PlacementMove{partitionA, b}) {
		t.Fatal("bootstrap selection", move, ok, err)
	}
	current.ActiveMove = partitionA
	if _, ok, err = SelectPlacementMove(slots, current, planned, []Owner{a, b}); err != nil || ok {
		t.Fatal("started another move before active completion", err)
	}
	// A dead active destination may be retargeted, but not an unrelated slot.
	current.ActiveMove = partitionB
	move, ok, err = SelectPlacementMove(slots, current, planned, []Owner{b})
	if err != nil || !ok || move != (PlacementMove{partitionB, b}) {
		t.Fatal("dead active destination prevented recovery", move, ok, err)
	}
	// Same stable node, replacement process: incarnation determines eligibility,
	// while membership planning itself uses only the stable NodeID.
	restarted := a
	restarted.Incarnation, restarted.Address = contender, "replacement:8080"
	keepNode := map[identity.PartitionID]identity.NodeID{partitionA: a.Node, partitionB: a.Node}
	current.ActiveMove = partitionA
	move, ok, err = SelectPlacementMove(slots, current, keepNode, []Owner{restarted})
	if err != nil || !ok || move.Owner != restarted || move.Partition != partitionA {
		t.Fatal("replacement incarnation not selected", move, ok, err)
	}
	current.ActiveMove = ""
	if !reflect.DeepEqual(current, unchanged) {
		t.Fatal("selector changed database identities or paths")
	}
	// Recovery takes precedence over a voluntary earlier-slot redistribution.
	p := current.Partitions[partitionB]
	p.Desired = Owner{Node: "nod_0000000000000000000003", Incarnation: contender, Address: "dead:8080"}
	current.Partitions[partitionB] = p
	move, ok, err = SelectPlacementMove(slots, current, planned, []Owner{a, b})
	if err != nil || !ok || move.Partition != partitionB {
		t.Fatal("voluntary move delayed failed-owner recovery", move, ok, err)
	}
	current.ActiveMove = "prt_0000000000000000000003"
	if _, _, err = SelectPlacementMove(slots, current, planned, []Owner{a, b}); !errors.Is(err, ErrInvalidPlacement) {
		t.Fatal("unknown active partition accepted", err)
	}
	current.ActiveMove = ""
	delete(planned, partitionB)
	if _, _, err = SelectPlacementMove(slots, current, planned, []Owner{a, b}); !errors.Is(err, ErrInvalidPlacement) {
		t.Fatal("partial plan accepted", err)
	}
}

// Exercise the selector against published control records, including replacing
// its coordinator while a destination is opening and all other slots are cold.
func TestPlacementMoveSurvivesCoordinatorReplacement(t *testing.T) {
	s := snapshotFixture(t)
	slots := []identity.PartitionID{partitionA, partitionB}
	a := owner(leader)
	b := Owner{Node: "nod_0000000000000000000002", Incarnation: contender, Address: "other:8080"}
	plan := map[identity.PartitionID]identity.NodeID{partitionA: b.Node, partitionB: b.Node}
	move, ok, err := SelectPlacementMove(slots, s.Control(), plan, []Owner{a, b})
	if err != nil || !ok {
		t.Fatal(move, ok, err)
	}
	w, err := s.Assign(leader, transition(20), map[identity.PartitionID]Owner{move.Partition: move.Owner})
	if err != nil {
		t.Fatal(err)
	}
	s = publishFixture(t, s.version, w, "v2")
	w, err = s.ChangeCoordinator(transition(21), CoordinatorChange{Expected: s.version, Incarnation: contender, Generation: 2})
	if err != nil {
		t.Fatal(err)
	}
	s = publishFixture(t, s.version, w, "v3")
	if _, ok, err = SelectPlacementMove(slots, s.Control(), plan, []Owner{a, b}); err != nil || ok {
		t.Fatal("replacement coordinator ignored persisted move", ok, err)
	}
	w, err = s.Reserve(contender, partitionA, 2, transition(22))
	if err != nil {
		t.Fatal(err)
	}
	s = publishFixture(t, s.version, w, "v4")
	w, err = s.MarkReady(contender, partitionA, 2, 1, transition(22), transition(23))
	if err != nil {
		t.Fatal(err)
	}
	s = publishFixture(t, s.version, w, "v5")
	move, ok, err = SelectPlacementMove(slots, s.Control(), plan, []Owner{a, b})
	if err != nil || !ok || move.Partition != partitionB {
		t.Fatal("ready completion did not release next move", move, ok, err)
	}
}

func TestPlacementSmallLayout(t *testing.T) {
	slots, nodes := placementInputs()
	// More eligible nodes than physical databases is legal; surplus nodes route.
	plan, err := PlanPlacement(DefaultPlacementConfig(), slots[:1], nodes)
	if err != nil || len(plan) != 1 || !slices.Contains(nodes, plan[slots[0]]) {
		t.Fatal(plan, err)
	}
}
