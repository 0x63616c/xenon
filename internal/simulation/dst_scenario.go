package simulation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"

	"github.com/0x63616c/xenon/internal/cluster"
	ids "github.com/0x63616c/xenon/internal/identity"
)

// DefaultDSTGenerator returns Xenon's code-authored ownership-move schedule.
// It drives the production cluster and partition state machines entirely in memory.
func DefaultDSTGenerator() (Generator, error) {
	scenario, err := defaultDSTScenario()
	if err != nil {
		return nil, err
	}
	scenarios := []CoupledScenario{
		scenario,
		lostResponseDSTScenario(scenario),
		storageErrorDSTScenario(scenario, "coordinator-old"),
		storageErrorDSTScenario(scenario, "writer-old"),
		sameAddressDSTScenario(scenario),
	}
	for _, fault := range []string{
		"crash_before_commit", "response_lost_after_commit", "drop_then_retry", "duplicate_delivery", "route_refresh", "closed_admission", "overlapping_join",
		"partition_before_reservation", "partition_after_reservation",
		"partition_before_open", "partition_after_open",
		"partition_before_ready", "partition_after_ready",
	} {
		candidate := cloneCoupledScenario(scenario)
		candidate.Steps = append(candidate.Steps, CoupledInput{Action: "seam", Actor: "coordinator-new", At: 100, Fault: fault})
		scenarios = append(scenarios, candidate)
	}
	aba, err := assignmentABADSTScenario(scenario)
	if err != nil {
		return nil, err
	}
	scenarios = append(scenarios, aba)
	generators := make([]*CoupledInterleavings, 0, len(scenarios))
	for _, candidate := range scenarios {
		generator, err := NewCoupledInterleavingsScenario(candidate)
		if err != nil {
			return nil, err
		}
		generators = append(generators, generator)
	}
	raw, err := json.Marshal(scenarios)
	if err != nil {
		return nil, err
	}
	return &dstCatalog{generators: generators, info: GeneratorInfo{"go-dst-catalog-v1", hash(raw), []string{CoupledKind, "code-authored-fault-catalog"}}}, nil
}

func defaultDSTScenario() (CoupledScenario, error) {
	partitionA := ids.PartitionID("prt_0000000000000000000001")
	partitionB := ids.PartitionID("prt_0000000000000000000002")
	first := cluster.Owner{Node: "nod_0000000000000000000001", Incarnation: "inc_0000000000000000000001", Address: "node-1:8080"}
	layout := cluster.Layout{
		Version:   1,
		Placement: cluster.PlacementConfig{Version: 1, Hash: "sha256-first8-be", Replicas: 20, Load: 1.25},
		Partitions: []cluster.PhysicalPartition{
			{LogicalName: "partition-a", ID: partitionA, Path: "data/1"},
			{LogicalName: "partition-b", ID: partitionB, Path: "data/2"},
		},
	}
	digest, err := layout.Digest()
	if err != nil {
		return CoupledScenario{}, err
	}
	view := func(coordinator ids.IncarnationID, generation uint64, node ids.NodeID, incarnation ids.IncarnationID, address string) *cluster.MembershipView {
		return &cluster.MembershipView{Coordinator: coordinator, Generation: generation, Ready: true, Members: []cluster.Owner{{Node: node, Incarnation: incarnation, Address: address}}}
	}
	return CoupledScenario{
		Version: 1, ProductionRevision: "code-authored", Toolchain: runtime.Version(),
		Initial: cluster.Control{
			Layout: &layout, Format: cluster.ControlFormat, Cluster: "clu_0000000000000000000001",
			Coordinator:        cluster.Coordinator{Incarnation: "inc_0000000000000000000001", Generation: 1},
			AssignmentRevision: 1,
			Partitions: map[ids.PartitionID]cluster.PartitionControl{
				partitionA: {Path: "data/1", Desired: first, AssignmentRevision: 1},
				partitionB: {Path: "data/2", Desired: first, AssignmentRevision: 1},
			},
		},
		ExpectedLayoutDigest: digest,
		Actors: []CoupledActor{
			{Name: "coordinator-old", Kind: "cluster", Incarnation: "inc_0000000000000000000001"},
			{Name: "coordinator-new", Kind: "cluster", Incarnation: "inc_0000000000000000000004"},
			{Name: "writer-old", Kind: "partition", Incarnation: "inc_0000000000000000000001", Partition: partitionA},
			{Name: "writer-target", Kind: "partition", Incarnation: "inc_0000000000000000000002", Partition: partitionA},
			{Name: "writer-current", Kind: "partition", Incarnation: "inc_0000000000000000000003", Partition: partitionA},
		},
		RequiredPartition: partitionA, RequiredOwner: "inc_0000000000000000000003",
		Steps: []CoupledInput{
			{Action: "poll", Actor: "writer-old"},
			{Action: "read", Actor: "writer-old", Effect: 1},
			{Action: "deliver", Actor: "writer-old", Effect: 1, Transition: "trn_0000000000000000000101"},
			{Action: "publish", Actor: "writer-old", Effect: 2},
			{Action: "deliver", Actor: "writer-old", Effect: 2},
			{Action: "poll", Actor: "coordinator-old", Membership: view("inc_0000000000000000000001", 1, "nod_0000000000000000000002", "inc_0000000000000000000002", "node-2:8080")},
			{Action: "read", Actor: "coordinator-old", Effect: 1},
			{Action: "deliver", Actor: "coordinator-old", Effect: 1, Transition: "trn_0000000000000000000201"},
			{Action: "publish", Actor: "coordinator-old", Effect: 2},
			{Action: "deliver", Actor: "coordinator-old", Effect: 2},
			{Action: "poll", Actor: "coordinator-old", At: 1, Membership: view("inc_0000000000000000000001", 1, "nod_0000000000000000000002", "inc_0000000000000000000002", "node-2:8080")},
			{Action: "read", Actor: "coordinator-old", At: 1, Effect: 3},
			{Action: "deliver", Actor: "coordinator-old", At: 1, Effect: 3, Transition: "trn_0000000000000000000202"},
			{Action: "publish", Actor: "coordinator-old", At: 1, Effect: 4},
			{Action: "deliver", Actor: "coordinator-old", At: 1, Effect: 4},
			{Action: "poll", Actor: "writer-target"},
			{Action: "read", Actor: "writer-target", Effect: 1},
			{Action: "deliver", Actor: "writer-target", Effect: 1, Transition: "trn_0000000000000000000301"},
			{Action: "publish", Actor: "writer-target", Effect: 2},
			{Action: "deliver", Actor: "writer-target", Effect: 2},
			{Action: "open", Actor: "writer-target", Effect: 3},
			{Action: "deliver", Actor: "writer-target", Effect: 3},
			{Action: "read", Actor: "writer-target", Effect: 4},
			{Action: "deliver", Actor: "writer-target", Effect: 4, Transition: "trn_0000000000000000000302"},
			{Action: "poll", Actor: "coordinator-old", At: 2, Membership: view("inc_0000000000000000000001", 1, "nod_0000000000000000000003", "inc_0000000000000000000003", "node-3:8080")},
			{Action: "read", Actor: "coordinator-old", At: 2, Effect: 5},
			{Action: "deliver", Actor: "coordinator-old", At: 2, Effect: 5, Transition: "trn_0000000000000000000203"},
			{Action: "poll", Actor: "coordinator-new", Membership: view("inc_0000000000000000000001", 1, "nod_0000000000000000000003", "inc_0000000000000000000003", "node-3:8080")},
			{Action: "read", Actor: "coordinator-new", Effect: 1},
			{Action: "deliver", Actor: "coordinator-new", Effect: 1, Transition: "trn_0000000000000000000401"},
			{Action: "poll", Actor: "coordinator-new", At: 20, Membership: view("inc_0000000000000000000001", 1, "nod_0000000000000000000003", "inc_0000000000000000000003", "node-3:8080")},
			{Action: "read", Actor: "coordinator-new", At: 20, Effect: 2},
			{Action: "deliver", Actor: "coordinator-new", At: 20, Effect: 2, Transition: "trn_0000000000000000000402"},
			{Action: "publish", Actor: "coordinator-new", At: 20, Effect: 3},
			{Action: "deliver", Actor: "coordinator-new", At: 20, Effect: 3},
			{Action: "poll", Actor: "coordinator-new", At: 21, Membership: view("inc_0000000000000000000004", 2, "nod_0000000000000000000003", "inc_0000000000000000000003", "node-3:8080")},
			{Action: "read", Actor: "coordinator-new", At: 21, Effect: 4},
			{Action: "deliver", Actor: "coordinator-new", At: 21, Effect: 4, Transition: "trn_0000000000000000000403"},
			{Action: "publish", Actor: "coordinator-new", At: 21, Effect: 5},
			{Action: "deliver", Actor: "coordinator-new", At: 21, Effect: 5},
			{Action: "poll", Actor: "writer-current"},
			{Action: "read", Actor: "writer-current", Effect: 1},
			{Action: "deliver", Actor: "writer-current", Effect: 1, Transition: "trn_0000000000000000000501"},
			{Action: "publish", Actor: "writer-current", Effect: 2},
			{Action: "deliver", Actor: "writer-current", Effect: 2},
			{Action: "open", Actor: "writer-current", Effect: 3},
			{Action: "deliver", Actor: "writer-current", Effect: 3},
			{Action: "read", Actor: "writer-current", Effect: 4},
			{Action: "deliver", Actor: "writer-current", Effect: 4, Transition: "trn_0000000000000000000502"},
			{Action: "publish", Actor: "writer-current", Effect: 5},
			{Action: "deliver", Actor: "writer-current", Effect: 5},
			{Action: "publish", Actor: "coordinator-old", At: 2, Effect: 6, Fault: "stale_plan"},
			{Action: "deliver", Actor: "coordinator-old", At: 2, Effect: 6},
			{Action: "publish", Actor: "writer-target", Effect: 5, Fault: "old_ready"},
			{Action: "deliver", Actor: "writer-target", Effect: 5},
			{Action: "read", Actor: "writer-target", Effect: 6},
			{Action: "deliver", Actor: "writer-target", Effect: 6, Transition: "trn_0000000000000000000303"},
			{Action: "close", Actor: "writer-target", Effect: 7},
			{Action: "deliver", Actor: "writer-target", Effect: 7},
			{Action: "read", Actor: "writer-target", Effect: 8},
			{Action: "deliver", Actor: "writer-target", Effect: 8, Transition: "trn_0000000000000000000304"},
			{Action: "open", Actor: "writer-old", Effect: 3},
			{Action: "deliver", Actor: "writer-old", Effect: 3},
			{Action: "commit", Actor: "writer-current", Effect: 3, Fault: "post_fence_commit"},
			{Action: "fence", Actor: "writer-current", Effect: 3},
			{Action: "close", Actor: "writer-current", Effect: 6},
			{Action: "deliver", Actor: "writer-current", Effect: 6},
			{Action: "read", Actor: "writer-current", Effect: 7},
			{Action: "deliver", Actor: "writer-current", Effect: 7, Transition: "trn_0000000000000000000503"},
			{Action: "publish", Actor: "writer-current", Effect: 8},
			{Action: "deliver", Actor: "writer-current", Effect: 8},
			{Action: "open", Actor: "writer-current", Effect: 9},
			{Action: "deliver", Actor: "writer-current", Effect: 9},
			{Action: "read", Actor: "writer-current", Effect: 10},
			{Action: "deliver", Actor: "writer-current", Effect: 10, Transition: "trn_0000000000000000000504"},
			{Action: "publish", Actor: "writer-current", Effect: 11},
			{Action: "deliver", Actor: "writer-current", Effect: 11},
			{Action: "read", Actor: "writer-old", Effect: 4},
			{Action: "deliver", Actor: "writer-old", Effect: 4, Transition: "trn_0000000000000000000102"},
			{Action: "close", Actor: "writer-old", Effect: 5},
			{Action: "deliver", Actor: "writer-old", Effect: 5},
			{Action: "read", Actor: "writer-old", Effect: 6},
			{Action: "deliver", Actor: "writer-old", Effect: 6, Transition: "trn_0000000000000000000103"},
			{Action: "commit", Actor: "writer-current", Effect: 9},
		},
	}, nil
}

type dstCatalog struct {
	generators []*CoupledInterleavings
	info       GeneratorInfo
}

func (g *dstCatalog) Info() GeneratorInfo {
	out := g.info
	out.Capabilities = append([]string(nil), out.Capabilities...)
	return out
}
func (g *dstCatalog) Next(ctx context.Context, request GenerateRequest) (Scenario, error) {
	if len(g.generators) == 0 {
		return Scenario{}, errors.New("empty DST catalog")
	}
	return g.generators[request.Index%uint64(len(g.generators))].Next(ctx, request)
}

func lostResponseDSTScenario(base CoupledScenario) CoupledScenario {
	c := cloneCoupledScenario(base)
	for i := range c.Steps {
		step := &c.Steps[i]
		if step.Actor == "coordinator-old" && step.Action == "deliver" && step.Effect == 2 {
			step.Fault = "lost_publish_response"
			break
		}
	}
	return c
}

func storageErrorDSTScenario(base CoupledScenario, actor string) CoupledScenario {
	c := cloneCoupledScenario(base)
	prefix := []CoupledInput{{Action: "poll", Actor: actor}, {Action: "read", Actor: actor, Effect: 1, Fault: "storage_read_error"}, {Action: "deliver", Actor: actor, Effect: 1}}
	for _, step := range c.Steps {
		if step.Actor == actor && step.Effect != 0 {
			step.Effect++
		}
		prefix = append(prefix, step)
	}
	c.Steps = prefix
	return c
}

func sameAddressDSTScenario(base CoupledScenario) CoupledScenario {
	c := cloneCoupledScenario(base)
	for i := range c.Steps {
		view := c.Steps[i].Membership
		if view == nil {
			continue
		}
		for j := range view.Members {
			if view.Members[j].Incarnation == "inc_0000000000000000000003" {
				view.Members[j].Address = "node-1:8080"
			}
		}
	}
	return c
}

func assignmentABADSTScenario(base CoupledScenario) (CoupledScenario, error) {
	c := cloneCoupledScenario(base)
	membership := func(actor string, node ids.NodeID, at cluster.Tick) (cluster.MembershipView, error) {
		for _, step := range c.Steps {
			if step.Actor != actor || step.At != at || step.Membership == nil {
				continue
			}
			for _, member := range step.Membership.Members {
				if member.Node == node {
					return *step.Membership, nil
				}
			}
		}
		return cluster.MembershipView{}, fmt.Errorf("missing %s membership for %s at %d", actor, node, at)
	}
	coordinatorView, err := membership("coordinator-new", "nod_0000000000000000000003", 21)
	if err != nil {
		return CoupledScenario{}, err
	}
	emit := func(action, actor string, at cluster.Tick, effect uint64, transition ids.TransitionID) {
		c.Steps = append(c.Steps, CoupledInput{Action: action, Actor: actor, At: at, Effect: effect, Transition: transition})
	}
	for i, fixture := range []struct {
		node ids.NodeID
		at   cluster.Tick
	}{{"nod_0000000000000000000002", 0}, {"nod_0000000000000000000003", 2}} {
		at := cluster.Tick(22 + i)
		read := uint64(6 + i*2)
		publish := read + 1
		view, err := membership("coordinator-old", fixture.node, fixture.at)
		if err != nil {
			return CoupledScenario{}, err
		}
		view.Generation = 2
		view.Coordinator = coordinatorView.Coordinator
		c.Steps = append(c.Steps, CoupledInput{Action: "poll", Actor: "coordinator-new", At: at, Membership: &view})
		emit("read", "coordinator-new", at, read, "")
		transition := ids.TransitionID("trn_0000000000000000000701")
		if i == 1 {
			transition = "trn_0000000000000000000702"
		}
		emit("deliver", "coordinator-new", at, read, transition)
		emit("publish", "coordinator-new", at, publish, "")
		emit("deliver", "coordinator-new", at, publish, "")
	}
	emit("poll", "writer-current", 0, 0, "")
	emit("read", "writer-current", 0, 12, "")
	emit("deliver", "writer-current", 0, 12, "trn_0000000000000000000703")
	emit("close", "writer-current", 0, 13, "")
	emit("deliver", "writer-current", 0, 13, "")
	emit("read", "writer-current", 0, 14, "")
	emit("deliver", "writer-current", 0, 14, "trn_0000000000000000000704")
	emit("publish", "writer-current", 0, 15, "")
	emit("deliver", "writer-current", 0, 15, "")
	emit("open", "writer-current", 0, 16, "")
	emit("deliver", "writer-current", 0, 16, "")
	emit("read", "writer-current", 0, 17, "")
	emit("deliver", "writer-current", 0, 17, "trn_0000000000000000000705")
	emit("publish", "writer-current", 0, 18, "")
	emit("deliver", "writer-current", 0, 18, "")
	emit("commit", "writer-current", 0, 16, "")
	if len(c.Steps) > 256 {
		return CoupledScenario{}, errors.New("assignment ABA DST exceeds bound")
	}
	return c, nil
}

func cloneCoupledScenario(c CoupledScenario) CoupledScenario {
	out := c
	out.Steps = append([]CoupledInput(nil), c.Steps...)
	for i := range out.Steps {
		if c.Steps[i].Membership != nil {
			view := *c.Steps[i].Membership
			view.Members = append([]cluster.Owner(nil), view.Members...)
			out.Steps[i].Membership = &view
		}
	}
	out.Actors = append([]CoupledActor(nil), c.Actors...)
	return out
}
