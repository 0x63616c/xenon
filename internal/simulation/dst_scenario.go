package simulation

import (
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
	return NewCoupledInterleavingsScenario(scenario)
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
