package cluster

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/0x63616c/xenon/internal/identity"
	consistent "github.com/buraksezer/consistent"
)

var ErrInvalidPlacement = errors.New("invalid cluster placement")

// PlacementConfig belongs in the immutable persisted cluster layout alongside
// its ordered physical partition IDs. Changing it requires a reviewed migration.
// Version 1 pins buraksezer/consistent v1.1.0 and the fixed parameters below.
type PlacementConfig struct {
	Version  uint32  `json:"version"`
	Hash     string  `json:"hash"`
	Replicas int     `json:"replicas"`
	Load     float64 `json:"load"`
}

func DefaultPlacementConfig() PlacementConfig {
	return PlacementConfig{Version: 1, Hash: "sha256-first8-be", Replicas: 20, Load: 1.25}
}

func (c PlacementConfig) Validate() error {
	if c != DefaultPlacementConfig() {
		return ErrInvalidPlacement
	}
	return nil
}

func validateSlots(slots []identity.PartitionID) error {
	if len(slots) == 0 {
		return ErrInvalidPlacement
	}
	seen := make(map[identity.PartitionID]bool, len(slots))
	for _, id := range slots {
		if id.Validate() != nil || seen[id] {
			return ErrInvalidPlacement
		}
		seen[id] = true
	}
	return nil
}

type placementHasher struct{}

func (placementHasher) Sum64(b []byte) uint64 {
	h := sha256.Sum256(b)
	return binary.BigEndian.Uint64(h[:8])
}

type placementMember identity.NodeID

func (m placementMember) String() string { return string(m) }

// PlanPlacement rebuilds a pure plan from a canonical membership snapshot.
// slots is the IMMUTABLE layout order, not a membership-dependent sort. Slot i
// maps directly to native partition i: LocateKey would hash it a second time and
// lose the partition-count load bound. Incarnations and addresses never enter it.
// The bound concerns counts, not workload, memory, or a supported node capacity.
func PlanPlacement(config PlacementConfig, slots []identity.PartitionID, nodes []identity.NodeID) (map[identity.PartitionID]identity.NodeID, error) {
	if config.Validate() != nil || validateSlots(slots) != nil || len(nodes) == 0 {
		return nil, ErrInvalidPlacement
	}
	nodes = slices.Clone(nodes)
	slices.Sort(nodes)
	members := make([]consistent.Member, len(nodes))
	for i, node := range nodes {
		if node.Validate() != nil || i > 0 && node == nodes[i-1] {
			return nil, ErrInvalidPlacement
		}
		members[i] = placementMember(node)
	}
	ring := consistent.New(members, consistent.Config{PartitionCount: len(slots), ReplicationFactor: config.Replicas, Load: config.Load, Hasher: placementHasher{}, ReplicaKey: consistent.DefaultReplicaKey})
	out := make(map[identity.PartitionID]identity.NodeID, len(slots))
	for i, id := range slots {
		out[id] = identity.NodeID(ring.GetPartitionOwner(i).String())
	}
	return out, nil
}

type PlacementMove struct {
	Partition identity.PartitionID
	Owner     Owner
}

// SelectPlacementMove selects at most one physical database assignment change.
// Control.ActiveMove is the authoritative persisted assignment barrier, retained
// across coordinator replacement; readiness alone is not a move counter. An
// ineligible active target may be replaced for that SAME partition, without
// abandoning its earlier native effects. Bootstrap recovery is not gated on all
// partitions being ready. The caller must CAS this selection through Assign.
func SelectPlacementMove(slots []identity.PartitionID, current Control, planned map[identity.PartitionID]identity.NodeID, eligible []Owner) (PlacementMove, bool, error) {
	if validateSlots(slots) != nil || current.validate() != nil || len(slots) != len(current.Partitions) || len(planned) != len(slots) || len(eligible) == 0 {
		return PlacementMove{}, false, ErrInvalidPlacement
	}
	owners := make(map[identity.NodeID]Owner, len(eligible))
	for _, owner := range eligible {
		if !owner.valid() {
			return PlacementMove{}, false, ErrInvalidPlacement
		}
		if _, duplicate := owners[owner.Node]; duplicate {
			return PlacementMove{}, false, ErrInvalidPlacement
		}
		owners[owner.Node] = owner
	}
	for _, id := range slots {
		_, present := current.Partitions[id]
		node, mapped := planned[id]
		if _, ready := owners[node]; !present || !mapped || !ready {
			return PlacementMove{}, false, ErrInvalidPlacement
		}
	}
	isEligible := func(owner Owner) bool {
		now, ok := owners[owner.Node]
		return ok && now.Incarnation == owner.Incarnation
	}
	if active := current.ActiveMove; active != "" {
		p, ok := current.Partitions[active]
		if !ok {
			return PlacementMove{}, false, ErrInvalidPlacement
		}
		if isEligible(p.Desired) {
			return PlacementMove{}, false, nil
		}
		return PlacementMove{active, owners[planned[active]]}, true, nil
	}
	// ponytail: one move cluster-wide; raise concurrency only with lifecycle and
	// recovery measurements. Repair failed owners before voluntary redistribution.
	for _, recovery := range []bool{true, false} {
		for _, id := range slots {
			p := current.Partitions[id]
			next := owners[planned[id]]
			needsRecovery := !isEligible(p.Desired)
			if needsRecovery == recovery && p.Desired != next {
				return PlacementMove{id, next}, true, nil
			}
		}
	}
	return PlacementMove{}, false, nil
}
