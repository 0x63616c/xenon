package main

import (
	"fmt"
	"github.com/0x63616c/xenon/internal/identity"
)

type Owner struct {
	Node        identity.NodeID        `json:"node"`
	Incarnation identity.IncarnationID `json:"incarnation"`
}
type Coordinator struct {
	Owner           Owner  `json:"owner"`
	Generation      uint64 `json:"generation"`
	RenewalSequence uint64 `json:"renewal_sequence"`
}
type Reservation struct {
	Transition         identity.TransitionID `json:"transition"`
	Desired            Owner                 `json:"desired_owner"`
	AssignmentRevision uint64                `json:"assignment_revision"`
	Generation         uint64                `json:"generation"`
}
type Ready struct {
	Owner              Owner  `json:"owner"`
	AssignmentRevision uint64 `json:"assignment_revision"`
	Generation         uint64 `json:"generation"`
}
type Partition struct {
	DataPrefix  string      `json:"data_prefix"`
	Desired     Owner       `json:"desired_owner"`
	Reservation Reservation `json:"reservation"`
	Ready       Ready       `json:"ready"`
}

// Exact concepts from dst-service-contracts.md, with explicit synthetic JSON
// field names/representation. Heartbeats are separate advisory records. Desired,
// reservation and ready are all populated (including old ready during movement).
// This is NOT the final production format. It adds one bounded last-attempt receipt per actor.
type Receipt struct {
	Transition   identity.TransitionID `json:"transition"`
	IntentDigest string                `json:"intent_digest"`
}
type Control struct {
	Receipts           map[int]Receipt                    `json:"receipts"`
	Format             uint32                             `json:"format"`
	Cluster            identity.ClusterID                 `json:"cluster"`
	Coordinator        Coordinator                        `json:"coordinator"`
	AssignmentRevision uint64                             `json:"assignment_revision"`
	Partitions         map[identity.PartitionID]Partition `json:"partitions"`
}
type shapeConfig struct {
	Instances int
	Counter   uint64
}

func fixture(count int, c shapeConfig) Control {
	owner := func(i int) Owner {
		return Owner{identity.NodeID(fmt.Sprintf("nod_%022d", i+1)), identity.IncarnationID(fmt.Sprintf("inc_%022d", i+1))}
	}
	out := Control{Receipts: make(map[int]Receipt), Format: 1, Cluster: "clu_0000000000000000000001", Coordinator: Coordinator{owner(0), c.Counter, c.Counter}, AssignmentRevision: c.Counter, Partitions: make(map[identity.PartitionID]Partition, count)}
	for i := 0; i <= c.Instances; i++ {
		out.Receipts[i] = Receipt{identity.TransitionID(fmt.Sprintf("trn_%022d", 100000+i)), fmt.Sprintf("%064d", 0)}
	}
	for i := 0; i < count; i++ {
		id := identity.PartitionID(fmt.Sprintf("prt_%022d", i+1))
		desired := owner(i % c.Instances)
		prior := owner((i + 1) % c.Instances)
		out.Partitions[id] = Partition{DataPrefix: fmt.Sprintf("v2/%s/%s", out.Cluster, id), Desired: desired, Reservation: Reservation{identity.TransitionID(fmt.Sprintf("trn_%022d", i+1)), desired, c.Counter, c.Counter}, Ready: Ready{prior, c.Counter - 1, c.Counter - 1}}
	}
	return out
}
