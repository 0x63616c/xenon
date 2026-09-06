package simulation

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/0x63616c/xenon/internal/cluster"
	ids "github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
)

// This observer uses only published records and native observations. It never
// calls production eligibility, election, placement, validation or ready helpers.
// JSON structs are shared vocabulary; every invariant below is independent.
type coupledChecker struct {
	scenario      CoupledScenario
	record        registry.Record
	actors        map[string]CoupledActor
	epochs        map[ids.PartitionID]uint64
	handles       map[string]CoupledTrace
	closed        map[string]bool
	commits       int
	rejectedPlans int
	rejectedReady int
}

func newCoupledChecker(s CoupledScenario, initial registry.Record) *coupledChecker {
	c := &coupledChecker{scenario: s, record: initial.Clone(), actors: map[string]CoupledActor{}, epochs: map[ids.PartitionID]uint64{}, handles: map[string]CoupledTrace{}, closed: map[string]bool{}}
	for _, actor := range s.Actors {
		c.actors[actor.Name] = actor
	}
	return c
}
func checkerControl(r registry.Record) (cluster.Control, error) {
	var envelope struct {
		Body []byte `json:"body"`
	}
	var c cluster.Control
	if err := json.Unmarshal(r.Body, &envelope); err != nil {
		return c, err
	}
	if err := json.Unmarshal(envelope.Body, &c); err != nil {
		return c, err
	}
	return c, nil
}
func (c *coupledChecker) observe(e CoupledTrace) error {
	key := fmt.Sprintf("%s/%d", e.Input.Actor, e.Input.Effect)
	switch e.Input.Action {
	case "open":
		if e.Epoch != c.epochs[e.Open.Partition]+1 {
			return fmt.Errorf("native_epoch: nonmonotonic native open")
		}
		c.epochs[e.Open.Partition] = e.Epoch
		c.handles[key] = e
	case "close":
		found := false
		for id, opened := range c.handles {
			if opened.Input.Actor == e.Input.Actor && opened.Epoch == e.Epoch && opened.Open == e.Open {
				c.closed[id] = true
				found = true
			}
		}
		if !found {
			return fmt.Errorf("native_close: unknown native handle")
		}
	case "commit":
		opened, exists := c.handles[key]
		if e.Accepted && (!exists || c.closed[key] || e.Epoch != c.epochs[e.Open.Partition] || opened.Open != e.Open) {
			return fmt.Errorf("post_fence_commit: obsolete native epoch committed")
		}
		if e.Accepted {
			c.commits++
		}
	case "publish":
		if e.Before.Version != c.record.Version || !bytes.Equal(e.Before.Body, c.record.Body) {
			return fmt.Errorf("registry_order: incorrect linearization predecessor")
		}
		if !e.Accepted {
			if !bytes.Equal(e.Before.Body, e.After.Body) || e.Before.Version != e.After.Version {
				return fmt.Errorf("registry_conflict: failed CAS changed record")
			}
			if e.Input.Fault == "stale_plan" {
				c.rejectedPlans++
			}
			if e.Input.Fault == "old_ready" {
				c.rejectedReady++
			}
			return nil
		}
		before, err := checkerControl(c.record)
		if err != nil {
			return err
		}
		after, err := checkerControl(e.After)
		if err != nil {
			return err
		}
		actor := c.actors[e.Input.Actor]
		if len(before.Partitions) != len(after.Partitions) {
			return fmt.Errorf("layout: database set changed")
		}
		changed := 0
		for id, old := range before.Partitions {
			next, exists := after.Partitions[id]
			if !exists || next.Path != old.Path {
				return fmt.Errorf("layout: stable database path changed")
			}
			// Check ready authority before generic CAS, so the stale-ready negative
			// control identifies the precise safety boundary it violates.
			if next.Ready && next != old {
				if actor.Kind != "partition" || actor.Incarnation != old.Desired.Incarnation || next.Desired != old.Desired || next.AssignmentRevision != old.AssignmentRevision || next.Reservation != old.Reservation || next.Generation != old.Generation {
					return fmt.Errorf("old_ready: obsolete assignment published ready")
				}
				live := false
				for handle, opened := range c.handles {
					a := opened.Open
					if !c.closed[handle] && opened.Input.Actor == e.Input.Actor && opened.Epoch == c.epochs[id] && a.Partition == id && a.Incarnation == actor.Incarnation && a.AssignmentRevision == next.AssignmentRevision && a.Reservation == next.Reservation && a.Generation == next.Generation {
						live = true
					}
				}
				if !live {
					return fmt.Errorf("old_ready: readiness lacks a current native open")
				}
			}
			if next.Desired != old.Desired {
				changed++
				if before.ActiveMove != "" && before.ActiveMove != id {
					return fmt.Errorf("move_budget: changed a second physical database")
				}
				if after.ActiveMove != id || next.Ready || next.AssignmentRevision <= old.AssignmentRevision || actor.Kind != "cluster" || actor.Incarnation != before.Coordinator.Incarnation {
					return fmt.Errorf("stale_plan: assignment lacks current coordinator authority")
				}
			}
		}
		if changed > 1 {
			return fmt.Errorf("move_budget: multiple physical moves")
		}
		if e.Expected != c.record.Version {
			return fmt.Errorf("stale_plan: publication ignored its original CAS condition")
		}
		if before.ActiveMove != "" && after.ActiveMove == "" && !after.Partitions[before.ActiveMove].Ready {
			return fmt.Errorf("move_budget: cleared active move without ready publication")
		}
		if after.ActiveMove != "" {
			p, exists := after.Partitions[after.ActiveMove]
			if !exists || p.Ready {
				return fmt.Errorf("move_budget: invalid persisted active move")
			}
		}
		if before.Coordinator != after.Coordinator {
			if actor.Kind != "cluster" || after.Coordinator.Incarnation != actor.Incarnation {
				return fmt.Errorf("coordinator: publication lacks actor authority")
			}
			if before.Coordinator.Incarnation == after.Coordinator.Incarnation {
				if after.Coordinator.Generation != before.Coordinator.Generation || after.Coordinator.Renewal != before.Coordinator.Renewal+1 {
					return fmt.Errorf("coordinator: invalid renewal")
				}
			} else if after.Coordinator.Generation != before.Coordinator.Generation+1 || after.Coordinator.Renewal != 0 {
				return fmt.Errorf("coordinator: invalid takeover")
			}
		}
		c.record = e.After.Clone()
	}
	return nil
}
func (c *coupledChecker) settled() error {
	final, err := checkerControl(c.record)
	if err != nil {
		return err
	}
	p := final.Partitions[c.scenario.RequiredPartition]
	if !p.Ready || p.Desired.Incarnation != c.scenario.RequiredOwner || final.ActiveMove != "" || c.commits == 0 {
		return fmt.Errorf("progress: required owner did not settle ready and commit within schedule")
	}
	for key, opened := range c.handles {
		if opened.Epoch != c.epochs[opened.Open.Partition] && !c.closed[key] {
			return fmt.Errorf("progress: obsolete native handle did not retire")
		}
	}
	if c.rejectedPlans != 1 || c.rejectedReady != 1 {
		return fmt.Errorf("coverage: missing delayed stale plan/ready conflicts")
	}
	return nil
}
