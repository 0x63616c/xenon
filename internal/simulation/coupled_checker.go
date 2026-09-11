package simulation

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"

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
	commits       map[string]CoupledTrace
	rejectedPlans int
	rejectedReady int
}

func newCoupledChecker(s CoupledScenario, initial registry.Record) *coupledChecker {
	c := &coupledChecker{scenario: s, record: initial.Clone(), actors: map[string]CoupledActor{}, epochs: map[ids.PartitionID]uint64{}, handles: map[string]CoupledTrace{}, closed: map[string]bool{}, commits: map[string]CoupledTrace{}}
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
	case "read":
		if e.Input.Fault == "storage_read_error" && (e.Result != "storage_read_error" || e.Before.Version != c.record.Version || !bytes.Equal(e.Before.Body, c.record.Body) || e.After.Version != e.Before.Version || !bytes.Equal(e.After.Body, e.Before.Body)) {
			return checkerFailure("registry_read", "failed_read_mutated_authority", "failed registry read changed authority")
		}
	case "open":
		if e.Epoch != c.epochs[e.Open.Partition]+1 {
			return checkerFailure("native_epoch", "nonmonotonic_open", "nonmonotonic native open")
		}
		c.epochs[e.Open.Partition] = e.Epoch
		c.handles[key] = e
	case "close":
		found := false
		for _, id := range slices.Sorted(maps.Keys(c.handles)) {
			opened := c.handles[id]
			if opened.Input.Actor == e.Input.Actor && opened.Epoch == e.Epoch && opened.Open == e.Open {
				c.closed[id] = true
				found = true
			}
		}
		if !found {
			return checkerFailure("native_close", "unknown_handle", "unknown native handle")
		}
	case "commit":
		opened, exists := c.handles[key]
		if e.Accepted && (!exists || c.closed[key] || e.Epoch != c.epochs[e.Open.Partition] || opened.Open != e.Open) {
			return checkerFailure("post_fence_commit", "obsolete_epoch", "obsolete native epoch committed")
		}
		if e.Accepted {
			c.commits[key] = e
		}
	case "publish":
		if e.Before.Version != c.record.Version || !bytes.Equal(e.Before.Body, c.record.Body) {
			return checkerFailure("registry_order", "wrong_predecessor", "incorrect linearization predecessor")
		}
		if !e.Accepted {
			if !bytes.Equal(e.Before.Body, e.After.Body) || e.Before.Version != e.After.Version {
				return checkerFailure("registry_conflict", "rejected_cas_mutated", "failed CAS changed record")
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
		if !reflect.DeepEqual(before.Layout, after.Layout) || before.Format != after.Format || before.Cluster != after.Cluster {
			return checkerFailure("layout", "configuration_changed", "immutable configuration changed")
		}
		if len(before.Partitions) != len(after.Partitions) {
			return checkerFailure("layout", "database_set_changed", "database set changed")
		}
		changed := 0
		for _, id := range slices.Sorted(maps.Keys(before.Partitions)) {
			old := before.Partitions[id]
			next, exists := after.Partitions[id]
			if !exists || next.Path != old.Path {
				return checkerFailure("layout", "database_path_changed", "stable database path changed")
			}
			if e.Expected == c.record.Version && next.Desired == old.Desired && (next.Generation != old.Generation || next.Reservation != old.Reservation) {
				// A reservation is an owner-only mutation for this exact partition.
				// It advances one generation and preserves coordinator/assignment
				// fields and the persisted move budget. No production Reserve or
				// eligibility helper participates in this independent check.
				if actor.Kind != "partition" || actor.Partition != id || actor.Incarnation != old.Desired.Incarnation || next.AssignmentRevision != old.AssignmentRevision || next.Generation <= old.Generation || next.Generation-old.Generation != 1 || next.Reservation == "" || next.Reservation == old.Reservation || next.Ready || after.Cluster != before.Cluster || after.Format != before.Format || after.Coordinator != before.Coordinator || after.AssignmentRevision != before.AssignmentRevision || after.ActiveMove != before.ActiveMove {
					return checkerFailure("reservation_authority", "invalid_reservation", "invalid owner reservation mutation")
				}
			}
			if e.Expected == c.record.Version && next.Desired == old.Desired && next.AssignmentRevision != old.AssignmentRevision {
				return checkerFailure("field_separation", "owner_changed_assignment", "owner changed assignment revision")
			}
			if e.Expected == c.record.Version && old.Ready && !next.Ready && next.Desired == old.Desired && next.Generation == old.Generation && next.Reservation == old.Reservation {
				return checkerFailure("field_separation", "ready_cleared_without_authority", "readiness cleared without a reservation or assignment")
			}
			// Check ready authority before generic CAS, so the stale-ready negative
			// control identifies the precise safety boundary it violates.
			if next.Ready && next != old {
				if actor.Kind != "partition" || actor.Partition != id || actor.Incarnation != old.Desired.Incarnation || next.Desired != old.Desired || next.AssignmentRevision != old.AssignmentRevision || next.Reservation != old.Reservation || next.Generation != old.Generation {
					return checkerFailure("old_ready", "obsolete_assignment", "obsolete assignment published ready")
				}
				live := false
				for _, handle := range slices.Sorted(maps.Keys(c.handles)) {
					opened := c.handles[handle]
					a := opened.Open
					if !c.closed[handle] && opened.Input.Actor == e.Input.Actor && opened.Epoch == c.epochs[id] && a.Partition == id && a.Incarnation == actor.Incarnation && a.AssignmentRevision == next.AssignmentRevision && a.Reservation == next.Reservation && a.Generation == next.Generation {
						live = true
					}
				}
				if !live {
					return checkerFailure("old_ready", "missing_current_open", "readiness lacks a current native open")
				}
			}
			if next.Desired != old.Desired {
				changed++
				if before.ActiveMove != "" && before.ActiveMove != id {
					return checkerFailure("move_budget", "second_database", "changed a second physical database")
				}
				if after.ActiveMove != id || next.Ready || next.Generation != old.Generation || next.Reservation != "" || next.AssignmentRevision <= old.AssignmentRevision || actor.Kind != "cluster" || actor.Incarnation != before.Coordinator.Incarnation {
					return checkerFailure("stale_plan", "missing_coordinator_authority", "assignment lacks current coordinator authority")
				}
			}
		}
		if changed > 1 {
			return checkerFailure("move_budget", "multiple_moves", "multiple physical moves")
		}
		if e.Expected != c.record.Version {
			return checkerFailure("stale_plan", "ignored_cas_condition", "publication ignored its original CAS condition")
		}
		if before.ActiveMove != "" && after.ActiveMove == "" && !after.Partitions[before.ActiveMove].Ready {
			return checkerFailure("move_budget", "cleared_before_ready", "cleared active move without ready publication")
		}
		if after.ActiveMove != "" {
			p, exists := after.Partitions[after.ActiveMove]
			if !exists || p.Ready {
				return checkerFailure("move_budget", "invalid_active_move", "invalid persisted active move")
			}
		}
		if before.Coordinator != after.Coordinator {
			if actor.Kind != "cluster" || after.Coordinator.Incarnation != actor.Incarnation {
				return checkerFailure("coordinator", "wrong_actor", "publication lacks actor authority")
			}
			if before.Coordinator.Incarnation == after.Coordinator.Incarnation {
				if after.Coordinator.Generation != before.Coordinator.Generation || after.Coordinator.Renewal != before.Coordinator.Renewal+1 {
					return checkerFailure("coordinator", "invalid_renewal", "invalid renewal")
				}
			} else if after.Coordinator.Generation != before.Coordinator.Generation+1 || after.Coordinator.Renewal != 0 {
				return checkerFailure("coordinator", "invalid_takeover", "invalid takeover")
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
	if !p.Ready || p.Desired.Incarnation != c.scenario.RequiredOwner || final.ActiveMove != "" {
		return checkerFailure("progress", "owner_not_ready", "required owner did not settle ready and commit within schedule")
	}
	progress := false
	for _, handle := range slices.Sorted(maps.Keys(c.commits)) {
		commit := c.commits[handle]
		a := commit.Open
		if !c.closed[handle] && commit.Epoch == c.epochs[c.scenario.RequiredPartition] && a.Partition == c.scenario.RequiredPartition && a.Incarnation == c.scenario.RequiredOwner && a.AssignmentRevision == p.AssignmentRevision && a.Reservation == p.Reservation && a.Generation == p.Generation {
			progress = true
		}
	}
	if !progress {
		return checkerFailure("progress", "missing_live_commit", "required final live owner has no successful commit in its current reservation and epoch")
	}
	for _, key := range slices.Sorted(maps.Keys(c.handles)) {
		opened := c.handles[key]
		if opened.Epoch != c.epochs[opened.Open.Partition] && !c.closed[key] {
			return checkerFailure("progress", "obsolete_handle_not_retired", "obsolete native handle did not retire")
		}
	}
	if c.rejectedPlans != 1 || c.rejectedReady != 1 {
		return checkerFailure("coverage", "missing_stale_conflicts", "missing delayed stale plan/ready conflicts")
	}
	return nil
}

// Each assertion supplies a stable mechanism explicitly; diagnostics never choose
// reduction identity. Invalid constant names fail closed as ordinary errors.
func checkerFailure(invariant, mechanism, diagnostic string) error {
	failure, err := NewInvariantFailure(FailureFingerprint{Invariant: invariant, Mechanism: mechanism}, fmt.Errorf("%s", diagnostic))
	if err != nil {
		return err
	}
	return failure
}
