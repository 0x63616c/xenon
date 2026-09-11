package simulation

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/0x63616c/xenon/internal/cluster"
	ids "github.com/0x63616c/xenon/internal/identity"
)

func TestCoupledRenewalUnknownAfterReplacement(t *testing.T) {
	c, _ := loadCoupled(t)
	appendCycle := func(actor string, at cluster.Tick, read, publish uint64, transition string, lost bool) {
		c.Steps = append(c.Steps, CoupledInput{Action: "poll", Actor: actor, At: at}, CoupledInput{Action: "read", Actor: actor, At: at, Effect: read}, CoupledInput{Action: "deliver", Actor: actor, At: at, Effect: read, Transition: ids.TransitionID(transition)})
		if publish != 0 {
			response := CoupledInput{Action: "deliver", Actor: actor, At: at, Effect: publish}
			if lost {
				response.Fault = "lost_publish_response"
			}
			c.Steps = append(c.Steps, CoupledInput{Action: "publish", Actor: actor, At: at, Effect: publish}, response)
		}
	}
	appendCycle("coordinator-new", 26, 6, 7, "trn_0000000000000000000601", true)
	appendCycle("coordinator-old", 3, 7, 0, "trn_0000000000000000000602", false)
	appendCycle("coordinator-old", 23, 8, 9, "trn_0000000000000000000603", false)
	appendCycle("coordinator-new", 27, 8, 0, "trn_0000000000000000000604", false)
	r, err := RunCoupled(c, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := checkReplacedRenewal(r); err != nil {
		t.Fatal(err)
	}
	replayCoupledReplacement(t, c, r)
	changed := r
	changed.Trace = append([]CoupledTrace(nil), r.Trace...)
	changed.Trace[len(changed.Trace)-1].UnresolvedPublication = nil
	if fp, ok := FingerprintOf(checkReplacedRenewal(changed)); !ok || fp.Invariant != "ambiguous_renewal" {
		t.Fatal("missing ambiguity history mutant escaped named invariant")
	}

}

func assignmentABAScenario(t *testing.T) CoupledScenario {
	t.Helper()
	base, _ := loadCoupled(t)
	c, err := assignmentABADSTScenario(base)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestCoupledAssignmentABARereservesWriter(t *testing.T) {
	c := assignmentABAScenario(t)
	r, err := RunCoupled(c, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := checkAssignmentABA(r, c.RequiredPartition); err != nil {
		t.Fatal(err)
	}
	replayCoupledReplacement(t, c, r)
	// Exercise a real attempted commit through the displaced original handle,
	// then make the modeled engine incorrectly accept it. Earlier mutation labels
	// are removed so the negative control must fail at this new ABA boundary.
	oldCommit := c
	oldCommit.Steps = append([]CoupledInput(nil), c.Steps...)
	for i := range oldCommit.Steps {
		if oldCommit.Steps[i].Fault == "post_fence_commit" {
			oldCommit.Steps[i].Fault = ""
		}
	}
	final := oldCommit.Steps[len(oldCommit.Steps)-1]
	oldCommit.Steps = append(oldCommit.Steps[:len(oldCommit.Steps)-1], CoupledInput{Action: "commit", Actor: "writer-current", Effect: 9, Fault: "post_fence_commit"}, final)
	if _, err := RunCoupled(oldCommit, ""); err != nil {
		t.Fatal(err)
	}
	violation, err := RunCoupled(oldCommit, "post_fence_commit")
	fp, ok := FingerprintOf(err)
	if !ok || fp.Invariant != "post_fence_commit" || len(violation.Trace) != len(oldCommit.Steps)-1 {
		t.Fatalf("ABA commit mutant failed at wrong boundary: %v", err)
	}
	for _, mutant := range []string{"omit_close", "reuse_revision"} {
		changed := r
		changed.Trace = append([]CoupledTrace(nil), r.Trace...)
		for i := range changed.Trace {
			e := &changed.Trace[i]
			if mutant == "omit_close" && e.Input.Actor == "writer-current" && e.Input.Action == "close" && e.Input.Effect == 13 {
				e.Input.Action = "read"
			}
			if mutant == "reuse_revision" && e.Input.Actor == "writer-current" && e.Input.Action == "open" && e.Input.Effect == 16 {
				e.Open.AssignmentRevision = 0
			}
		}
		if fp, ok := FingerprintOf(checkAssignmentABA(changed, c.RequiredPartition)); !ok || fp.Invariant != "assignment_aba" {
			t.Fatalf("%s escaped named invariant", mutant)
		}
	}

}

func replayCoupledReplacement(t *testing.T, c CoupledScenario, first CoupledResult) {
	t.Helper()
	second, err := RunCoupled(c, "")
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if !bytes.Equal(a, b) {
		t.Fatal("expanded replacement trace changed")
	}
}

// These oracles inspect only observed publications/handles, not production
// election, placement, reservation or authority eligibility helpers.
func checkReplacedRenewal(r CoupledResult) error {
	var attempted CoupledTrace
	lost := 0
	for _, e := range r.Trace {
		if e.Input.Actor == "coordinator-new" && e.Input.Action == "publish" && e.Input.Effect == 7 {
			attempted = e
		}
		if e.Result == "unknown_publication" {
			lost++
		}
	}
	last := r.Trace[len(r.Trace)-1]
	if !attempted.Accepted || lost != 1 || last.Input.Actor != "coordinator-new" || len(last.ClusterEffects) != 0 || last.UnresolvedPublication == nil {
		return checkerFailure("ambiguous_renewal", "missing_history_or_invalid_admission", "ambiguous renewal history or admission incorrect")
	}
	observed, err := checkerControl(last.After)
	if err != nil {
		return err
	}
	proposed, err := checkerControl(attempted.After)
	if err != nil {
		return err
	}
	history := last.UnresolvedPublication
	proposedBytes, _ := json.Marshal(proposed)
	if observed.Coordinator.Incarnation == proposed.Coordinator.Incarnation || observed.Coordinator.Generation != proposed.Coordinator.Generation+1 || history.Expected != attempted.Expected || history.Write.Transition != "trn_0000000000000000000601" || !bytes.Equal(history.Write.Body, proposedBytes) {
		return checkerFailure("ambiguous_renewal", "rewritten_history", "replacement erased or rewrote original ambiguity")
	}
	return nil
}

func checkAssignmentABA(r CoupledResult, partition ids.PartitionID) error {
	var before cluster.PartitionControl
	var after cluster.PartitionControl
	moves, closed, opened := 0, 0, 0
	for _, e := range r.Trace {
		if e.Input.Actor == "coordinator-new" && e.Input.Action == "publish" && (e.Input.Effect == 7 || e.Input.Effect == 9) {
			a, err := checkerControl(e.Before)
			if err != nil {
				return err
			}
			b, err := checkerControl(e.After)
			if err != nil {
				return err
			}
			if !e.Accepted || a.Partitions[partition].Desired == b.Partitions[partition].Desired {
				return checkerFailure("assignment_aba", "missing_move", "assignment move did not occur")
			}
			if moves == 0 {
				before = a.Partitions[partition]
			}
			after = b.Partitions[partition]
			moves++
		}
		if e.Input.Actor == "writer-current" && e.Input.Action == "close" && e.Input.Effect == 13 {
			closed++
		}
		if e.Input.Actor == "writer-current" && e.Input.Action == "open" && e.Input.Effect == 16 {
			if closed != 1 || e.Open.AssignmentRevision != after.AssignmentRevision || e.Open.Generation != before.Generation+1 || e.Open.Reservation == before.Reservation {
				return checkerFailure("assignment_aba", "reused_authority", "ABA reused old writer authority")
			}
			opened++
		}
	}
	if moves != 2 || before.Desired != after.Desired || after.AssignmentRevision <= before.AssignmentRevision || closed != 1 || opened != 1 {
		return checkerFailure("assignment_aba", "missing_reacquisition", "missing full ABA and writer reacquisition")
	}
	return nil
}
