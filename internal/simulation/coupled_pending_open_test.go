package simulation

import (
	"github.com/0x63616c/xenon/internal/cluster"
	ids "github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/partitions"
	"testing"
)

func TestCoupledPendingOpenAssignmentABA(t *testing.T) {
	c := assignmentABAScenario(t)
	// Stop immediately after the reservation response emitted Open16. The native
	// completion is withheld across another complete A->B->A assignment cycle.
	cut := 0
	for i, s := range c.Steps {
		if s.Actor == "writer-current" && s.Action == "open" && s.Effect == 16 {
			cut = i
			break
		}
	}
	if cut == 0 {
		t.Fatal("missing pending Open boundary")
	}
	c.Steps = c.Steps[:cut]
	emit := func(action, actor string, at cluster.Tick, effect uint64, transition ids.TransitionID) {
		c.Steps = append(c.Steps, CoupledInput{Action: action, Actor: actor, At: at, Effect: effect, Transition: transition})
	}
	for i, memberIndex := range []int{5, 24} {
		at := cluster.Tick(24 + i)
		read := uint64(10 + i*2)
		publish := read + 1
		view := *c.Steps[memberIndex].Membership
		view.Coordinator = c.Steps[35].Membership.Coordinator
		view.Generation = 2
		c.Steps = append(c.Steps, CoupledInput{Action: "poll", Actor: "coordinator-new", At: at, Membership: &view})
		emit("read", "coordinator-new", at, read, "")
		transition := ids.TransitionID("trn_0000000000000000000801")
		if i == 1 {
			transition = "trn_0000000000000000000802"
		}
		emit("deliver", "coordinator-new", at, read, transition)
		emit("publish", "coordinator-new", at, publish, "")
		emit("deliver", "coordinator-new", at, publish, "")
	}
	// At tick25 the renewal interval takes precedence. A fresh tick26 read
	// receives a new placement opportunity and performs the return assignment.
	view := *c.Steps[35].Membership
	c.Steps = append(c.Steps, CoupledInput{Action: "poll", Actor: "coordinator-new", At: 26, Membership: &view})
	emit("read", "coordinator-new", 26, 14, "")
	emit("deliver", "coordinator-new", 26, 14, "trn_0000000000000000000806")
	emit("publish", "coordinator-new", 26, 15, "")
	emit("deliver", "coordinator-new", 26, 15, "")
	emit("poll", "writer-current", 0, 0, "")
	emit("read", "writer-current", 0, 17, "")
	emit("deliver", "writer-current", 0, 17, "trn_0000000000000000000803")
	emit("open", "writer-current", 0, 16, "")
	emit("deliver", "writer-current", 0, 16, "")
	emit("close", "writer-current", 0, 18, "")
	emit("deliver", "writer-current", 0, 18, "")
	emit("read", "writer-current", 0, 19, "")
	emit("deliver", "writer-current", 0, 19, "trn_0000000000000000000804")
	emit("publish", "writer-current", 0, 20, "")
	emit("deliver", "writer-current", 0, 20, "")
	emit("open", "writer-current", 0, 21, "")
	emit("deliver", "writer-current", 0, 21, "")
	emit("read", "writer-current", 0, 22, "")
	emit("deliver", "writer-current", 0, 22, "trn_0000000000000000000805")
	emit("publish", "writer-current", 0, 23, "")
	emit("deliver", "writer-current", 0, 23, "")
	emit("commit", "writer-current", 0, 21, "")
	result, err := RunCoupled(c, "")
	if err != nil {
		t.Fatal(err)
	}
	replayCoupledReplacement(t, c, result)
	checkPendingOpen(t, result)
	displaced := displaceDuringPendingOpen(c)
	observed, err := RunCoupled(displaced, "")
	if err != nil {
		t.Fatal(err)
	}
	replayCoupledReplacement(t, displaced, observed)
	checkPendingOpen(t, observed)
	denied := 0
	for _, e := range observed.Trace {
		if e.Input.Actor == "writer-current" && e.Input.Action == "commit" && e.Input.Effect == 16 && e.Result == "fenced" {
			denied++
		}
	}
	if denied != 1 {
		t.Fatal("pending Open was not displaced and fenced")
	}
	for i := range displaced.Steps {
		if displaced.Steps[i].Fault == "post_fence_commit" && displaced.Steps[i].Effect != 16 {
			displaced.Steps[i].Fault = ""
		}
	}
	violation, err := RunCoupled(displaced, "post_fence_commit")
	fp, ok := FingerprintOf(err)
	if !ok || fp.Invariant != "post_fence_commit" || violation.Trace[len(violation.Trace)-1].Input.Effect != 16 {
		t.Fatalf("pending Open mutant wrong failure: %v", err)
	}
}

func checkPendingOpen(t *testing.T, r CoupledResult) {
	t.Helper()
	observed, retired, moves := 0, 0, 0
	var pending partitions.OpenRequest
	for _, e := range r.Trace {
		if pending.Partition != "" && retired == 0 && e.Input.Action == "publish" && e.Accepted {
			before, err := checkerControl(e.Before)
			if err != nil {
				t.Fatal(err)
			}
			after, err := checkerControl(e.After)
			if err != nil {
				t.Fatal(err)
			}
			if before.Partitions[pending.Partition].Desired != after.Partitions[pending.Partition].Desired {
				moves++
			}
		}
		for _, effect := range e.PartitionEffects {
			if e.Input.Actor == "writer-current" && effect.Kind == partitions.OpenEngine && effect.ID == 16 {
				pending = effect.Open
			}
		}

		if e.Input.Actor != "writer-current" {
			continue
		}
		if e.Input.Action == "deliver" && e.Input.Effect == 17 {
			control, err := checkerControl(e.After)
			if err != nil {
				t.Fatal(err)
			}
			current := control.Partitions[pending.Partition]
			if current.Desired.Incarnation != pending.Incarnation || current.AssignmentRevision <= pending.AssignmentRevision {
				t.Fatal("no actual assignment ABA while Open pending")
			}
			if len(e.PartitionEffects) != 0 {
				t.Fatal("pending Open exclusion released before completion")
			}
			observed++
		}
		if e.Input.Action == "deliver" && e.Input.Effect == 16 {
			if len(e.PartitionEffects) != 1 || e.PartitionEffects[0].Kind != partitions.CloseEngine || e.PartitionEffects[0].Handle != 16 {
				t.Fatal("late obsolete Open was not immediately retired")
			}
			retired++
		}
	}
	if observed != 1 || retired != 1 || moves != 2 {
		t.Fatal("pending Open ABA cut missing")
	}
}

func displaceDuringPendingOpen(c CoupledScenario) CoupledScenario {
	out := c
	out.Steps = nil
	add := func(action, actor string, effect uint64, transition ids.TransitionID) {
		out.Steps = append(out.Steps, CoupledInput{Action: action, Actor: actor, Effect: effect, Transition: transition})
	}
	for _, s := range c.Steps {
		if s.Actor == "coordinator-new" && s.Action == "poll" && s.At == 24 {
			add("open", "writer-current", 16, "")
		}
		if s.Actor == "writer-current" && s.Action == "open" && s.Effect == 16 {
			continue
		}
		out.Steps = append(out.Steps, s)
		if s.Actor == "coordinator-new" && s.Action == "deliver" && s.Effect == 11 {
			add("poll", "writer-target", 0, "")
			add("read", "writer-target", 9, "")
			add("deliver", "writer-target", 9, "trn_0000000000000000000901")
			add("publish", "writer-target", 10, "")
			add("deliver", "writer-target", 10, "")
			add("open", "writer-target", 11, "")
			add("deliver", "writer-target", 11, "")
			out.Steps = append(out.Steps, CoupledInput{Action: "commit", Actor: "writer-current", Effect: 16, Fault: "post_fence_commit"})
		}
	}
	add("read", "writer-target", 12, "")
	add("deliver", "writer-target", 12, "trn_0000000000000000000902")
	add("close", "writer-target", 13, "")
	add("deliver", "writer-target", 13, "")
	add("read", "writer-target", 14, "")
	add("deliver", "writer-target", 14, "trn_0000000000000000000903")
	return out
}
