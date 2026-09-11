package simulation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/0x63616c/xenon/internal/cluster"
	ids "github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/registry"
)

// CoupledScenario is a bounded expanded schedule, not a random seed or a general
// actor framework. All ticks are local to the named actor incarnation. Registry
// linearization, native completion and delivery are separately scheduled actions.
// Native epochs are a model assumption; this does not qualify real SlateDB.
type CoupledScenario struct {
	Version              int               `json:"version"`
	ProductionRevision   string            `json:"production_revision"`
	Toolchain            string            `json:"toolchain"`
	Initial              cluster.Control   `json:"initial"`
	ExpectedLayoutDigest [32]byte          `json:"expected_layout_digest"`
	Actors               []CoupledActor    `json:"actors"`
	RequiredPartition    ids.PartitionID   `json:"required_partition"`
	RequiredOwner        ids.IncarnationID `json:"required_owner"`
	Steps                []CoupledInput    `json:"steps"`
}
type CoupledActor struct {
	Name        string            `json:"name"`
	Kind        string            `json:"kind"`
	Incarnation ids.IncarnationID `json:"incarnation"`
	Partition   ids.PartitionID   `json:"partition,omitempty"`
}
type CoupledInput struct {
	Action     string                  `json:"action"`
	Actor      string                  `json:"actor"`
	At         cluster.Tick            `json:"at"`
	Effect     uint64                  `json:"effect,omitempty"`
	Transition ids.TransitionID        `json:"transition,omitempty"`
	Membership *cluster.MembershipView `json:"membership,omitempty"`
	Fault      string                  `json:"fault,omitempty"` // explicit delivery fault or named negative-control cut
}
type CoupledTrace struct {
	UnresolvedPublication *cluster.Effect        `json:"unresolved_publication,omitempty"`
	Input                 CoupledInput           `json:"input"`
	ClusterEffects        []cluster.Effect       `json:"cluster_effects,omitempty"`
	PartitionEffects      []partitions.Effect    `json:"partition_effects,omitempty"`
	Before                registry.Record        `json:"before"`
	After                 registry.Record        `json:"after"`
	Expected              registry.Version       `json:"expected,omitempty"`
	Accepted              bool                   `json:"accepted,omitempty"`
	Result                string                 `json:"result,omitempty"`
	Open                  partitions.OpenRequest `json:"open"`
	Epoch                 uint64                 `json:"epoch,omitempty"`
}
type CoupledResult struct {
	Trace []CoupledTrace  `json:"trace"`
	Final registry.Record `json:"final"`
}
type coupledCompletion struct {
	record registry.Record
	err    error
	kind   string
}
type coupledWriter struct {
	open   partitions.OpenRequest
	epoch  uint64
	closed bool
}
type coupledMachine struct {
	spec      CoupledActor
	cluster   cluster.State
	partition partitions.State
	at        cluster.Tick
}

const coupledKey registry.Key = "cluster/control"
const coupledLimit = 1 << 20

// RunCoupled executes the exact expanded sequence through production Steps. A
// missing effect, double application or wrong delivery order is a replay error.
// negative is empty or one named test mutation; the same checker observes both.
func RunCoupled(scenario CoupledScenario, negative string) (CoupledResult, error) {
	return runCoupled(context.Background(), scenario, negative, nil)
}

func runCoupled(ctx context.Context, scenario CoupledScenario, negative string, observe func(CoupledTrace) error) (CoupledResult, error) {
	result := CoupledResult{}
	if scenario.Version != 1 || len(scenario.Steps) == 0 || len(scenario.Steps) > 256 || len(scenario.Actors) != 5 {
		return result, fmt.Errorf("invalid bounded coupled scenario")
	}
	if err := validateCoupledFaults(scenario.Steps); err != nil {
		return result, err
	}
	if negative != "" && negative != "stale_plan" && negative != "old_ready" && negative != "post_fence_commit" {
		return result, fmt.Errorf("unknown negative control %q", negative)
	}
	machines := map[string]*coupledMachine{}
	for _, actor := range scenario.Actors {
		if actor.Name == "" || machines[actor.Name] != nil || actor.Incarnation.Validate() != nil {
			return result, fmt.Errorf("invalid actor")
		}
		machine := &coupledMachine{spec: actor}
		var err error
		switch actor.Kind {
		case "cluster":
			machine.cluster, err = cluster.NewState(cluster.ControllerConfig{Key: coupledKey, Incarnation: actor.Incarnation, MaxControlBytes: coupledLimit, RenewalInterval: 5 * time.Nanosecond, SuspectAfter: 20 * time.Nanosecond, ExpectedLayoutDigest: scenario.ExpectedLayoutDigest})
		case "partition":
			machine.partition, err = partitions.NewState(partitions.ControllerConfig{Key: coupledKey, Incarnation: actor.Incarnation, Partition: actor.Partition, MaxControlBytes: coupledLimit, ExpectedLayoutDigest: scenario.ExpectedLayoutDigest})
		default:
			err = fmt.Errorf("unknown actor kind")
		}
		if err != nil {
			return result, err
		}
		machines[actor.Name] = machine
	}
	raw, err := json.Marshal(scenario.Initial)
	if err != nil {
		return result, err
	}
	write, err := registry.NewWrite(coupledKey, "", "trn_0000000000000000000001", raw)
	if err != nil {
		return result, err
	}
	encoded, err := registry.Encode(coupledKey, "", write)
	if err != nil {
		return result, err
	}
	current := registry.Record{Body: encoded, Version: "v1"}
	revision := 1
	if _, err := cluster.DecodeControl(coupledKey, current, coupledLimit); err != nil {
		return result, err
	}
	checker := newCoupledChecker(scenario, current)
	completions := map[string]coupledCompletion{}
	writers := map[string]*coupledWriter{}
	epochs := map[ids.PartitionID]uint64{}
	key := func(actor string, effect uint64) string { return fmt.Sprintf("%s/%d", actor, effect) }
	for index, input := range scenario.Steps {
		if err := ctx.Err(); err != nil {
			result.Final = current.Clone()
			return result, err
		}
		fail := func(err error) (CoupledResult, error) {
			result.Final = current.Clone()
			return result, fmt.Errorf("step %d %s %s/%d: %w", index, input.Action, input.Actor, input.Effect, err)
		}
		machine := machines[input.Actor]
		if machine == nil || input.At < machine.at {
			return fail(fmt.Errorf("unknown actor or regressing incarnation tick"))
		}
		machine.at = input.At
		entry := CoupledTrace{Input: input}
		slot := key(input.Actor, input.Effect)
		emitCluster := func(event cluster.Event) {
			event.Incarnation = machine.spec.Incarnation
			event.At = machine.at
			machine.cluster, entry.ClusterEffects = cluster.Step(machine.cluster, event)
		}
		emitPartition := func(event partitions.Event) {
			event.Incarnation = machine.spec.Incarnation
			machine.partition, entry.PartitionEffects = partitions.Step(machine.partition, event)
		}
		var ce cluster.Effect
		var pe partitions.Effect
		found := false
		if input.Effect != 0 {
			if machine.spec.Kind == "cluster" {
				for _, e := range machine.cluster.Pending() {
					if uint64(e.ID) == input.Effect {
						ce = e
						found = true
					}
				}
			} else {
				for _, e := range machine.partition.Pending() {
					if uint64(e.ID) == input.Effect {
						pe = e
						found = true
					}
				}
			}
		}
		switch input.Action {
		case "poll":
			if input.Effect != 0 {
				return fail(fmt.Errorf("poll contains effect"))
			}
			if machine.spec.Kind == "cluster" {
				view := cluster.MembershipView{}
				if input.Membership != nil {
					view = *input.Membership
				}
				emitCluster(cluster.Event{Kind: cluster.Poll, Membership: view})
			} else {
				emitPartition(partitions.Event{Kind: partitions.Poll})
			}
		case "read", "publish", "open", "close":
			if !found {
				return fail(fmt.Errorf("expected pending effect is absent"))
			}
			if _, exists := completions[slot]; exists {
				return fail(fmt.Errorf("effect already applied"))
			}
			completion := coupledCompletion{kind: input.Action}
			switch input.Action {
			case "read":
				if machine.spec.Kind == "cluster" && ce.Kind != cluster.ReadControl || machine.spec.Kind == "partition" && pe.Kind != partitions.ReadControl {
					return fail(fmt.Errorf("not a read effect"))
				}
				completion.record = current.Clone()
				entry.After = current.Clone()
				entry.Result = "read"
			case "publish":
				var w registry.Write
				var expected registry.Version
				if machine.spec.Kind == "cluster" {
					if ce.Kind != cluster.PublishControl {
						return fail(fmt.Errorf("not a publish effect"))
					}
					w, expected = ce.Write, ce.Expected
				} else {
					if pe.Kind != partitions.PublishControl {
						return fail(fmt.Errorf("not a publish effect"))
					}
					w, expected = pe.Write, pe.Expected
				}
				entry.Before = current.Clone()
				entry.Expected = expected
				accepted := expected == current.Version
				if input.Fault == negative && (negative == "stale_plan" || negative == "old_ready") {
					accepted = true
				}
				if accepted {
					encoded, err := registry.Encode(coupledKey, expected, w)
					if err != nil {
						return fail(err)
					}
					revision++
					current = registry.Record{Body: encoded, Version: registry.Version(fmt.Sprintf("v%d", revision))}
					completion.record = current.Clone()
					entry.Accepted = true
					entry.Result = "published"
				} else {
					completion.err = &registry.Conflict{Key: coupledKey}
					entry.Result = "conflict"
				}
				entry.After = current.Clone()
			case "open":
				if machine.spec.Kind != "partition" || pe.Kind != partitions.OpenEngine {
					return fail(fmt.Errorf("not an open effect"))
				}
				epochs[pe.Open.Partition]++
				writers[slot] = &coupledWriter{open: pe.Open, epoch: epochs[pe.Open.Partition]}
				entry.Open = pe.Open
				entry.Epoch = epochs[pe.Open.Partition]
				entry.Accepted = true
			case "close":
				if machine.spec.Kind != "partition" || pe.Kind != partitions.CloseEngine {
					return fail(fmt.Errorf("not a close effect"))
				}
				writer := writers[key(input.Actor, uint64(pe.Handle))]
				if writer == nil || writer.closed {
					return fail(fmt.Errorf("missing/already closed handle"))
				}
				writer.closed = true
				entry.Open = writer.open
				entry.Epoch = writer.epoch
				entry.Accepted = true
			}
			completions[slot] = completion
		case "deliver":
			completion, ok := completions[slot]
			if !ok || !found {
				return fail(fmt.Errorf("missing completion or pending effect"))
			}
			delete(completions, slot)
			// The operation linearized, but its response did not arrive. The
			// controller receives uncertainty, never the successful receipt.
			if input.Fault == "lost_publish_response" {
				if completion.kind != "publish" || completion.err != nil {
					return fail(fmt.Errorf("lost response requires a successful publication"))
				}
				transition := pe.Write.Transition
				if machine.spec.Kind == "cluster" {
					transition = ce.Write.Transition
				}
				completion.record = registry.Record{}
				completion.err = &registry.UnknownOutcome{Key: coupledKey, Transition: transition, Cause: errors.New("simulated lost publication response")}
			} else if input.Fault != "" {
				return fail(fmt.Errorf("unsupported delivery fault %q", input.Fault))
			}
			entry.After = completion.record.Clone()
			if completion.err != nil {
				entry.Result = "conflict"
				var unknown *registry.UnknownOutcome
				if errors.As(completion.err, &unknown) {
					entry.Result = "unknown_publication"
				}
			} else {
				entry.Result = "success"
			}
			if machine.spec.Kind == "cluster" {
				kind := cluster.ReadCompleted
				if completion.kind == "publish" {
					kind = cluster.PublishCompleted
				}
				event := cluster.Event{Kind: kind, Effect: ce.ID, Record: completion.record, Err: completion.err}
				if kind == cluster.ReadCompleted {
					event.Transition = input.Transition
				}
				emitCluster(event)
			} else {
				event := partitions.Event{Effect: pe.ID, Record: completion.record, Err: completion.err}
				switch completion.kind {
				case "read":
					event.Kind = partitions.ReadCompleted
					event.Transition = input.Transition
				case "publish":
					event.Kind = partitions.PublishCompleted
				case "open":
					event.Kind = partitions.OpenCompleted
					event.HasWriter = true
				case "close":
					event.Kind = partitions.CloseCompleted
				}
				emitPartition(event)
			}
		case "fence":
			writer := writers[slot]
			if machine.spec.Kind != "partition" || writer == nil || writer.closed || writer.epoch == epochs[writer.open.Partition] {
				return fail(fmt.Errorf("fence requires an obsolete live native handle"))
			}
			emitPartition(partitions.Event{Kind: partitions.Fenced, Handle: partitions.EffectID(input.Effect)})
		case "commit":
			writer := writers[slot]
			if writer == nil {
				return fail(fmt.Errorf("unknown commit handle"))
			}
			entry.Open = writer.open
			entry.Epoch = writer.epoch
			entry.Accepted = !writer.closed && writer.epoch == epochs[writer.open.Partition]
			if input.Fault == negative && negative == "post_fence_commit" {
				entry.Accepted = true
			}
			if entry.Accepted {
				entry.Result = "durable_model_commit"
			} else {
				entry.Result = "fenced"
			}
		default:
			return fail(fmt.Errorf("unknown action"))
		}
		if machine.spec.Kind == "cluster" && machine.cluster.LastUnknown != nil {
			effect := machine.cluster.LastUnknown.Effect
			effect.Write.Body = append([]byte(nil), effect.Write.Body...)
			entry.UnresolvedPublication = &effect
		}
		result.Trace = append(result.Trace, entry)
		if observe != nil {
			if err := observe(entry); err != nil {
				return fail(err)
			}
		}
		if err := checker.observe(entry); err != nil {
			return fail(err)
		}
	}
	result.Final = current.Clone()
	if len(completions) != 0 {
		return result, fmt.Errorf("unsettled completion queue")
	}
	for _, name := range slices.Sorted(maps.Keys(machines)) {
		m := machines[name]
		if len(m.cluster.Pending()) != 0 || len(m.partition.Pending()) != 0 {
			return result, fmt.Errorf("unsettled effects for %s", name)
		}
	}
	if err := checker.settled(); err != nil {
		return result, err
	}
	return result, nil
}

// Fault capability validation precedes all actor construction and execution.
func validateCoupledFaults(steps []CoupledInput) error {
	applications := map[string]string{}
	for _, step := range steps {
		key := fmt.Sprintf("%s/%d", step.Actor, step.Effect)
		switch step.Action {
		case "read", "publish", "open", "close":
			applications[key] = step.Action
		}
		valid := false
		switch step.Fault {
		case "":
			valid = true
		case "lost_publish_response":
			valid = step.Action == "deliver" && step.Effect != 0 && applications[key] == "publish"
		case "stale_plan", "old_ready":
			valid = step.Action == "publish"
		case "post_fence_commit":
			valid = step.Action == "commit"
		}
		if !valid {
			return fmt.Errorf("unsupported coupled fault %q on %s", step.Fault, step.Action)
		}
	}
	return nil
}
