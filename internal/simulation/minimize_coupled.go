package simulation

import (
	"context"
	"encoding/json"
	"errors"
	"io"
)

// CoupledReductionSpace removes one action and its transitive effect consumers.
// IDs, actors, membership, topology and final assertions are never rewritten.
// Production Steps validate each proposal; disappearing/renumbered dependencies
// reject the proposal rather than guessing a replacement operation identity.
// Faults counts named schedule labels, not independently injected physical faults.
type CoupledReductionSpace struct{ Limits WorkloadLimits }

func (r CoupledReductionSpace) Measure(s Scenario) (ReductionSize, error) {
	c, err := decodeCoupled(s)
	if err != nil {
		return ReductionSize{}, err
	}
	raw, err := json.Marshal(s)
	size := ReductionSize{Actions: uint64(len(c.Steps)), Bytes: uint64(len(raw))}
	for _, step := range c.Steps {
		if step.Fault != "" {
			size.Faults++
		}
	}
	return size, err
}
func (r CoupledReductionSpace) Validate(s Scenario) error {
	if len(s.Workload)+len(s.Topology)+len(s.Faults) > r.Limits.MaxPayloadBytes {
		return errors.New("coupled scenario exceeds saved payload limit")
	}
	if err := (&CoupledDriver{}).Validate(s, r.Limits); err != nil {
		return err
	}
	c, err := decodeCoupled(s)
	if err != nil {
		return err
	}
	result, err := runCoupled(context.Background(), c, "", nil)
	if len(result.Trace) != len(c.Steps) {
		return errors.New("reduction requires a complete production trace")
	}
	if _, ok := FingerprintOf(err); ok {
		return nil
	}
	return err
}
func (r CoupledReductionSpace) Candidate(ctx context.Context, s Scenario, index uint64) (Scenario, error) {
	c, err := decodeCoupled(s)
	if err != nil {
		return Scenario{}, err
	}
	if index >= uint64(len(c.Steps)) {
		return Scenario{}, io.EOF
	}
	result, err := runCoupled(ctx, c, "", nil)
	if err != nil {
		if _, ok := FingerprintOf(err); !ok {
			return Scenario{}, err
		}
	}
	if len(result.Trace) != len(c.Steps) {
		return Scenario{}, errors.New("reduction requires a complete production trace; early-assertion inputs unsupported")
	}
	type effectKey struct {
		actor string
		id    uint64
	}
	created := map[effectKey]int{}
	applied := map[effectKey]int{}
	opened := map[effectKey]int{}
	dependent := make([][]int, len(c.Steps))
	edge := func(parent, child int) {
		if parent < child {
			dependent[parent] = append(dependent[parent], child)
		}
	}
	for i, entry := range result.Trace {
		step := entry.Input
		k := effectKey{step.Actor, step.Effect}
		if step.Effect != 0 {
			if parent, ok := created[k]; ok {
				edge(parent, i)
			}
			if step.Action == "deliver" {
				if parent, ok := applied[k]; ok {
					edge(parent, i)
				}
			}
			if step.Action == "commit" || step.Action == "fence" {
				if parent, ok := opened[k]; ok {
					edge(parent, i)
				}
			}
		}
		switch step.Action {
		case "read", "publish", "open", "close":
			applied[k] = i
		}
		if step.Action == "open" {
			opened[k] = i
		}
		for _, e := range entry.ClusterEffects {
			created[effectKey{step.Actor, uint64(e.ID)}] = i
		}
		for _, e := range entry.PartitionEffects {
			created[effectKey{step.Actor, uint64(e.ID)}] = i
			if e.Handle != 0 {
				if parent, ok := opened[effectKey{step.Actor, uint64(e.Handle)}]; ok {
					edge(parent, i)
				}
			}
		}
	}
	removed := make([]bool, len(c.Steps))
	var remove func(int)
	remove = func(i int) {
		if removed[i] {
			return
		}
		removed[i] = true
		for _, child := range dependent[i] {
			remove(child)
		}
	}
	remove(int(index))
	steps := make([]CoupledInput, 0, len(c.Steps))
	for i, step := range c.Steps {
		if !removed[i] {
			steps = append(steps, step)
		}
	}
	candidate := cloneScenario(s)
	candidate.Faults, err = json.Marshal(steps)
	return candidate, err
}
