package simulation

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"math/rand/v2"
)

//go:embed coupled_generate.go
var coupledGenerateSource []byte

// CoupledInterleavings explores delivery delays between independent actors. It
// preserves each actor's order and the order of registry/native linearizations.
// It therefore explores completion scheduling, not arbitrary storage outcomes.
// Every choice is expanded before execution; no failed schedules are resampled.
type CoupledInterleavings struct{ base Scenario }

func NewCoupledInterleavings(raw []byte) (*CoupledInterleavings, error) {
	s, err := ExpandCoupled(raw)
	if err != nil {
		return nil, err
	}
	c, err := decodeCoupled(s)
	if err != nil {
		return nil, err
	}
	if len(c.Steps) == 0 || len(c.Steps) > 256 {
		return nil, errors.New("invalid coupled interleaving bounds")
	}
	for _, step := range c.Steps {
		switch step.Action {
		case "poll", "deliver", "read", "publish", "open", "close", "fence", "commit":
		default:
			return nil, errors.New("unsupported coupled interleaving action")
		}
	}
	return &CoupledInterleavings{base: s}, nil
}

func (*CoupledInterleavings) Info() GeneratorInfo {
	return GeneratorInfo{"coupled-interleavings-v1", hash(coupledGenerateSource), []string{CoupledKind, "actor-delivery-interleavings; fixed linearization order; workload RNG unused"}}
}

func (g *CoupledInterleavings) Next(ctx context.Context, r GenerateRequest) (Scenario, error) {
	c, err := decodeCoupled(g.base)
	if err != nil {
		return Scenario{}, err
	}
	// Each step has at most two predecessors: its actor and the previous external
	// linearization. Sorted-index ready enumeration avoids map iteration entropy.
	predecessors := make([][]int, len(c.Steps))
	lastActor := map[string]int{}
	lastExternal := -1
	for i, step := range c.Steps {
		if previous, ok := lastActor[step.Actor]; ok {
			predecessors[i] = append(predecessors[i], previous)
		}
		lastActor[step.Actor] = i
		if step.Action != "poll" && step.Action != "deliver" {
			if lastExternal >= 0 {
				predecessors[i] = append(predecessors[i], lastExternal)
			}
			lastExternal = i
		}
	}
	rng := rand.New(rand.NewPCG(r.FaultSeed, r.Index))
	done := make([]bool, len(c.Steps))
	steps := make([]CoupledInput, 0, len(c.Steps))
	for len(steps) < len(c.Steps) {
		if err := ctx.Err(); err != nil {
			return Scenario{}, err
		}
		ready := []int{}
		for i := range c.Steps {
			if done[i] {
				continue
			}
			eligible := true
			for _, p := range predecessors[i] {
				if !done[p] {
					eligible = false
					break
				}
			}
			if eligible {
				ready = append(ready, i)
			}
		}
		if len(ready) == 0 {
			return Scenario{}, errors.New("cyclic coupled schedule")
		}
		selected := ready[rng.IntN(len(ready))]
		done[selected] = true
		steps = append(steps, c.Steps[selected])
	}
	out := cloneScenario(g.base)
	out.Faults, err = json.Marshal(steps)
	return out, err
}
