package simulation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"time"
)

// ReductionSize is ordered lexicographically; reducers never accept equal size.
type ReductionSize struct {
	Actions uint64 `json:"actions"`
	Faults  uint64 `json:"faults"`
	Bytes   uint64 `json:"bytes"`
}

func (s ReductionSize) Less(t ReductionSize) bool {
	if s.Actions != t.Actions {
		return s.Actions < t.Actions
	}
	if s.Faults != t.Faults {
		return s.Faults < t.Faults
	}
	return s.Bytes < t.Bytes
}

// ReductionSpace enumerates deterministic, dependency-repaired proposals. EOF
// exhausts the current base. Callbacks must honor context and own no resources.
type ReductionSpace interface {
	Measure(Scenario) (ReductionSize, error)
	Validate(Scenario) error
	Candidate(context.Context, Scenario, uint64) (Scenario, error)
}
type PredicateResult struct {
	Fingerprint     FailureFingerprint `json:"fingerprint"`
	TraceSHA256     string             `json:"trace_sha256"`
	CleanupVerified bool               `json:"cleanup_verified"`
	Pending         bool               `json:"pending"`
	EvidencePath    string             `json:"evidence_path,omitempty"`
}

// Evaluate includes cleanup within budget. Pending forbids all subsequent reuse.
type RunPredicate interface {
	Evaluate(context.Context, Scenario, time.Duration) (PredicateResult, error)
}
type MinimizeConfig struct {
	Clock         Clock         `json:"-"`
	MaxDuration   time.Duration `json:"max_duration"`
	AttemptBudget time.Duration `json:"attempt_budget"`
	MaxAttempts   uint64        `json:"max_attempts"`
	MaxProposals  uint64        `json:"max_proposals"`
	Simulation    bool          `json:"simulation"`
	Directory     string        `json:"directory"`
}
type ReductionAttempt struct {
	ScenarioSHA256 string          `json:"scenario_sha256"`
	Outcome        PredicateResult `json:"outcome"`
	Error          string          `json:"error,omitempty"`
}
type MinimizeResult struct {
	Version          int                `json:"version"`
	Original         Scenario           `json:"original"`
	Best             Scenario           `json:"best"`
	Fingerprint      FailureFingerprint `json:"fingerprint"`
	BestSize         ReductionSize      `json:"best_size"`
	OriginalVerified bool               `json:"original_verified"`
	Attempts         []ReductionAttempt `json:"attempts"`
	Proposals        uint64             `json:"proposals"`
	StopReason       string             `json:"stop_reason"`
	Complete         bool               `json:"complete"`
}

// Minimize greedily accepts smaller candidates only after three reproductions.
// Complete means this deterministic proposal space was exhausted, not global minimality.
func Minimize(ctx context.Context, cfg MinimizeConfig, original Scenario, target FailureFingerprint, space ReductionSpace, predicate RunPredicate) (out MinimizeResult, resultErr error) {
	out = MinimizeResult{Version: 1, Original: cloneScenario(original), Best: cloneScenario(original), Fingerprint: target, StopReason: "invalid"}
	if ctx == nil || space == nil || predicate == nil || cfg.Clock == nil || cfg.MaxDuration <= 0 || cfg.AttemptBudget <= 0 || cfg.MaxAttempts < 3 || cfg.MaxProposals == 0 || cfg.Directory == "" {
		return out, errors.New("invalid minimization configuration")
	}
	deadline := cfg.Clock.Now().Add(cfg.MaxDuration)
	if err := target.Validate(); err != nil {
		return out, err
	}
	if err := space.Validate(cloneScenario(original)); err != nil {
		return out, err
	}
	size, err := space.Measure(cloneScenario(original))
	if err != nil {
		return out, err
	}
	out.BestSize = size
	if err = makeDir(cfg.Directory); err != nil {
		return out, err
	}
	if err = save(filepath.Join(cfg.Directory, "original.json"), out); err != nil {
		return out, err
	}
	defer func() { resultErr = errors.Join(resultErr, save(filepath.Join(cfg.Directory, "result.json"), out)) }()
	if err = save(filepath.Join(cfg.Directory, "config.json"), cfg); err != nil {
		return out, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := func() error {
		if ctx.Err() != nil {
			out.StopReason = "canceled"
			return ctx.Err()
		}
		if !cfg.Clock.Now().Before(deadline) || uint64(len(out.Attempts)) >= cfg.MaxAttempts {
			out.StopReason = "budget"
			return context.DeadlineExceeded
		}
		return nil
	}
	reproduce := func(s Scenario) (bool, error) {
		raw, _ := json.Marshal(s)
		var trace string
		for repeat := 0; repeat < 3; repeat++ {
			if err := stop(); err != nil {
				return false, err
			}
			// Persist candidate before any effects; every repeat receives its own input copy.
			index := len(out.Attempts)
			if err := save(filepath.Join(cfg.Directory, fmt.Sprintf("attempt-%06d-input.json", index)), s); err != nil {
				return false, err
			}
			var evaluated PredicateResult
			budget := min(cfg.AttemptBudget, deadline.Sub(cfg.Clock.Now()))
			e, pending := bounded(runCtx, cfg.Clock, budget, func(c context.Context) error {
				var err error
				evaluated, err = predicate.Evaluate(c, cloneScenario(s), budget)
				return err
			})
			receipt := PredicateResult{Pending: true}
			if pending == nil {
				receipt = evaluated
			} else {
				e = errors.Join(e, ErrPending)
			}
			attempt := ReductionAttempt{ScenarioSHA256: hash(raw), Outcome: receipt}
			if e != nil {
				attempt.Error = e.Error()
			}
			out.Attempts = append(out.Attempts, attempt)
			if err := save(filepath.Join(cfg.Directory, fmt.Sprintf("attempt-%06d.json", index)), attempt); err != nil {
				return false, err
			}
			if receipt.Pending || errors.Is(e, ErrPending) {
				out.StopReason = "pending"
				return false, ErrPending
			}
			if !receipt.CleanupVerified {
				out.StopReason = "cleanup_failed"
				return false, errors.New("predicate cleanup not verified")
			}
			if err := stopAfterAttempt(ctx, cfg.Clock, deadline); err != nil {
				if ctx.Err() != nil {
					out.StopReason = "canceled"
				} else {
					out.StopReason = "budget"
				}
				return false, err
			}
			if e != nil || receipt.Fingerprint != target || receipt.Fingerprint.Validate() != nil {
				return false, nil
			}
			if cfg.Simulation {
				if receipt.TraceSHA256 == "" || (repeat > 0 && trace != receipt.TraceSHA256) {
					return false, nil
				}
				trace = receipt.TraceSHA256
			}
		}
		return true, nil
	}
	ok, err := reproduce(original)
	if err != nil {
		return out, err
	}
	if !ok {
		out.StopReason = "original_not_reproduced"
		return out, errors.New("original failure did not reproduce three times")
	}
	out.OriginalVerified = true
	for index := uint64(0); ; index++ {
		if err = stop(); err != nil {
			return out, err
		}
		if out.Proposals >= cfg.MaxProposals {
			out.StopReason = "budget"
			return out, context.DeadlineExceeded
		}
		var candidate Scenario
		base := cloneScenario(out.Best)
		e, pending := bounded(runCtx, cfg.Clock, deadline.Sub(cfg.Clock.Now()), func(c context.Context) error {
			var err error
			candidate, err = space.Candidate(c, base, index)
			return err
		})
		if pending != nil {
			out.StopReason = "pending"
			return out, ErrPending
		}
		out.Proposals++
		if e == io.EOF {
			out.StopReason = "exhausted"
			out.Complete = true
			return out, nil
		}
		if e != nil {
			out.StopReason = "proposal_error"
			return out, e
		}
		if err = stop(); err != nil {
			return out, err
		}
		if space.Validate(cloneScenario(candidate)) != nil {
			continue
		}
		candidateSize, e := space.Measure(cloneScenario(candidate))
		if e != nil {
			continue
		}
		if !candidateSize.Less(out.BestSize) {
			continue
		}
		ok, e = reproduce(candidate)
		if e != nil {
			return out, e
		}
		if !ok {
			continue
		}
		out.Best = cloneScenario(candidate)
		out.BestSize = candidateSize
		if err = save(filepath.Join(cfg.Directory, fmt.Sprintf("best-%06d.json", out.Proposals)), out); err != nil {
			return out, err
		}
		index = ^uint64(0) // restart deterministic enumeration from the new best
	}
}
func stopAfterAttempt(ctx context.Context, clock Clock, deadline time.Time) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if !clock.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}
