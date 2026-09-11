package simulation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// RunnerPredicate uses the same validation, trace latch, settle and cleanup path
// as search/replay. Construct one per serial minimization; never reuse after pending.
type RunnerPredicate struct {
	Runner  Runner
	Config  SearchConfig
	next    uint64
	pending bool
}

func (p *RunnerPredicate) Evaluate(ctx context.Context, s Scenario, budget time.Duration) (out PredicateResult, resultErr error) {
	if p.pending {
		out.Pending = true
		return out, ErrPending
	}
	cfg := p.Config
	cfg.MaxCases = 1
	cfg.Continuous = false
	if budget <= cfg.CleanupBudget {
		return out, errors.New("attempt budget must exceed cleanup budget")
	}
	cfg.MaxDuration = budget - cfg.CleanupBudget
	if err := validateRunner(cfg, &p.Runner); err != nil {
		return out, err
	}
	start := p.Runner.Clock.Now()
	dir := filepath.Join(p.Runner.Directory, fmt.Sprintf("attempt-%06d", p.next))
	p.next++
	if err := makeDir(dir); err != nil {
		return out, err
	}
	out.EvidencePath = dir
	raw, _ := json.Marshal(s)
	request := GenerateRequest{Index: 0, WorkloadSeed: streamSeed("workload/v1", cfg.WorkloadSeed, 0), FaultSeed: streamSeed("fault/v1", cfg.FaultSeed, 0), Limits: cfg.Limits}
	record := artifact{Version: 2, Config: cfg, Request: request, Generator: GeneratorInfo{Version: "minimize-expanded-v1", SHA256: hash(raw), Capabilities: []string{s.Kind}}, Provenance: p.Runner.Provenance, Scenario: cloneScenario(s), SHA256: hash(raw)}
	if err := saveArtifact(filepath.Join(dir, "scenario.json"), record); err != nil {
		return out, err
	}
	detail, err := p.Runner.runCase(ctx, cfg, cloneScenario(s), dir, start.Add(cfg.MaxDuration))
	out.Pending = errors.Is(err, ErrPending)
	p.pending = out.Pending
	out.CleanupVerified = detail.Cleaned && !out.Pending && len(detail.Secondary) == 0
	saveErr := save(filepath.Join(dir, "result.json"), detail)
	trace, readErr := os.ReadFile(filepath.Join(dir, "trace.jsonl"))
	if readErr == nil {
		out.TraceSHA256 = hash(trace)
	}
	if detail.FailureFingerprint != nil && out.CleanupVerified && saveErr == nil && readErr == nil {
		out.Fingerprint = *detail.FailureFingerprint
		return out, nil
	}
	return out, errors.Join(err, saveErr, readErr)
}
