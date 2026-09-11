package simulation

import (
	"context"
	"errors"
	"time"
)

// RunDST explores code-authored schedules in memory. Disk is touched only when
// retaining the first reproducible failure, keeping the ordinary loop fast.
func RunDST(ctx context.Context, seed, cases uint64, evidence string, provenance Provenance, clock Clock) (RunResult, error) {
	result := RunResult{StopReason: "completed"}
	if ctx == nil || cases == 0 || evidence == "" || clock == nil {
		return result, errors.New("invalid DST run")
	}
	generator, err := DefaultDSTGenerator()
	if err != nil {
		return result, err
	}
	limits := WorkloadLimits{MaxOperations: 256, MaxDepth: 1, MaxPayloadBytes: 1 << 20, Features: []string{CoupledKind}}
	for index := uint64(0); index < cases; index++ {
		if err = ctx.Err(); err != nil {
			result.StopReason = "canceled"
			return result, err
		}
		request := GenerateRequest{Index: index, WorkloadSeed: streamSeed("workload/v1", seed, index), FaultSeed: streamSeed("fault/v1", seed, index), Limits: limits}
		scenario, generateErr := generator.Next(ctx, request)
		if generateErr == nil {
			var coupled CoupledScenario
			coupled, generateErr = decodeCoupled(scenario)
			if generateErr == nil {
				_, generateErr = runCoupled(ctx, coupled, "", nil)
			}
		}
		if generateErr == nil {
			result.Completed++
			continue
		}
		result.StopReason = "first_failure"
		// Re-run the exact expanded schedule through the evidence runner so replay
		// and minimization receive the same durable artifact format as other modes.
		cfg := SearchConfig{MaxCases: 1, MaxDuration: time.Minute, MaxInFlight: 1, SettleBudget: time.Second, CleanupBudget: time.Second, MaxTraceBytes: 8 << 20, WorkloadSeed: seed, FaultSeed: seed, Limits: limits}
		retained, retainedErr := Search(ctx, cfg, &fixedDSTGenerator{scenario: scenario, info: generator.Info()}, &Runner{Driver: &ArtifactDriver{}, Clock: clock, Directory: evidence, Provenance: provenance})
		retained.Completed = result.Completed
		return retained, errors.Join(generateErr, retainedErr)
	}
	return result, nil
}

type fixedDSTGenerator struct {
	scenario Scenario
	info     GeneratorInfo
}

func (g *fixedDSTGenerator) Info() GeneratorInfo { return g.info }
func (g *fixedDSTGenerator) Next(context.Context, GenerateRequest) (Scenario, error) {
	return cloneScenario(g.scenario), nil
}
