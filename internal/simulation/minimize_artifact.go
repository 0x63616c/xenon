package simulation

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// MinimizeArtifact reduces only saved coupled production-Step failures. The
// recorded primary fingerprint is a hypothesis until the original reproduces.
// Expanded input decoding/checksum validation is identical to Replay.
func MinimizeArtifact(ctx context.Context, path string, cfg MinimizeConfig, runner *Runner) (MinimizeResult, error) {
	record, err := readReplayArtifact(path, false)
	if err != nil {
		return MinimizeResult{}, err
	}
	if err := validateReplayProvenance(record, runner); err != nil {
		return MinimizeResult{}, err
	}
	if decoded, e := hex.DecodeString(record.Generator.SHA256); e != nil || len(decoded) != 32 || record.Generator.Version == "" || len(record.Generator.Capabilities) == 0 {
		return MinimizeResult{}, errors.New("generator metadata required")
	}
	if record.Scenario.Kind != CoupledKind {
		return MinimizeResult{}, errors.New("minimization supports coupled production-Step artifacts only")
	}
	f, err := os.Open(filepath.Join(filepath.Dir(path), "failure.json"))
	if err != nil {
		return MinimizeResult{}, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return MinimizeResult{}, err
	}
	if len(raw) > 1<<20 {
		return MinimizeResult{}, errors.New("oversized failure receipt")
	}
	var failure caseResult
	if err = strictJSON(raw, &failure); err != nil {
		return MinimizeResult{}, err
	}
	if failure.FailureFingerprint == nil || failure.FailureFingerprint.Validate() != nil || (failure.FailurePhase != "run" && failure.FailurePhase != "settle") {
		return MinimizeResult{}, errors.New("saved primary invariant fingerprint required")
	}
	if runner == nil || cfg.AttemptBudget <= record.Config.CleanupBudget {
		return MinimizeResult{}, errors.New("runner and attempt budget greater than saved cleanup budget required")
	}
	attemptRunner := *runner
	attemptRunner.Directory = cfg.Directory
	check := record.Config
	check.MaxCases = 1
	check.Continuous = false
	if err = validateRunner(check, &attemptRunner); err != nil {
		return MinimizeResult{}, err
	}
	cfg.Simulation = true
	out, err := Minimize(ctx, cfg, record.Scenario, *failure.FailureFingerprint, CoupledReductionSpace{Limits: record.Config.Limits}, &RunnerPredicate{Runner: attemptRunner, Config: check})
	if out.OriginalVerified {
		// A stable path is directly replayable without any generator/reducer installed.
		record.Provenance = attemptRunner.Provenance
		record.Scenario = cloneScenario(out.Best)
		raw, _ = json.Marshal(record.Scenario)
		record.SHA256 = hash(raw)
		record.ReplayOf = path
		err = errors.Join(err, saveArtifact(filepath.Join(cfg.Directory, "best-scenario.json"), record))
	}
	return out, err
}
