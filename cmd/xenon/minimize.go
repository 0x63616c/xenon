package main

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"time"

	"github.com/0x63616c/xenon/internal/buildinfo"
	"github.com/0x63616c/xenon/internal/simulation"
	"github.com/spf13/cobra"
)

func minimizeCommand(build func() buildinfo.Info, clock simulation.Clock) *cobra.Command {
	var artifact, evidence string
	var development bool
	cfg := simulation.MinimizeConfig{Clock: clock, Simulation: true}
	c := &cobra.Command{Use: "minimize", Short: "Reduce a saved coupled production-Step invariant failure", Args: cobra.NoArgs, PersistentPreRunE: func(*cobra.Command, []string) error { return nil }}
	c.Flags().StringVar(&artifact, "artifact", "", "Saved case scenario.json with sibling failure.json")
	c.Flags().StringVar(&evidence, "evidence", "", "New reduction evidence directory (must not exist)")
	c.Flags().BoolVar(&development, "development", false, "Allow unknown/modified build provenance; label output development")
	c.Flags().DurationVar(&cfg.MaxDuration, "duration", time.Minute, "Total reduction time including attempts and cleanup")
	c.Flags().DurationVar(&cfg.AttemptBudget, "attempt-budget", 5*time.Second, "Each replay budget including saved cleanup allowance")
	c.Flags().Uint64Var(&cfg.MaxAttempts, "max-attempts", 256, "Maximum replay attempts; original consumes three")
	c.Flags().Uint64Var(&cfg.MaxProposals, "max-proposals", 2048, "Maximum deterministic proposals including rejected inputs")
	_ = c.MarkFlagRequired("artifact")
	_ = c.MarkFlagRequired("evidence")
	_ = c.MarkFlagFilename("artifact", "json")
	c.RunE = func(c *cobra.Command, _ []string) error {
		runner, err := simulationRunner(evidence, development, build(), clock)
		if err != nil {
			return err
		}
		cfg.Directory = runner.Directory
		result, err := simulation.MinimizeArtifact(c.Context(), artifact, cfg, runner)
		qualification := "component"
		if development {
			qualification = "development"
		}
		best := ""
		if result.OriginalVerified {
			best = filepath.Join(cfg.Directory, "best-scenario.json")
		}
		outputErr := json.NewEncoder(c.OutOrStdout()).Encode(struct {
			Schema        int                       `json:"schema"`
			Mode          string                    `json:"mode"`
			Qualification string                    `json:"qualification"`
			Result        simulation.MinimizeResult `json:"result"`
			BestArtifact  string                    `json:"best_artifact,omitempty"`
		}{1, "coupled-component-minimization", qualification, result, best})
		if outputErr != nil {
			return errors.Join(err, outputErr)
		}
		switch result.StopReason {
		case "budget":
			return &commandExit{2, errors.Join(context.DeadlineExceeded, err)}
		case "canceled":
			return &commandExit{130, errors.Join(context.Canceled, err)}
		}
		return err
	}
	return c
}
