package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/0x63616c/xenon/internal/app"
	"github.com/spf13/cobra"
)

func devCommand(run func(context.Context, string, string, string, bool) (app.DevResult, error)) *cobra.Command {
	command := &cobra.Command{Use: "dev", Short: "Manage an explicitly owned local three-node fixture"}
	for _, action := range []string{"up", "down"} {
		var fixture, state, output string
		var timeout time.Duration
		var ephemeral bool
		child := &cobra.Command{Use: action, Args: cobra.NoArgs, Short: action + " the recorded local fixture"}
		child.Flags().StringVar(&state, "state", "", "Persistent ownership and evidence directory (required)")
		child.Flags().StringVar(&output, "output", "json", "Output format: json")
		child.Flags().DurationVar(&timeout, "timeout", 5*time.Minute, "One total lifecycle budget (positive, at most ten minutes)")
		if action == "up" {
			child.Flags().StringVar(&fixture, "fixture", "", "Explicit schema-1 fixture file with immutable image (required)")
		} else {
			child.Flags().BoolVar(&ephemeral, "ephemeral", false, "Permanently retire this fixture and delete its owned object volume")
		}
		child.RunE = func(cmd *cobra.Command, _ []string) error {
			if output != "json" {
				return fmt.Errorf("--output must be json")
			}
			result := app.DevResult{Schema: 1, Status: "invalid"}
			var err error
			if state == "" || action == "up" && fixture == "" {
				err = fmt.Errorf("explicit --state and (for up) --fixture required")
			} else if timeout <= 0 || timeout > 10*time.Minute {
				err = fmt.Errorf("--timeout must be positive and at most ten minutes")
			} else {
				ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
				defer cancel()
				result, err = run(ctx, action, fixture, state, ephemeral)
			}
			return errors.Join(err, json.NewEncoder(cmd.OutOrStdout()).Encode(result))
		}
		command.AddCommand(child)
	}
	return command
}
