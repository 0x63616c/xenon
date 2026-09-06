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

func inspectCommand(inspect func(context.Context, app.Config) (app.Inspection, error)) *cobra.Command {
	var path, output string
	var timeout time.Duration
	command := &cobra.Command{Use: "inspect", Short: "Read persisted authority without provisioning or probing native storage", Args: cobra.NoArgs}
	command.Flags().StringVar(&path, "config", "", "Explicit Xenon JSON configuration file (required)")
	command.Flags().StringVar(&output, "output", "json", "Output format: json")
	command.Flags().DurationVar(&timeout, "timeout", 5*time.Second, "Total inspection budget (positive, at most one minute)")
	command.RunE = func(cmd *cobra.Command, _ []string) error {
		result := app.Inspection{Schema: 1, Status: "invalid"}
		var err error
		if output != "json" {
			return fmt.Errorf("--output must be json")
		}
		if path == "" {
			err = fmt.Errorf("--config FILE required")
		} else if timeout <= 0 || timeout > time.Minute {
			err = fmt.Errorf("--timeout must be positive and at most one minute")
		}
		var c app.Config
		if err == nil {
			c, err = app.Load(path)
		}
		if err == nil {
			err = app.ValidateServiceLayout(c)
		}
		if err == nil {
			ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
			defer cancel()
			result, err = inspect(ctx, c)
		} else {
			result.Error = err.Error()
		}
		if writeErr := json.NewEncoder(cmd.OutOrStdout()).Encode(result); writeErr != nil {
			return errors.Join(err, writeErr)
		}
		return err
	}
	return command
}
