package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/0x63616c/xenon/internal/integration"
	"github.com/spf13/cobra"
)

type integrationRunner func(context.Context, string, io.Writer) (integration.JourneyResult, error)

var integrationJourneys = []string{"slatedb-minio", "multi-node-ownership", "temporal-compatibility"}

func integrationCommand(run integrationRunner) *cobra.Command {
	var only string
	cmd := &cobra.Command{
		Use:   "integration",
		Short: "Run a bounded real-system journey",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			names := integrationJourneys
			if only != "" {
				known := false
				for _, name := range integrationJourneys {
					known = known || name == only
				}
				if !known {
					return fmt.Errorf("unknown integration journey %q", only)
				}
				names = []string{only}
			}
			results := make([]integration.JourneyResult, 0, len(names))
			var runErr error
			for _, name := range names {
				result, err := run(cmd.Context(), name, cmd.ErrOrStderr())
				if result.Name == "" {
					result.Name = name
				}
				if err != nil {
					result.Status, result.Error = "failed", err.Error()
				}
				results = append(results, result)
				if err != nil {
					runErr = fmt.Errorf("%s: %w", name, err)
					break
				}
			}
			encodeErr := json.NewEncoder(cmd.OutOrStdout()).Encode(struct {
				Schema  int                         `json:"schema"`
				Results []integration.JourneyResult `json:"results"`
			}{1, results})
			return errors.Join(runErr, encodeErr)
		},
	}
	cmd.Flags().StringVar(&only, "only", "", "Run only one named integration journey")
	return cmd
}
