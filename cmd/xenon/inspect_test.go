package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/0x63616c/xenon/internal/app"
	"github.com/spf13/cobra"
)

func TestInspectCommandValidationAndDeadline(t *testing.T) {
	path := configPath(t)
	for _, tc := range []struct {
		name   string
		args   []string
		calls  int
		status app.InspectionStatus
	}{
		{"valid", []string{"--config", path, "--timeout", "2s"}, 1, app.InspectionObserved},
		{"missing config", nil, 0, app.InspectionInvalid},
		{"missing file", []string{"--config", "missing.json"}, 0, app.InspectionInvalid},
		{"zero timeout", []string{"--config", path, "--timeout", "0s"}, 0, app.InspectionInvalid},
		{"unbounded timeout", []string{"--config", path, "--timeout", "2m"}, 0, app.InspectionInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			var out bytes.Buffer
			command := inspectCommand(func(ctx context.Context, c app.Config) (app.Inspection, error) {
				calls++
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > 2*time.Second || time.Until(deadline) <= 0 || c.ServiceStorage == nil {
					t.Fatal("lost validated input/deadline")
				}
				return app.Inspection{Schema: 1, Status: app.InspectionObserved}, nil
			})
			command.SilenceErrors = true
			command.SilenceUsage = true
			command.SetOut(&out)
			command.SetArgs(tc.args)
			err := command.ExecuteContext(context.Background())
			var got app.Inspection
			decoder := json.NewDecoder(&out)
			if decodeErr := decoder.Decode(&got); decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if decoder.Decode(new(any)) != io.EOF || got.Status != tc.status || calls != tc.calls || (err != nil) != (tc.calls == 0) {
				t.Fatal(got, calls, err)
			}
		})
	}
}

func TestInspectUnavailableRemainsNonzeroJSON(t *testing.T) {
	var out bytes.Buffer
	failure := errors.New("authority unavailable")
	command := inspectCommand(func(context.Context, app.Config) (app.Inspection, error) {
		return app.Inspection{Schema: 1, Status: app.InspectionUnavailable, Error: failure.Error()}, failure
	})
	command.SilenceErrors = true
	command.SilenceUsage = true
	command.SetOut(&out)
	command.SetArgs([]string{"--config", configPath(t)})
	if err := command.ExecuteContext(context.Background()); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	var got app.Inspection
	if err := json.Unmarshal(out.Bytes(), &got); err != nil || got.Status != app.InspectionUnavailable || got.Control != nil {
		t.Fatal(got, err)
	}
}

func TestCheckConfigJSONContract(t *testing.T) {
	path := configPath(t)
	for _, tc := range []struct {
		args   []string
		status string
		code   int
	}{
		{[]string{"--config", path}, "valid", 0},
		{nil, "invalid", 1},
		{[]string{"--config", "missing.json"}, "invalid", 1},
	} {
		var out, diagnostics bytes.Buffer
		args := append([]string{"check-config", "--output", "json"}, tc.args...)
		code := execute(context.Background(), args, forbiddenInput{t}, &out, &diagnostics, func(context.Context, app.Config) error { t.Fatal("backend started"); return nil })
		var got configCheckResult
		decoder := json.NewDecoder(&out)
		if err := decoder.Decode(&got); err != nil {
			t.Fatal(err)
		}
		if decoder.Decode(new(any)) != io.EOF || got.Schema != 1 || got.Status != tc.status || code != tc.code || (diagnostics.Len() > 0) != (code != 0) {
			t.Fatal(got, code, &diagnostics)
		}
	}
}

func TestEveryCommandHelpIsPassive(t *testing.T) {
	root := newCommand(forbiddenInput{t}, io.Discard, io.Discard, func(context.Context, app.Config) error { t.Fatal("backend started"); return nil })
	var paths [][]string
	var walk func(*cobra.Command, []string)
	walk = func(command *cobra.Command, path []string) {
		paths = append(paths, append(append([]string{}, path...), "--help"))
		for _, child := range command.Commands() {
			walk(child, append(append([]string{}, path...), child.Name()))
		}
	}
	walk(root, nil)
	for _, args := range paths {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var out, diagnostics bytes.Buffer
			code := execute(context.Background(), args, forbiddenInput{t}, &out, &diagnostics, func(context.Context, app.Config) error { t.Fatal("backend started"); return nil })
			if code != 0 || out.Len() == 0 || diagnostics.Len() != 0 {
				t.Fatal(code, &out, &diagnostics)
			}
		})
	}
}
