package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/0x63616c/xenon/internal/app"
	"github.com/0x63616c/xenon/internal/buildinfo"
	"github.com/spf13/cobra"
)

type forbiddenInput struct{ t *testing.T }

func (r forbiddenInput) Read([]byte) (int, error) {
	r.t.Fatal("CLI unexpectedly prompted on stdin")
	return 0, io.EOF
}

type effectCounts [4]int

func instrumentedDependencies(counts *effectCounts) commandDependencies {
	return commandDependencies{
		start: func(context.Context, app.Config) error { return nil },
		inspect: func(context.Context, app.Config) (app.Inspection, error) {
			return app.Inspection{Schema: 1, Status: app.InspectionObserved}, nil
		},
		dev: func(context.Context, string, string, string, bool) (app.DevResult, error) {
			return app.DevResult{Schema: 1, Status: "started", CleanupVerified: true}, nil
		},
		profile: func(context.Context, profileRequest, io.Writer) (profileResult, error) {
			return profileResult{Schema: 1, Status: "component-passed", CleanupVerified: true}, nil
		},
		getenv: func(string) string { return "" },
		observe: func(effects ...commandEffect) {
			for _, effect := range effects {
				counts[effect]++
			}
		},
	}
}

// This is the executable CLI-01 stream/exit matrix. Keeping it in Go makes
// additions compile-checked and lets every row use the production command tree.
func TestCLIContractMatrixAndZeroEffects(t *testing.T) {
	path := configPath(t)
	invalid := filepath.Join(t.TempDir(), "invalid.json")
	if err := os.WriteFile(invalid, []byte(`{"unknown":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	type row struct {
		name   string
		args   []string
		exit   int
		stdout string
		stderr bool
		env    string
	}
	rows := []row{
		{"root-help", []string{"--help"}, 0, "text", false, ""},
		{"version", []string{"version"}, 0, "json", false, ""},
		{"completion", []string{"completion", "bash"}, 0, "text", false, ""},
		{"config-text-valid", []string{"check-config", "--config", path}, 0, "valid", false, ""},
		{"config-json-valid", []string{"check-config", "--config", path, "--output", "json"}, 0, "json", false, ""},
		{"config-text-invalid", []string{"check-config", "--config", invalid}, 1, "empty", true, ""},
		{"config-json-invalid", []string{"check-config", "--config", invalid, "--output", "json"}, 1, "json", true, ""},
		{"config-missing", []string{"check-config"}, 1, "empty", true, ""},
		{"config-unsupported-env", []string{"check-config", "--config", path}, 1, "empty", true, "XENON_CONFIG"},
		{"config-unsupported-env-file", []string{"check-config", "--config", path}, 1, "empty", true, "XENON_CONFIG_FILE"},
		{"inspect-unsupported-env", []string{"inspect", "--config", path}, 1, "json", true, "XENON_CONFIG"},
		{"inspect-unsupported-env-file", []string{"inspect", "--config", path}, 1, "json", true, "XENON_CONFIG_FILE"},
		{"unknown-command", []string{"unknown"}, 1, "empty", true, ""},
		{"start-missing-config", []string{"start"}, 1, "empty", true, ""},
		{"inspect-missing-config", []string{"inspect"}, 1, "json", true, ""},
		{"dev-invalid-timeout", []string{"dev", "up", "--fixture", "f", "--state", "s", "--timeout", "0s"}, 1, "json", true, ""},
		{"generate-unknown-flag", []string{"generate", "workflow", "--bad"}, 1, "empty", true, ""},
		{"simulation-unknown-flag", []string{"test", "simulation", "--bad"}, 1, "empty", true, ""},
		{"workflow-unknown-flag", []string{"test", "workflow", "--bad"}, 1, "empty", true, ""},
		{"profile-missing-bindings", []string{"test", "smoke"}, 1, "empty", true, ""},
		{"search-unknown-flag", []string{"search", "--bad"}, 1, "empty", true, ""},
		{"replay-unknown-flag", []string{"replay", "--bad"}, 1, "empty", true, ""},
		{"minimize-unknown-flag", []string{"minimize", "--bad"}, 1, "empty", true, ""},
	}
	root := newCommandWithDependencies(strings.NewReader(""), io.Discard, io.Discard, instrumentedDependencies(new(effectCounts)))
	helpPaths := 0
	var addHelpRows func(*cobra.Command, []string)
	addHelpRows = func(command *cobra.Command, path []string) {
		rows = append(rows, row{"help-" + strings.Join(path, "-"), append(append([]string{}, path...), "--help"), 0, "text", false, ""})
		helpPaths++
		for _, child := range command.Commands() {
			addHelpRows(child, append(append([]string{}, path...), child.Name()))
		}
	}
	addHelpRows(root, nil)
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			var counts effectCounts
			dependencies := instrumentedDependencies(&counts)
			dependencies.getenv = func(name string) string {
				if name == tc.env {
					return "forbidden"
				}
				return ""
			}
			var out, diagnostics bytes.Buffer
			code := executeWithDependencies(context.Background(), tc.args, forbiddenInput{t}, &out, &diagnostics, dependencies)
			if code != tc.exit || (diagnostics.Len() != 0) != tc.stderr || counts != (effectCounts{}) {
				t.Fatalf("code=%d stdout=%q stderr=%q effects=%v", code, &out, &diagnostics, counts)
			}
			switch tc.stdout {
			case "empty":
				if out.Len() != 0 {
					t.Fatal("stdout must be empty", &out)
				}
			case "text":
				if out.Len() == 0 {
					t.Fatal("stdout must contain help")
				}
			case "valid":
				if out.String() != "XENON_CONFIG_VALID\n" {
					t.Fatal(&out)
				}
			case "json":
				decoder := json.NewDecoder(&out)
				var value map[string]any
				if decoder.Decode(&value) != nil || decoder.Decode(new(any)) != io.EOF {
					t.Fatal("stdout is not exactly one JSON object", &out)
				}
			}
		})
	}
	t.Logf("CLI_CONTRACT_MATRIX rows=%d help_paths=%d config_sources=1", len(rows), helpPaths)
}

func TestCLIContractEffectCountersDetectEveryBoundary(t *testing.T) {
	path := configPath(t)
	tests := []struct {
		name string
		args []string
		want effectCounts
	}{
		{"start", []string{"start", "--config", path}, effectCounts{1, 1, 0, 1}},
		{"inspect", []string{"inspect", "--config", path}, effectCounts{0, 0, 0, 1}},
		{"dev", []string{"dev", "up", "--fixture", "fixture", "--state", "state", "--timeout", time.Second.String()}, effectCounts{1, 0, 1, 1}},
		{"profile", []string{"test", "smoke", "--repository", "repo", "--evidence", "evidence"}, effectCounts{1, 1, 1, 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var counts effectCounts
			var out, diagnostics bytes.Buffer
			code := executeWithDependencies(context.Background(), tc.args, forbiddenInput{t}, &out, &diagnostics, instrumentedDependencies(&counts))
			if code != 0 || counts != tc.want {
				t.Fatalf("code=%d effects=%v want=%v stderr=%s", code, counts, tc.want, &diagnostics)
			}
		})
	}
	t.Logf("CLI_EFFECT_NEGATIVE_CONTROLS boundaries=%d counters=4", len(tests))
}

func configPath(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("../../test/scenarios/agent/a.json")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "agent.json")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLightweightCommandsNeverStartBackend(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	path := configPath(t)
	for _, args := range [][]string{
		{}, {"--help"}, {"help", "start"}, {"start", "--help"},
		{"version"}, {"check-config", "--config", path},
		{"completion", "bash"}, {"completion", "zsh"}, {"completion", "fish"}, {"completion", "powershell"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var out, diagnostics bytes.Buffer
			code := execute(context.Background(), args, forbiddenInput{t}, &out, &diagnostics,
				func(context.Context, app.Config) error { t.Fatal("backend started"); return nil })
			if code != 0 || diagnostics.Len() != 0 || out.Len() == 0 {
				t.Fatalf("code=%d stdout=%q stderr=%q", code, &out, &diagnostics)
			}
			if len(args) > 0 && args[0] == "version" {
				var got buildinfo.Info
				// Validate exactly one JSON value, including the current stable field names.
				var value map[string]any
				decoder := json.NewDecoder(&out)
				if err := decoder.Decode(&value); err != nil {
					t.Fatal(err)
				}
				if err := decoder.Decode(&got); err != io.EOF {
					t.Fatal("extra stdout", err)
				}
				expected, _ := json.Marshal(buildinfo.Read())
				var fields map[string]any
				_ = json.Unmarshal(expected, &fields)
				if len(value) != len(fields) {
					t.Fatal(value)
				}
				for field := range fields {
					if _, ok := value[field]; !ok {
						t.Fatal("missing field", field)
					}
				}
			}
			if len(args) > 0 && args[0] == "check-config" && out.String() != "XENON_CONFIG_VALID\n" {
				t.Fatal(&out)
			}
		})
	}
}

func TestCommandErrorsUseOnlyStderrAndDoNotStart(t *testing.T) {
	invalid := filepath.Join(t.TempDir(), "invalid.json")
	if err := os.WriteFile(invalid, []byte(`{"unknown":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"unknown"}, {"version", "extra"}, {"version", "--config", invalid},
		{"start"}, {"start", "--config="}, {"start", "--bad"},
		{"check-config", "--config", invalid}, {"start", "--config", invalid},
		{"start", "--config", "missing-file"}, {"completion", "unknown-shell"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var out, diagnostics bytes.Buffer
			code := execute(context.Background(), args, forbiddenInput{t}, &out, &diagnostics,
				func(context.Context, app.Config) error { t.Fatal("backend started"); return nil })
			if code != 1 || out.Len() != 0 || diagnostics.Len() == 0 || strings.Contains(diagnostics.String(), "Usage:") {
				t.Fatalf("code=%d stdout=%q stderr=%q", code, &out, &diagnostics)
			}
		})
	}
}

func TestStartReceivesValidatedConfigAndCallerCancellation(t *testing.T) {
	path := configPath(t)
	for _, failure := range []error{nil, errors.New("shutdown incomplete: native call retained")} {
		ctx, cancel := context.WithCancel(context.Background())
		var out, diagnostics bytes.Buffer
		called := 0
		code := execute(ctx, []string{"start", "--config", path}, forbiddenInput{t}, &out, &diagnostics,
			func(actual context.Context, c app.Config) error {
				called++
				if actual != ctx || c.ServiceStorage == nil || c.ServiceStorage.ClusterID == "" {
					t.Fatal("lost context/config")
				}
				if !json.Valid(bytes.TrimSpace(out.Bytes())) {
					t.Fatal("missing startup JSON before backend", &out)
				}
				cancel()
				<-actual.Done()
				return failure
			})
		cancel()
		if called != 1 {
			t.Fatal("start calls", called)
		}
		if failure == nil && (code != 0 || diagnostics.String() != startupBanner) {
			t.Fatal(code, &diagnostics)
		}
		if failure != nil && (code != 1 || diagnostics.String() != startupBanner+failure.Error()+"\n") {
			t.Fatal("lost primary failure", code, &diagnostics)
		}
	}
}

func TestCanceledInvocationCannotStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, diagnostics bytes.Buffer
	code := execute(ctx, []string{"start", "--config", configPath(t)}, forbiddenInput{t}, &out, &diagnostics,
		func(context.Context, app.Config) error { t.Fatal("canceled invocation started"); return nil })
	if code != 1 || out.Len() != 0 || diagnostics.String() != "context canceled\n" {
		t.Fatal(code, &out, &diagnostics)
	}
}

type brokenOutput struct{}

func (brokenOutput) Write([]byte) (int, error) { return 0, errors.New("output unavailable") }
func TestOutputFailureCannotStart(t *testing.T) {
	var diagnostics bytes.Buffer
	code := execute(context.Background(), []string{"start", "--config", configPath(t)}, forbiddenInput{t}, brokenOutput{}, &diagnostics,
		func(context.Context, app.Config) error { t.Fatal("started without startup output"); return nil })
	if code != 1 || diagnostics.String() != "output unavailable\n" {
		t.Fatal(code, &diagnostics)
	}
}

// Exercise the exact file copied by Dockerfile.unified-agent, not a separate
// scenario fixture that can stay valid while the shipped example goes stale.
func TestPackagedExamplePassesCLIValidation(t *testing.T) {
	var out, diagnostics bytes.Buffer
	code := execute(context.Background(), []string{"check-config", "--config", "../../deploy/agent.example.json"}, forbiddenInput{t}, &out, &diagnostics,
		func(context.Context, app.Config) error { t.Fatal("validation started backend"); return nil })
	if code != 0 || out.String() != "XENON_CONFIG_VALID\n" || diagnostics.Len() != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, &out, &diagnostics)
	}
}
