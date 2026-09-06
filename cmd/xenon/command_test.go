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

	"github.com/0x63616c/xenon/internal/agent"
	"github.com/0x63616c/xenon/internal/buildinfo"
)

type forbiddenInput struct{ t *testing.T }

func (r forbiddenInput) Read([]byte) (int, error) {
	r.t.Fatal("CLI unexpectedly prompted on stdin")
	return 0, io.EOF
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
				func(context.Context, agent.Config) error { t.Fatal("backend started"); return nil })
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
				func(context.Context, agent.Config) error { t.Fatal("backend started"); return nil })
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
			func(actual context.Context, c agent.Config) error {
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
		if failure == nil && (code != 0 || diagnostics.Len() != 0) {
			t.Fatal(code, &diagnostics)
		}
		if failure != nil && (code != 1 || diagnostics.String() != failure.Error()+"\n") {
			t.Fatal("lost primary failure", code, &diagnostics)
		}
	}
}

func TestCanceledInvocationCannotStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, diagnostics bytes.Buffer
	code := execute(ctx, []string{"start", "--config", configPath(t)}, forbiddenInput{t}, &out, &diagnostics,
		func(context.Context, agent.Config) error { t.Fatal("canceled invocation started"); return nil })
	if code != 1 || out.Len() != 0 || diagnostics.String() != "context canceled\n" {
		t.Fatal(code, &out, &diagnostics)
	}
}

type brokenOutput struct{}

func (brokenOutput) Write([]byte) (int, error) { return 0, errors.New("output unavailable") }
func TestOutputFailureCannotStart(t *testing.T) {
	var diagnostics bytes.Buffer
	code := execute(context.Background(), []string{"start", "--config", configPath(t)}, forbiddenInput{t}, brokenOutput{}, &diagnostics,
		func(context.Context, agent.Config) error { t.Fatal("started without startup output"); return nil })
	if code != 1 || diagnostics.String() != "output unavailable\n" {
		t.Fatal(code, &diagnostics)
	}
}
