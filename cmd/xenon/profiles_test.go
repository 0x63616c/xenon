package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/0x63616c/xenon/internal/agent"
	"github.com/spf13/cobra"
)

func TestRealProfileCommandsUseTypedExecutorAndCleanStreams(t *testing.T) {
	for _, name := range []string{"smoke", "local-release-ten-minute"} {
		var out, diagnostics bytes.Buffer
		called := 0
		root := &cobra.Command{Use: "xenon", SilenceErrors: true, SilenceUsage: true}
		group := &cobra.Command{Use: "test"}
		root.AddCommand(group)
		group.AddCommand(realProfileCommands(func(ctx context.Context, request profileRequest, w io.Writer) (profileResult, error) {
			called++
			if request.Repository != "explicit/repo" || request.Evidence != "new/evidence" || ctx == nil {
				t.Fatal(request)
			}
			fmt.Fprintln(w, "retained diagnostic")
			return profileResult{Schema: 1, Profile: request.Profile, Status: "canceled", Evidence: request.Evidence}, &commandExit{130, context.Canceled}
		})...)
		root.SetOut(&out)
		root.SetErr(&diagnostics)
		root.SetArgs([]string{"test", name, "--repository", "explicit/repo", "--evidence", "new/evidence"})
		err := root.ExecuteContext(context.Background())
		var exit *commandExit
		if !errors.As(err, &exit) || exit.code != 130 || called != 1 {
			t.Fatal(err, called)
		}
		var result profileResult
		if json.Unmarshal(out.Bytes(), &result) != nil || result.Status != "canceled" || diagnostics.String() != "retained diagnostic\n" {
			t.Fatal(&out, &diagnostics)
		}
	}
}
func TestRealProfileHelpNeverInitializesBackend(t *testing.T) {
	for _, name := range []string{"smoke", "local-release-ten-minute"} {
		var out, diagnostics bytes.Buffer
		code := execute(context.Background(), []string{"test", name, "--help"}, forbiddenInput{t}, &out, &diagnostics, func(context.Context, agent.Config) error { t.Fatal("backend started"); return nil })
		if code != 0 || diagnostics.Len() != 0 || !strings.Contains(out.String(), "--repository") || !strings.Contains(out.String(), "Docker") {
			t.Fatal(code, &out, &diagnostics)
		}
	}
}
func TestRealProfilePreCanceledRetainsReceiptWithoutTools(t *testing.T) {
	t.Setenv("PATH", "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	evidence := filepath.Join(t.TempDir(), "evidence")
	result, err := runRealProfile(ctx, profileRequest{Repository: t.TempDir(), Evidence: evidence, Profile: "smoke"}, io.Discard)
	var exit *commandExit
	if !errors.As(err, &exit) || exit.code != 130 || result.Status != "canceled" || !result.CleanupVerified {
		t.Fatal(result, err)
	}
	if _, err = os.Stat(filepath.Join(evidence, "cli-result.json")); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(evidence, "helper.log")); !os.IsNotExist(err) {
		t.Fatal("helper launched")
	}
}
func TestRealProfileRefusesExistingEvidenceAndMissingCheckout(t *testing.T) {
	existing := t.TempDir()
	if _, err := runRealProfile(context.Background(), profileRequest{Repository: t.TempDir(), Evidence: existing, Profile: "smoke"}, io.Discard); err == nil {
		t.Fatal("overwrote evidence")
	}
	evidence := filepath.Join(t.TempDir(), "new")
	if _, err := runRealProfile(context.Background(), profileRequest{Repository: t.TempDir(), Evidence: evidence, Profile: "smoke"}, io.Discard); err == nil {
		t.Fatal("missing helper accepted")
	}
}
func TestProfileProcessHelper(t *testing.T) {
	mode := os.Args[len(os.Args)-1]
	if !strings.HasPrefix(mode, "--profile-helper-") {
		return
	}
	if mode == "--profile-helper-ignore" {
		signal.Ignore(syscall.SIGTERM)
		fmt.Println("ready")
		time.Sleep(30 * time.Second)
		os.Exit(0)
	}
	if mode == "--profile-helper-failure" {
		fmt.Println("first cause retained")
		os.Exit(3)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	fmt.Println("ready")
	<-signals
	fmt.Println("cleanup completed")
	os.Exit(0)
}
func TestProfileProcessCancellationAndForcedReap(t *testing.T) {
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"graceful", "ignore"} {
		t.Run(mode, func(t *testing.T) {
			log := filepath.Join(t.TempDir(), "helper.log")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			canceled := make(chan struct{})
			go func() {
				defer close(canceled)
				for i := 0; i < 200; i++ {
					raw, _ := os.ReadFile(log)
					if bytes.Contains(raw, []byte("ready")) {
						cancel()
						return
					}
					time.Sleep(5 * time.Millisecond)
				}
				cancel()
			}()
			started := time.Now()
			err, forced := superviseProfile(ctx, executable, []string{"-test.run=^TestProfileProcessHelper$", "--", "--profile-helper-" + mode}, t.TempDir(), log, io.Discard, 50*time.Millisecond)
			<-canceled
			if !errors.Is(err, context.Canceled) || forced != (mode == "ignore") || time.Since(started) > 3*time.Second {
				t.Fatal(err, forced, time.Since(started))
			}
			raw, readErr := os.ReadFile(log)
			if readErr != nil || !bytes.Contains(raw, []byte("ready")) {
				t.Fatal(readErr, string(raw))
			}
			if mode == "graceful" && !bytes.Contains(raw, []byte("cleanup completed")) {
				t.Fatal(string(raw))
			}
		})
	}
}
func TestProfileProcessFailureKeepsOutputOutOfJSONStream(t *testing.T) {
	executable, _ := os.Executable()
	log := filepath.Join(t.TempDir(), "helper.log")
	var diagnostics bytes.Buffer
	err, forced := superviseProfile(context.Background(), executable, []string{"-test.run=^TestProfileProcessHelper$", "--", "--profile-helper-failure"}, t.TempDir(), log, &diagnostics, time.Second)
	raw, _ := os.ReadFile(log)
	if err == nil || forced || !bytes.Contains(raw, []byte("first cause retained")) || !strings.Contains(diagnostics.String(), log) {
		t.Fatal(err, forced, string(raw), &diagnostics)
	}
}

func TestRealProfileActualHelperReceiptAndOutput(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	tools := t.TempDir()
	if err = os.Symlink(python, filepath.Join(tools, "python3")); err != nil {
		t.Fatal(err)
	}
	for _, tool := range []string{"go", "rustup", "cargo", "cc", "aws", "docker"} {
		if err = os.WriteFile(filepath.Join(tools, tool), []byte("#!/bin/sh\nexit 99\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err = os.WriteFile(filepath.Join(tools, "git"), []byte("#!/bin/sh\nif [ \"$3\" = rev-parse ]; then printf aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa; fi\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", tools)
	repository := t.TempDir()
	if err = os.Mkdir(filepath.Join(repository, "scripts"), 0700); err != nil {
		t.Fatal(err)
	}
	helper := `import argparse,hashlib,json,pathlib
p=argparse.ArgumentParser();p.add_argument('--profile');p.add_argument('--evidence');a=p.parse_args()
e=pathlib.Path(a.evidence);e.mkdir()
(e/'result.json').write_text(json.dumps({'schema':1,'revision':'a'*40,'dirty':False,'development':False,'harness_hashes':{'agent-smoke.py':hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()},'profile':a.profile,'status':'component-passed'}))
print('helper diagnostics retained separately')
print(json.dumps({'receipt':str(e),'status':'component-passed'}))
`
	if err = os.WriteFile(filepath.Join(repository, "scripts/agent-smoke.py"), []byte(helper), 0600); err != nil {
		t.Fatal(err)
	}
	evidence := filepath.Join(t.TempDir(), "proof")
	var diagnostics bytes.Buffer
	result, err := runRealProfile(context.Background(), profileRequest{Repository: repository, Evidence: evidence, Profile: "smoke"}, &diagnostics)
	if err != nil || result.Status != "component-passed" || !result.CleanupVerified || diagnostics.Len() != 0 {
		t.Fatal(result, err, &diagnostics)
	}
	raw, err := os.ReadFile(filepath.Join(evidence, "helper.log"))
	if err != nil || !bytes.Contains(raw, []byte("helper diagnostics")) {
		t.Fatal(err, string(raw))
	}
	minimal := strings.Replace(helper, "'schema':1,'revision':'a'*40,'dirty':False,'development':False,'harness_hashes':{'agent-smoke.py':hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()},", "", 1)
	if err = os.WriteFile(filepath.Join(repository, "scripts/agent-smoke.py"), []byte(minimal), 0600); err != nil {
		t.Fatal(err)
	}
	result, err = runRealProfile(context.Background(), profileRequest{Repository: repository, Evidence: filepath.Join(t.TempDir(), "missing-provenance"), Profile: "smoke"}, io.Discard)
	if err == nil || result.Status != "failed" || !strings.Contains(err.Error(), "provenance") {
		t.Fatal(result, err)
	}
	blocked := `import argparse,json,pathlib,time
p=argparse.ArgumentParser();p.add_argument('--profile');p.add_argument('--evidence');a=p.parse_args()
e=pathlib.Path(a.evidence);e.mkdir()
(e/'first-failure.json').write_text(json.dumps({'error':'RuntimeError: workload acknowledgement missing'}))
time.sleep(30)
`
	if err = os.WriteFile(filepath.Join(repository, "scripts/agent-smoke.py"), []byte(blocked), 0600); err != nil {
		t.Fatal(err)
	}
	evidence = filepath.Join(t.TempDir(), "failed-before-cancel")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := make(chan struct{})
	go func() {
		defer close(observed)
		for i := 0; i < 200; i++ {
			if _, e := os.Stat(filepath.Join(evidence, "run/first-failure.json")); e == nil {
				cancel()
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
	}()
	result, err = runRealProfile(ctx, profileRequest{Repository: repository, Evidence: evidence, Profile: "smoke"}, io.Discard)
	<-observed
	var canceled *commandExit
	if err == nil || result.Status != "failed" || errors.As(err, &canceled) || !strings.Contains(err.Error(), "workload acknowledgement missing") {
		t.Fatal(result, err)
	}
}

func TestFirstProfileFailureSurvivesMissingFinalReceipt(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "first-failure.json")
	if err := os.WriteFile(path, []byte(`{"error":"RuntimeError: workload acknowledgement missing"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if got := firstProfileFailure(filepath.Join(root, "result.json")); got != "RuntimeError: workload acknowledgement missing" {
		t.Fatal(got)
	}
}
