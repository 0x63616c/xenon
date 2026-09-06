//go:build darwin || linux

package simulation

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestResidentProcessTerminalFailureBeforeBlockedExit(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "child")
	if e := os.WriteFile(script, []byte("#!/bin/sh\necho 'iteration 0 encountered error: invariant'\ntrap 'exit 0' INT\nsleep 30\n"), 0700); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	e := runOmesProcess(ctx, script, nil, dir, filepath.Join(dir, "run.log"))
	if e == nil || !strings.Contains(e.Error(), "terminal Omes") {
		t.Fatalf("primary %v", e)
	}
	raw, e := os.ReadFile(filepath.Join(dir, "run.log.failure.json"))
	if e != nil || !strings.Contains(string(raw), "terminal Omes") {
		t.Fatalf("durable primary %s %v", raw, e)
	}
}
func TestResidentProcessPersistsExitOutput(t *testing.T) {
	dir := t.TempDir()
	e := runOmesProcess(context.Background(), "/bin/sh", []string{"-c", "echo exact; exit 3"}, dir, filepath.Join(dir, "run.log"))
	raw, re := os.ReadFile(filepath.Join(dir, "run.log"))
	if e == nil || re != nil || string(raw) != "exact\n" {
		t.Fatalf("%v %v %q", e, re, raw)
	}
}
func TestResidentProcessAlreadyExitedTerminalDoesNotWaitTwice(t *testing.T) {
	dir := t.TempDir()
	start := time.Now()
	e := runOmesProcess(context.Background(), "/bin/sh", []string{"-c", "echo 'iteration 0 failed: terminal'; exit 1"}, dir, filepath.Join(dir, "run.log"))
	if e == nil || !strings.Contains(e.Error(), "terminal Omes") || time.Since(start) > time.Second*3 {
		t.Fatalf("exit race %v duration %s", e, time.Since(start))
	}
}
