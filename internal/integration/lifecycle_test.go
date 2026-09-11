package integration

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/0x63616c/xenon/internal/partitions"
)

// A real child process exercises signals and Wait without Docker/native state.
func TestLifecycleHelperProcess(t *testing.T) {
	mode := os.Getenv("XENON_LIFECYCLE_TEST")
	if mode == "" {
		return
	}
	if mode == "ignore" {
		signal.Ignore(os.Interrupt)
	}
	if err := os.WriteFile(os.Getenv("XENON_LIFECYCLE_READY"), nil, 0600); err != nil {
		os.Exit(2)
	}
	if mode == "exit" {
		os.Exit(7)
	}
	if mode == "exit-on-interrupt" {
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, os.Interrupt)
		<-interrupt
		os.Exit(1)
	}
	for {
		time.Sleep(time.Hour)
	}
}

func testProcess(t *testing.T, mode string) *processLifecycle {
	t.Helper()
	ready := filepath.Join(t.TempDir(), "ready")
	command := exec.Command(os.Args[0], "-test.run=^TestLifecycleHelperProcess$")
	command.Env = append(os.Environ(), "XENON_LIFECYCLE_TEST="+mode, "XENON_LIFECYCLE_READY="+ready)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	process := trackProcess(command)
	t.Cleanup(func() { _ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := wait(ctx, func() bool { _, err := os.Stat(ready); return err == nil }); err != nil {
		t.Fatal(err)
	}
	return process
}

func TestProcessCleanupReportsUnexpectedExitAndTimeout(t *testing.T) {
	p := testProcess(t, "exit")
	<-p.done
	if err := p.stop(false); err == nil || !strings.Contains(err.Error(), "exit status 7") {
		t.Fatalf("unexpected exit passed: %v", err)
	}
	if err := p.stop(false); err == nil {
		t.Fatal("second cleanup forgot the failure")
	}
	p = testProcess(t, "ignore")
	started := time.Now()
	if err := p.terminate(false, 30*time.Millisecond); err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("forced cleanup passed: %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("cleanup was not bounded")
	}
	p = testProcess(t, "wait")
	if err := p.stop(true); err != nil {
		t.Fatalf("intentional kill failed: %v", err)
	}
	p = testProcess(t, "exit-on-interrupt")
	if err := p.stop(false); err != nil {
		t.Fatalf("coordinated non-zero shutdown failed: %v", err)
	}
}

func TestOwnedWorkerCleanupAndBoundedLogs(t *testing.T) {
	dir := t.TempDir()
	ready := filepath.Join(dir, "ready")
	env := append(os.Environ(), "XENON_LIFECYCLE_TEST=exit", "XENON_LIFECYCLE_READY="+ready)
	p, err := startOwned(context.Background(), dir, filepath.Join(dir, "worker.log"), env, os.Args[0], "-test.run=^TestLifecycleHelperProcess$")
	if err != nil {
		t.Fatal(err)
	}
	<-p.lifecycle.done
	if err = p.stop(false); err == nil {
		t.Fatal("worker exit failure lost")
	}
	if err = p.stop(false); err == nil {
		t.Fatal("worker cleanup failure lost on repeat")
	}
	b := new(synchronizedBuffer)
	_, _ = b.Write([]byte(strings.Repeat("x", diagnosticLimit*2)))
	_, _ = b.Write([]byte("last diagnostic"))
	if len(b.String()) != diagnosticLimit || !strings.HasSuffix(b.String(), "last diagnostic") {
		t.Fatal("diagnostics not bounded or recent output discarded")
	}
	f, err := os.Create(filepath.Join(dir, "bounded.log"))
	if err != nil {
		t.Fatal(err)
	}
	l := &processLog{file: f, lines: make(chan string, 1)}
	_, err = l.Write([]byte(strings.Repeat("y", 2*diagnosticLimit)))
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	info, _ := os.Stat(f.Name())
	if info.Size() != diagnosticLimit || len(l.pending) > diagnosticLimit {
		t.Fatal("worker logs unbounded")
	}
}

type closeFailure struct {
	partitions.Writer
	err     error
	bounded bool
}

func (w *closeFailure) Close(ctx context.Context) error { _, w.bounded = ctx.Deadline(); return w.err }
func TestNativeWriterCleanupPropagatesErrors(t *testing.T) {
	failure := errors.New("native shutdown failed")
	w := &closeFailure{err: failure}
	if err := closeWriter(w, false); !errors.Is(err, failure) || !w.bounded {
		t.Fatal(err, w.bounded)
	}
	w.err = partitions.ErrFenced
	if err := closeWriter(w, false); err == nil {
		t.Fatal("unexpected fence ignored")
	}
	if err := closeWriter(w, true); err != nil {
		t.Fatal("expected displacement rejected", err)
	}
}

func TestEvidenceRetainsFailureAndRemovesSuccess(t *testing.T) {
	for _, failed := range []bool{false, true} {
		dir := t.TempDir()
		result := JourneyResult{Name: "test", Inputs: map[string]string{}, Configuration: map[string]json.RawMessage{}}
		diagnostics := new(strings.Builder)
		e := &journeyEvidence{directory: dir, started: time.Now(), result: &result, diagnostics: diagnostics, log: new(synchronizedBuffer)}
		if err := e.config("fixture", map[string]int{"seed": 42}); err != nil {
			t.Fatal(err)
		}
		if result.Inputs["fixture"] != digest([]byte(`{"seed":42}`)) {
			t.Fatal("configuration hash differs")
		}
		_, _ = io.WriteString(e.writer(), "bounded failure diagnostic")
		ctx, cancel := context.WithCancel(context.Background())
		if failed {
			cancel()
		}
		defer cancel()
		var runErr error
		e.finish(ctx, &runErr)
		if failed {
			if !errors.Is(runErr, context.Canceled) || result.Status != "failed" {
				t.Fatal("timeout passed", runErr, result)
			}
			raw, err := os.ReadFile(filepath.Join(dir, "result.json"))
			if err != nil || !json.Valid(raw) {
				t.Fatal(err, string(raw))
			}
			if !strings.Contains(diagnostics.String(), dir) {
				t.Fatal("failure path not printed")
			}
			raw, err = os.ReadFile(filepath.Join(dir, "diagnostics.log"))
			if err != nil || !strings.Contains(string(raw), "bounded failure") {
				t.Fatal(err, string(raw))
			}
		} else {
			if runErr != nil || result.Status != "passed" {
				t.Fatal(runErr, result)
			}
			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Fatal("success resources retained", err)
			}
		}
	}
}

func TestJourneyPreflightFailureHasReproducibleReceipt(t *testing.T) {
	// The fake Docker CLI makes this a real preflight failure without starting a
	// container. Git and binary hashing still execute against the current tree.
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte("#!/bin/sh\necho fake-docker-preflight >&2\nexit 9\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	result, err := RunSlateDBMinIO(context.Background(), io.Discard)
	if err == nil || result.Status != "failed" || len(result.SourceRevision) != 40 || len(result.Inputs["source-tree"]) != 64 || len(result.Inputs["runner-binary"]) != 64 || result.Environment["go"] == "" {
		t.Fatalf("incomplete failure receipt: %+v %v", result, err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(result.EvidencePath) })
	raw, readErr := os.ReadFile(filepath.Join(result.EvidencePath, "result.json"))
	if readErr != nil || !json.Valid(raw) || !strings.Contains(string(raw), "fake-docker-preflight") {
		t.Fatal(readErr, string(raw))
	}
}

func TestNodeCleanupRetainsLogsAndReportsSaveFailure(t *testing.T) {
	for _, saveFails := range []bool{false, true} {
		dir := t.TempDir()
		bundle := t.TempDir()
		if saveFails {
			bundle = filepath.Join(bundle, "missing")
		}
		p := &nodeProcess{lifecycle: testProcess(t, "wait"), output: new(synchronizedBuffer), directory: dir, evidence: &journeyEvidence{directory: bundle}}
		_, _ = io.WriteString(p.output, "last node failure diagnostic")
		err := p.stop()
		if saveFails {
			if err == nil || p.stop() == nil {
				t.Fatal("failure saving node diagnostics was discarded")
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(filepath.Join(bundle, filepath.Base(dir)+".log"))
			if err != nil || string(raw) != "last node failure diagnostic" {
				t.Fatal(err, string(raw))
			}
		}
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatal("local node state survived cleanup", err)
		}
	}
}
