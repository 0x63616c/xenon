package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"github.com/0x63616c/xenon/internal/proof/s3meter"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestMeterCLIHelper(t *testing.T) {
	if os.Getenv("XENON_METER_HELPER") != "1" {
		t.Skip("child process only")
	}
	flag.CommandLine = flag.NewFlagSet("meter", flag.ExitOnError)
	os.Args = []string{"meter", "-config", os.Getenv("XENON_METER_CONFIG")}
	if e := run(); e != nil {
		t.Fatal(e)
	}
	os.Exit(0)
}
func TestMeterCLI(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("control")) }))
	defer upstream.Close()
	path := filepath.Join(t.TempDir(), "config.json")
	b, _ := json.Marshal(config{Listen: "127.0.0.1:0", ReportListen: "127.0.0.1:0", Target: upstream.URL})
	if e := os.WriteFile(path, b, 0600); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMeterCLIHelper$")
	child.Env = append(os.Environ(), "XENON_METER_HELPER=1", "XENON_METER_CONFIG="+path)
	stdout, e := child.StdoutPipe()
	if e != nil {
		t.Fatal(e)
	}
	child.Stderr = os.Stderr
	if e = child.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	lines := make(chan string, 4)
	go func() {
		defer close(lines)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			select {
			case lines <- scanner.Text():
			case <-ctx.Done():
				return
			}
		}
	}()
	line := func() string {
		t.Helper()
		select {
		case s, ok := <-lines:
			if !ok {
				t.Fatal("meter exited before event")
			}
			return s
		case <-ctx.Done():
			t.Fatal("meter event deadline")
		}
		return ""
	}
	var ready struct {
		Event        string
		Listen       string
		ReportListen string `json:"report_listen"`
	}
	if e = json.Unmarshal([]byte(line()), &ready); e != nil || ready.Event != "ready" {
		t.Fatal("invalid ready event", e)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	response, e := client.Get("http://" + ready.Listen + "/object")
	if e != nil {
		t.Fatal(e)
	}
	got, e := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if e != nil || string(got) != "control" {
		t.Fatal("CLI request failed", e)
	}
	for {
		response, e = client.Get("http://" + ready.ReportListen + "/report")
		if e != nil {
			t.Fatal(e)
		}
		var report s3meter.Report
		e = json.NewDecoder(response.Body).Decode(&report)
		_ = response.Body.Close()
		if e != nil {
			t.Fatal(e)
		}
		if report.Inflight == 0 {
			if report.Attempts != 1 || report.Completed != 1 || report.ResponseBodyBytes != 7 {
				t.Fatal("invalid report", report)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("drain deadline")
		case <-time.After(time.Millisecond):
		}
	}
	if e = child.Process.Signal(syscall.SIGTERM); e != nil {
		t.Fatal(e)
	}
	var final s3meter.Report
	if e = json.Unmarshal([]byte(line()), &final); e != nil || final.Inflight != 0 || final.Attempts != 1 || final.Completed != 1 {
		t.Fatal("invalid final report", e, final)
	}
	if e = child.Wait(); e != nil {
		t.Fatal(e)
	}
}
