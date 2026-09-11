package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// This opt-in acceptance check measures the compiled CLI, including startup and
// cleanup. A median of three warm runs tolerates one noisy host scheduling event.
// It is deliberately separate from ordinary correctness and race tests.
func TestDSTPerformance(t *testing.T) {
	if os.Getenv("XENON_DST_PERFORMANCE") != "1" {
		t.Skip("dedicated check: XENON_DST_PERFORMANCE=1 go test ./cmd/xenon -run '^TestDSTPerformance$' -count=1 -v")
	}
	binary := filepath.Join(t.TempDir(), "xenon")
	build := exec.CommandContext(t.Context(), "go", "build", "-race=false", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("compile CLI: %v\n%s", err, output)
	}
	raw, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	type sample struct {
		ElapsedNS int64            `json:"command_elapsed_ns"`
		Receipt   simulationOutput `json:"receipt"`
	}
	type profile struct {
		Args     []string `json:"args"`
		LimitNS  int64    `json:"limit_ns"`
		MedianNS int64    `json:"median_ns"`
		Passed   bool     `json:"passed"`
		Samples  []sample `json:"samples"`
	}
	report := struct {
		Schema       int       `json:"schema"`
		BinarySHA256 string    `json:"binary_sha256"`
		Profiles     []profile `json:"profiles"`
	}{Schema: 1, BinarySHA256: hex.EncodeToString(digest[:]), Profiles: []profile{
		{Args: []string{"test", "dst"}, LimitNS: int64(10 * time.Second)},
		{Args: []string{"test", "dst", "--seed", "42", "--cases", "1000"}, LimitNS: int64(30 * time.Second)},
	}}
	for i := range report.Profiles {
		p := &report.Profiles[i]
		var elapsed []int64
		for run := 0; run < 4; run++ {
			started := time.Now()
			command := exec.CommandContext(t.Context(), binary, p.Args...)
			output, err := command.CombinedOutput()
			duration := time.Since(started).Nanoseconds()
			if err != nil {
				t.Fatalf("%v: %v\n%s", p.Args, err, output)
			}
			var receipt simulationOutput
			if err := json.Unmarshal(output, &receipt); err != nil {
				t.Fatalf("decode receipt: %v\n%s", err, output)
			}
			if receipt.Result.StopReason != "completed" || receipt.Result.Measurement == nil || receipt.Result.Completed != receipt.Result.Measurement.Cases {
				t.Fatalf("incomplete DST receipt: %+v", receipt)
			}
			if run > 0 { // The first run warms OS/runtime caches; compilation is excluded.
				elapsed = append(elapsed, duration)
				p.Samples = append(p.Samples, sample{duration, receipt})
			}
		}
		p.MedianNS, p.Passed = dstPerformanceWithinBudget(elapsed, p.LimitNS)
		if !p.Passed {
			t.Errorf("%v median %s exceeds strict %s limit", p.Args, time.Duration(p.MedianNS), time.Duration(p.LimitNS))
		}
	}
	receipt, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if path := os.Getenv("XENON_DST_PERFORMANCE_RECEIPT"); path != "" {
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(receipt, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Log(string(receipt))
}

func dstPerformanceWithinBudget(samples []int64, limit int64) (int64, bool) {
	ordered := slices.Clone(samples)
	slices.Sort(ordered)
	median := ordered[len(ordered)/2]
	return median, median < limit
}

func TestDSTPerformanceBudgetRejectsRegression(t *testing.T) {
	// One noisy run is tolerated; a repeatable regression (or equality) fails.
	for _, tc := range []struct {
		samples []int64
		passed  bool
	}{
		{[]int64{1, 12, 9}, true},
		{[]int64{1, 12, 11}, false},
		{[]int64{1, 12, 10}, false},
	} {
		if _, passed := dstPerformanceWithinBudget(tc.samples, 10); passed != tc.passed {
			t.Fatalf("samples %v: passed=%v, want %v", tc.samples, passed, tc.passed)
		}
	}
}
