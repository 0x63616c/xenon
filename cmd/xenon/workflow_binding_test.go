package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/0x63616c/xenon/internal/simulation"
)

// Constructor-only metadata fixture: no executable or server is available, so
// binding and its per-member pin check cannot accidentally exercise a runtime.
func TestResidentBindingRejectsChangedMemberBuild(t *testing.T) {
	dir := t.TempDir()
	hash := strings.Repeat("a", 64)
	build := map[string]any{
		"api_commit":           "d96bd55e87799e9f6a33a1c40a56cfa932566bdf",
		"upstream_commit":      "c6978ba39aa03551ce28974117e8d7ecf983d2b3",
		"patch_sha256":         "cdb70939ac3a6f0449534421dd13579984c69fcb1c5b737c72d744a63b47bc09",
		"effective_worker_sdk": map[string]string{"module": "go.temporal.io/sdk", "version": "v1.48.0", "checksum": "h1:WDctKDVuh0Z8Nf7euAyqs/EwcPg1JTIIq1Fut8Tq118="},
		"prepared":             true, "compatibility_overlay": true,
		"source": filepath.Join(dir, "source"), "binary": filepath.Join(dir, "omes"),
		"binary_sha256": hash, "source_sha256": map[string]string{"source.go": hash},
		"prepared_sha256": map[string]string{"a": hash, "b": hash, "c": hash, "d": hash},
	}
	write := func(path string, value any) {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	buildPath, fixturePath := filepath.Join(dir, "build.json"), filepath.Join(dir, "fixture.json")
	write(buildPath, build)
	write(fixturePath, simulation.ResidentTopology{Version: 1, Address: "unused.invalid:7233", Namespace: "test", NexusEndpoint: "xenon-fuzz", NexusTaskQueue: "omes-xenon-ministack-fuzz"})
	f := residentFlags{build: buildPath, fixture: fixturePath, oracle: filepath.Join(dir, "missing-oracle"), oracleSHA: hash, logs: filepath.Join(dir, "runtime")}
	r := &simulation.Runner{Directory: filepath.Join(dir, "shared"), Provenance: simulation.Provenance{Versions: map[string]string{}}}
	if _, err := f.bind(r); err != nil {
		t.Fatal(err)
	}
	d := r.Driver.(*simulation.BatchWorkflowDriver)
	if _, err := d.NewRuntime(); err != nil {
		t.Fatalf("unchanged pins rejected: %v", err)
	}
	before := r.Provenance.Versions["corrected_omes_build_sha256"]
	build["binary_sha256"] = strings.Repeat("b", 64)
	write(buildPath, build)
	// The replacement is independently valid metadata, but is not the bundle
	// recorded in this run's initial provenance.
	if _, err := simulation.NewOmesRuntime(f.build, f.fixture, f.oracle, f.oracleSHA); err != nil {
		t.Fatalf("replacement fixture invalid: %v", err)
	}
	if _, err := d.NewRuntime(); err == nil || !strings.Contains(err.Error(), "changed after initial provenance binding") {
		t.Fatalf("changed member build accepted: %v", err)
	}
	if r.Provenance.Versions["corrected_omes_build_sha256"] != before {
		t.Fatal("initial receipt changed")
	}
	if _, err := os.Stat(f.logs); !os.IsNotExist(err) {
		t.Fatalf("runtime evidence created during binding: %v", err)
	}
}
