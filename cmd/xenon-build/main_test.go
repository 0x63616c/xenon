package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadPinsIsStrict(t *testing.T) {
	valid := `{"schema":1,"source_url":"https://example.test/repo.git","source_commit":"3fb9e8abab0c9f5833f0c154140ceef009fea02a","go_module":"example.test/binding","go_version":"v1.2.3","rust_toolchain":"1.94.0","go_toolchain":"go1.27.1","runtime_threads":"2"}`
	path := filepath.Join(t.TempDir(), "pins.json")
	if err := os.WriteFile(path, []byte(valid), 0600); err != nil {
		t.Fatal(err)
	}
	if p, raw, err := readPins(path); err != nil || p.Schema != 1 || len(raw) != len(valid) {
		t.Fatal(p, err)
	}
	for name, invalid := range map[string]string{"unknown": valid[:len(valid)-1] + `,"extra":true}`, "trailing": valid + ` {}`, "commit": `{"schema":1}`} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(invalid), 0600); err != nil {
				t.Fatal(err)
			}
			if _, _, err := readPins(path); err == nil {
				t.Fatal("invalid pins accepted")
			}
		})
	}
}

func TestXenonBuildUsesSlateDBProductionTag(t *testing.T) {
	args := xenonBuildArgs("metadata", "/tmp/xenon")
	want := []string{"build", "-buildvcs=true", "-tags=slatedb", "-ldflags", "metadata", "-o", "/tmp/xenon", "./cmd/xenon"}
	if len(args) != len(want) {
		t.Fatalf("build args = %q, want %q", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Fatalf("build args = %q, want %q", args, want)
		}
	}
}

func TestHashFilesBindsNamesOrderAndContent(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("same"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "b"), []byte("same"), 0600); err != nil {
		t.Fatal(err)
	}
	a, err := hashFiles([]byte("pin"), root, []string{"a", "b"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := hashFiles([]byte("pin"), root, []string{"b", "a"})
	c, _ := hashFiles([]byte("other"), root, []string{"a", "b"})
	if a == b || a == c {
		t.Fatal("fingerprint omitted order or pin")
	}
	if err = os.WriteFile(filepath.Join(root, "b"), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	d, _ := hashFiles([]byte("pin"), root, []string{"a", "b"})
	if a == d {
		t.Fatal("fingerprint omitted content")
	}
}

func TestCargoConfigPreflightFindsAncestor(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "checkout", "source")
	if err := os.MkdirAll(source, 0755); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(root, ".cargo", "config.toml")
	if err := os.MkdirAll(filepath.Dir(config), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config, []byte("[build]"), 0600); err != nil {
		t.Fatal(err)
	}
	found := cargoConfigs(source)
	present := false
	for _, path := range found {
		if path == config {
			present = true
		}
	}
	if !present {
		t.Fatalf("ancestor config not found: %v", found)
	}
}
