// xenon-build is the dependency-free Go bootstrap for a local Xenon binary.
// It exists outside cmd/xenon so preparing the native library cannot require
// that library to be linked already.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

type pins struct {
	Schema         int    `json:"schema"`
	SourceURL      string `json:"source_url"`
	SourceCommit   string `json:"source_commit"`
	GoModule       string `json:"go_module"`
	GoVersion      string `json:"go_version"`
	RustToolchain  string `json:"rust_toolchain"`
	GoToolchain    string `json:"go_toolchain"`
	RuntimeThreads string `json:"runtime_threads"`
}

type buildState struct {
	Schema            int    `json:"schema"`
	NativeFingerprint string `json:"native_fingerprint"`
	NativeCommit      string `json:"native_commit"`
	NativeSHA256      string `json:"native_sha256"`
	BinaryFingerprint string `json:"binary_fingerprint"`
	BinarySHA256      string `json:"binary_sha256"`
	SourceRevision    string `json:"source_revision"`
}

type moduleDownload struct{ Dir, Version, Sum string }

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	if err := build(ctx, "."); err != nil {
		fmt.Fprintln(os.Stderr, "xenon build:", err)
		os.Exit(1)
	}
}

func build(ctx context.Context, root string) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	p, rawPins, err := readPins(filepath.Join(root, "tools/slatedb-native.json"))
	if err != nil {
		return err
	}
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		return fmt.Errorf("unsupported native build platform %s", runtime.GOOS)
	}
	local := filepath.Join(root, ".local")
	source, target, bin := filepath.Join(local, "slatedb-native-source"), filepath.Join(local, "slatedb-native-target"), filepath.Join(local, "bin")
	if err = os.MkdirAll(bin, 0755); err != nil {
		return err
	}
	env := environment(p, target)
	if err = prepareSource(ctx, source, p, env); err != nil {
		return err
	}
	module, err := downloadModule(ctx, root, p, env)
	if err != nil {
		return err
	}
	if err = verifyBindings(source, module.Dir); err != nil {
		return err
	}
	rustc, err := output(ctx, root, env, "rustc", "+"+p.RustToolchain, "-vV")
	if err != nil {
		return err
	}
	nativeFingerprint, err := hashFiles([]byte(runtime.GOOS+"\x00"+runtime.GOARCH+"\x00"+rustc+"\x00"+string(rawPins)), source, []string{"Cargo.lock", "bindings/go/uniffi/slatedb.go", "bindings/go/uniffi/slatedb.h", "bindings/go/uniffi/cgo_flags.go"})
	if err != nil {
		return err
	}
	library := filepath.Join(target, "debug", nativeLibraryName())
	statePath := filepath.Join(local, "xenon-build.json")
	state := readState(statePath)
	nativeWarm := state.NativeFingerprint == nativeFingerprint && state.NativeCommit == p.SourceCommit && fileHashEquals(library, state.NativeSHA256)
	if !nativeWarm {
		fmt.Println("Building pinned SlateDB native library...")
		if err = run(ctx, source, env, "cargo", "+"+p.RustToolchain, "build", "--locked", "-p", "slatedb-uniffi"); err != nil {
			return err
		}
	}
	nativeSHA, err := fileHash(library)
	if err != nil {
		return fmt.Errorf("native library: %w", err)
	}
	installedLibrary := filepath.Join(bin, nativeLibraryName())
	if !fileHashEquals(installedLibrary, nativeSHA) {
		if err = copyFile(library, installedLibrary, 0755); err != nil {
			return err
		}
	}
	sourceRevision, err := output(ctx, root, env, "git", "rev-parse", "HEAD")
	if err != nil {
		return err
	}
	binaryFingerprint, err := sourceFingerprint(root, nativeSHA)
	if err != nil {
		return err
	}
	binary := filepath.Join(bin, "xenon")
	binaryWarm := nativeWarm && state.BinaryFingerprint == binaryFingerprint && state.SourceRevision == sourceRevision && fileHashEquals(binary, state.BinarySHA256)
	if !binaryWarm {
		fmt.Println("Building Xenon...")
		flags := fmt.Sprintf("-X github.com/0x63616c/xenon/internal/buildinfo.NativeCommit=%s -X github.com/0x63616c/xenon/internal/buildinfo.NativeSHA256=%s -X github.com/0x63616c/xenon/internal/buildinfo.SourceRevision=%s", p.SourceCommit, nativeSHA, sourceRevision)
		buildEnv := append([]string{}, env...)
		if runtime.GOOS == "darwin" {
			buildEnv = setEnv(buildEnv, "CGO_LDFLAGS", "-L"+filepath.Dir(library)+" -Wl,-rpath,@loader_path")
		} else {
			buildEnv = setEnv(buildEnv, "CGO_LDFLAGS", "-L"+filepath.Dir(library)+" -Wl,-rpath,$ORIGIN")
		}
		if err = run(ctx, root, buildEnv, "go", "build", "-buildvcs=true", "-ldflags", flags, "-o", binary, "./cmd/xenon"); err != nil {
			return err
		}
	}
	binarySHA, err := fileHash(binary)
	if err != nil {
		return err
	}
	if err = verifyBinary(ctx, root, env, binary, p.SourceCommit, nativeSHA); err != nil {
		return err
	}
	state = buildState{1, nativeFingerprint, p.SourceCommit, nativeSHA, binaryFingerprint, binarySHA, sourceRevision}
	encoded, _ := json.MarshalIndent(state, "", "  ")
	if err = os.WriteFile(statePath, append(encoded, '\n'), 0644); err != nil {
		return err
	}
	if nativeWarm && binaryWarm {
		fmt.Println("Xenon is up to date:", binary)
	} else {
		fmt.Println("Built Xenon:", binary)
	}
	return nil
}

func readPins(path string) (pins, []byte, error) {
	var p pins
	raw, err := os.ReadFile(path)
	if err != nil {
		return p, nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&p); err != nil {
		return p, nil, fmt.Errorf("native pins: %w", err)
	}
	if decoder.Decode(new(any)) != io.EOF {
		return p, nil, errors.New("native pins contain trailing data")
	}
	if p.Schema != 1 || len(p.SourceCommit) != 40 || p.SourceURL == "" || p.GoModule == "" || p.GoVersion == "" || p.RustToolchain == "" || p.GoToolchain == "" || p.RuntimeThreads == "" {
		return p, nil, errors.New("native pins are incomplete")
	}
	if _, err = hex.DecodeString(p.SourceCommit); err != nil {
		return p, nil, errors.New("native source commit is not hexadecimal")
	}
	return p, raw, nil
}

func environment(p pins, target string) []string {
	env := os.Environ()
	for key, value := range map[string]string{"CARGO_TARGET_DIR": target, "GOTOOLCHAIN": p.GoToolchain, "GOENV": "off", "GOWORK": "off", "GOFLAGS": "-mod=readonly", "CGO_ENABLED": "1", "CGO_LDFLAGS": "-L" + filepath.Join(target, "debug"), "LD_LIBRARY_PATH": filepath.Join(target, "debug"), "DYLD_LIBRARY_PATH": filepath.Join(target, "debug"), "SLATEDB_UNIFFI_RUNTIME_THREADS": p.RuntimeThreads} {
		env = setEnv(env, key, value)
	}
	return env
}
func setEnv(env []string, key, value string) []string {
	prefix := key + "="
	out := make([]string, 0, len(env)+1)
	for _, pair := range env {
		if !strings.HasPrefix(pair, prefix) {
			out = append(out, pair)
		}
	}
	return append(out, prefix+value)
}

func prepareSource(ctx context.Context, source string, p pins, env []string) error {
	if _, err := os.Stat(filepath.Join(source, ".git")); errors.Is(err, os.ErrNotExist) {
		if err = os.MkdirAll(filepath.Dir(source), 0755); err != nil {
			return err
		}
		if err = run(ctx, filepath.Dir(source), env, "git", "init", source); err != nil {
			return err
		}
		if err = run(ctx, source, env, "git", "remote", "add", "origin", p.SourceURL); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	status, err := output(ctx, source, env, "git", "status", "--porcelain=v1", "--untracked-files=all")
	if err != nil {
		return err
	}
	if status != "" {
		return errors.New("native source checkout is dirty")
	}
	head, _ := output(ctx, source, env, "git", "rev-parse", "HEAD")
	if head != p.SourceCommit {
		if err = run(ctx, source, env, "git", "fetch", "--depth=1", "origin", p.SourceCommit); err != nil {
			return err
		}
		if err = run(ctx, source, env, "git", "checkout", "--detach", p.SourceCommit); err != nil {
			return err
		}
	}
	head, err = output(ctx, source, env, "git", "rev-parse", "HEAD")
	if err != nil || head != p.SourceCommit {
		return errors.New("native source revision mismatch")
	}
	for _, path := range cargoConfigs(source) {
		return fmt.Errorf("ambient Cargo configuration is not allowed: %s", path)
	}
	return nil
}
func cargoConfigs(source string) []string {
	var found []string
	for current := source; ; current = filepath.Dir(current) {
		for _, name := range []string{"config", "config.toml"} {
			path := filepath.Join(current, ".cargo", name)
			if info, err := os.Stat(path); err == nil && !info.IsDir() {
				found = append(found, path)
			}
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
	}
	if home, err := os.UserHomeDir(); err == nil {
		for _, name := range []string{"config", "config.toml"} {
			path := filepath.Join(home, ".cargo", name)
			if info, err := os.Stat(path); err == nil && !info.IsDir() {
				found = append(found, path)
			}
		}
	}
	sort.Strings(found)
	return found
}

func downloadModule(ctx context.Context, root string, p pins, env []string) (moduleDownload, error) {
	raw, err := output(ctx, root, env, "go", "mod", "download", "-json", p.GoModule+"@"+p.GoVersion)
	if err != nil {
		return moduleDownload{}, err
	}
	var m moduleDownload
	if json.Unmarshal([]byte(raw), &m) != nil || m.Dir == "" || m.Version != p.GoVersion || m.Sum == "" {
		return m, errors.New("invalid downloaded Go binding")
	}
	return m, nil
}
func verifyBindings(source, module string) error {
	for _, name := range []string{"uniffi/slatedb.go", "uniffi/slatedb.h", "uniffi/cgo_flags.go"} {
		a, err := fileHash(filepath.Join(module, name))
		if err != nil {
			return err
		}
		b, err := fileHash(filepath.Join(source, "bindings/go", name))
		if err != nil {
			return err
		}
		if a != b {
			return fmt.Errorf("published Go binding differs from native source: %s", name)
		}
	}
	return nil
}

func nativeLibraryName() string {
	if runtime.GOOS == "darwin" {
		return "libslatedb_uniffi.dylib"
	}
	if runtime.GOOS == "linux" {
		return "libslatedb_uniffi.so"
	}
	return "libslatedb_uniffi.unsupported"
}
func sourceFingerprint(root, nativeSHA string) (string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		if d.IsDir() && (rel == ".git" || rel == ".local" || strings.HasPrefix(rel, "website/node_modules")) {
			return filepath.SkipDir
		}
		if !d.IsDir() && ((strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go")) || rel == "go.mod" || rel == "go.sum" || rel == "tools/slatedb-native.json") {
			paths = append(paths, rel)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(paths)
	return hashFiles([]byte(nativeSHA), root, paths)
}
func hashFiles(prefix []byte, root string, paths []string) (string, error) {
	hash := sha256.New()
	hash.Write(prefix)
	for _, name := range paths {
		raw, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			return "", err
		}
		hash.Write([]byte("\x00" + name + "\x00"))
		hash.Write(raw)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
func fileHash(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err = io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
func fileHashEquals(path, want string) bool {
	if want == "" {
		return false
	}
	got, err := fileHash(path)
	return err == nil && got == want
}
func copyFile(from, to string, mode fs.FileMode) error {
	raw, err := os.ReadFile(from)
	if err != nil {
		return err
	}
	temporary := to + ".tmp"
	if err = os.WriteFile(temporary, raw, mode); err != nil {
		return err
	}
	return os.Rename(temporary, to)
}
func readState(path string) buildState {
	var state buildState
	raw, err := os.ReadFile(path)
	if err == nil {
		_ = json.Unmarshal(raw, &state)
	}
	return state
}
func verifyBinary(ctx context.Context, root string, env []string, binary, commit, sha string) error {
	raw, err := output(ctx, root, env, binary, "version")
	if err != nil {
		return err
	}
	var value struct {
		SlateDBNative struct {
			SourceCommit   string `json:"source_commit"`
			ArtifactSHA256 string `json:"artifact_sha256"`
		} `json:"slatedb_native"`
	}
	if json.Unmarshal([]byte(raw), &value) != nil || value.SlateDBNative.SourceCommit != commit || value.SlateDBNative.ArtifactSHA256 != sha {
		return errors.New("Xenon binary native attestation mismatch")
	}
	return nil
}
func run(ctx context.Context, dir string, env []string, name string, args ...string) error {
	command := exec.CommandContext(ctx, name, args...)
	command.Dir = dir
	command.Env = env
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}
func output(ctx context.Context, dir string, env []string, name string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, name, args...)
	command.Dir = dir
	command.Env = env
	raw, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s: %w: %s", name, err, strings.TrimSpace(string(raw)))
	}
	return strings.TrimSpace(string(raw)), nil
}
