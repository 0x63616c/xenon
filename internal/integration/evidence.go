package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/0x63616c/xenon/internal/buildinfo"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

// JourneyResult describes the executed inputs as well as the assertion outcome.
// Configuration contains generated inputs; Inputs hashes committed fixtures,
// source files and executables without recording credentials or the environment.
type JourneyResult struct {
	Name           string                     `json:"name"`
	Duration       time.Duration              `json:"duration"`
	Assertions     []string                   `json:"assertions"`
	Status         string                     `json:"status"`
	Error          string                     `json:"error,omitempty"`
	EvidencePath   string                     `json:"evidence_path,omitempty"`
	SourceRevision string                     `json:"source_revision"`
	SourceDirty    bool                       `json:"source_dirty"`
	Inputs         map[string]string          `json:"input_sha256"`
	Configuration  map[string]json.RawMessage `json:"configuration"`
	Environment    map[string]string          `json:"environment"`
	Reproduce      string                     `json:"reproduce"`
}

type journeyEvidence struct {
	directory   string
	started     time.Time
	result      *JourneyResult
	diagnostics io.Writer
	log         *synchronizedBuffer
}

func beginJourney(ctx context.Context, result *JourneyResult, name string, diagnostics io.Writer) (*journeyEvidence, error) {
	if diagnostics == nil {
		diagnostics = io.Discard
	}
	e := &journeyEvidence{started: time.Now(), result: result, diagnostics: diagnostics, log: new(synchronizedBuffer)}
	*result = JourneyResult{Name: name, Status: "failed", Inputs: map[string]string{}, Configuration: map[string]json.RawMessage{}, Environment: map[string]string{"go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH}, Reproduce: "just xenon test integration --only " + name}
	var err error
	e.directory, err = os.MkdirTemp("", "xenon-"+name+"-evidence-")
	if err != nil {
		return e, err
	}
	root, err := repositoryRoot(ctx)
	if err != nil {
		return e, fmt.Errorf("record source provenance: %w", err)
	}
	revision, err := runOutput(ctx, root, nil, "git", "rev-parse", "HEAD")
	if err != nil {
		return e, err
	}
	result.SourceRevision = strings.TrimSpace(revision)
	status, err := runOutput(ctx, root, nil, "git", "status", "--porcelain", "--untracked-files=normal")
	if err != nil {
		return e, err
	}
	result.SourceDirty = status != ""
	files, err := runOutput(ctx, root, nil, "git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", "go.mod", "go.sum", "internal", "cmd", "api", "tools", "test/scenarios/ministack")
	if err != nil {
		return e, err
	}
	source := sha256.New()
	paths := strings.Split(strings.TrimSuffix(files, "\x00"), "\x00")
	sort.Strings(paths)
	for _, path := range paths {
		raw, readErr := os.ReadFile(filepath.Join(root, path))
		if errors.Is(readErr, os.ErrNotExist) {
			fmt.Fprintf(source, "%s\x00deleted\n", path)
			continue
		}
		if readErr != nil {
			return e, readErr
		}
		hash := digest(raw)
		fmt.Fprintf(source, "%s\x00%s\n", path, hash)
		if path == "go.mod" || path == "go.sum" || strings.HasPrefix(path, "tools/") || strings.HasPrefix(path, "test/scenarios/ministack/") {
			result.Inputs[path] = hash
		}
	}
	result.Inputs["source-tree"] = hex.EncodeToString(source.Sum(nil))
	if err = e.config("runner-build", buildinfo.Read()); err != nil {
		return e, err
	}
	binary, err := os.Executable()
	if err != nil {
		return e, err
	}
	if err = e.file("runner-binary", binary); err != nil {
		return e, err
	}
	versionCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	version, versionErr := runOutput(versionCtx, root, nil, "docker", "version", "--format", "{{json .}}")
	if versionErr != nil {
		result.Environment["docker"] = "unavailable: " + versionErr.Error()
	} else {
		result.Environment["docker"] = strings.TrimSpace(version)
	}
	if err = e.config("minio-image", minioImage); err != nil {
		return e, err
	}
	return e, nil
}

func digest(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func (e *journeyEvidence) file(name, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	hash := sha256.New()
	if _, err = io.Copy(hash, f); err != nil {
		return err
	}
	e.result.Inputs[name] = hex.EncodeToString(hash.Sum(nil))
	return nil
}
func (e *journeyEvidence) config(name string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	e.result.Configuration[name] = raw
	e.result.Inputs[name] = digest(raw)
	return nil
}
func (e *journeyEvidence) writer() io.Writer { return io.MultiWriter(e.diagnostics, e.log) }
func (e *journeyEvidence) save(name string, raw []byte) error {
	if len(raw) > diagnosticLimit {
		raw = raw[len(raw)-diagnosticLimit:]
	}
	return os.WriteFile(filepath.Join(e.directory, name), raw, 0600)
}
func (e *journeyEvidence) finish(ctx context.Context, runErr *error) {
	*runErr = errors.Join(*runErr, ctx.Err())
	e.result.Duration = time.Since(e.started)
	if *runErr == nil {
		if err := os.RemoveAll(e.directory); err == nil {
			e.result.Status = "passed"
			return
		} else {
			*runErr = fmt.Errorf("remove journey resources: %w", err)
		}
	}
	if e.directory == "" {
		e.result.Error = (*runErr).Error()
		return
	}
	e.result.Status, e.result.Error, e.result.EvidencePath = "failed", (*runErr).Error(), e.directory
	*runErr = errors.Join(*runErr, e.save("diagnostics.log", []byte(e.log.String())))
	e.result.Error = (*runErr).Error()
	raw, err := json.MarshalIndent(e.result, "", "  ")
	if err == nil {
		err = os.WriteFile(filepath.Join(e.directory, "result.json"), raw, 0600)
	}
	*runErr = errors.Join(*runErr, err)
	fmt.Fprintf(e.diagnostics, "Integration failure evidence: %s\n", e.directory)
}

// outputCommand bounds both the captured diagnostics and time spent waiting on
// inherited stdout/stderr descriptors after the direct child exits.
func outputCommand(cmd *exec.Cmd) ([]byte, error) {
	buffer := new(synchronizedBuffer)
	cmd.Stdout, cmd.Stderr, cmd.WaitDelay = buffer, buffer, time.Second
	err := cmd.Run()
	return []byte(buffer.String()), err
}
