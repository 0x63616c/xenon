package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/0x63616c/xenon/internal/buildinfo"
	"github.com/spf13/cobra"
)

type profileRequest struct {
	Repository  string `json:"repository"`
	Evidence    string `json:"evidence"`
	Profile     string `json:"profile"`
	Development bool   `json:"development"`
}
type profileResult struct {
	Schema          int    `json:"schema"`
	Profile         string `json:"profile"`
	Status          string `json:"status"`
	Evidence        string `json:"evidence_path"`
	HelperReceipt   string `json:"helper_receipt,omitempty"`
	CleanupVerified bool   `json:"cleanup_verified"`
}
type profileExecutor func(context.Context, profileRequest, io.Writer) (profileResult, error)

func realProfileCommands(execute profileExecutor) []*cobra.Command {
	var commands []*cobra.Command
	for _, entry := range []struct{ name, profile, description string }{
		{"smoke", "smoke", "Run the real local identical-agent smoke profile"},
		{"local-release-ten-minute", "ten-minute", "Run the opt-in real Omes/Nexus ten-minute churn profile"},
	} {
		request := profileRequest{Profile: entry.profile}
		command := &cobra.Command{Use: entry.name, Short: entry.description, Args: cobra.NoArgs,
			Long:              entry.description + ". Requires an explicit clean Xenon source checkout, Python 3, Git, Go/Rust toolchains, C compiler, AWS CLI and Docker Compose with pinned images already available. Uses local scenario ports and S3 emulator resources. See test/scenarios/agent/ten-minute.md.",
			PersistentPreRunE: func(*cobra.Command, []string) error { return nil }}
		command.Flags().StringVar(&request.Repository, "repository", "", "Explicit Xenon source checkout containing scripts and pinned fixtures")
		command.Flags().StringVar(&request.Evidence, "evidence", "", "New evidence directory; parent must exist")
		command.Flags().BoolVar(&request.Development, "development", false, "Permit dirty source and retain development qualification")
		_ = command.MarkFlagRequired("repository")
		_ = command.MarkFlagRequired("evidence")
		command.RunE = func(command *cobra.Command, _ []string) error {
			result, err := execute(command.Context(), request, command.ErrOrStderr())
			if writeErr := json.NewEncoder(command.OutOrStdout()).Encode(result); writeErr != nil {
				return errors.Join(err, writeErr)
			}
			return err
		}
		commands = append(commands, command)
	}
	return commands
}

func runRealProfile(ctx context.Context, request profileRequest, diagnostics io.Writer) (profileResult, error) {
	result := profileResult{Schema: 1, Profile: request.Profile, Status: "failed"}
	if request.Profile != "smoke" && request.Profile != "ten-minute" {
		return result, fmt.Errorf("unknown real profile")
	}
	repository, err := filepath.Abs(request.Repository)
	if err != nil {
		return result, err
	}
	evidence, err := filepath.Abs(request.Evidence)
	if err != nil {
		return result, err
	}
	if request.Repository == "" || request.Evidence == "" {
		return result, fmt.Errorf("explicit repository and evidence paths required")
	}
	request.Repository, request.Evidence = repository, evidence
	result.Evidence = evidence
	if err = os.Mkdir(evidence, 0700); err != nil {
		return result, err
	}
	result.HelperReceipt = filepath.Join(evidence, "run", "result.json")
	finish := func(primary error) (profileResult, error) {
		if err := saveProfileJSON(filepath.Join(evidence, "cli-result.json"), result); err != nil {
			primary = errors.Join(primary, err)
		}
		return result, primary
	}
	if err = saveProfileJSON(filepath.Join(evidence, "cli-request.json"), struct {
		Request profileRequest `json:"request"`
		Build   buildinfo.Info `json:"build"`
	}{request, buildinfo.Read()}); err != nil {
		return finish(err)
	}
	if err = ctx.Err(); err != nil {
		result.Status = "canceled"
		code := 130
		if errors.Is(err, context.DeadlineExceeded) {
			result.Status = "budget"
			code = 2
		}
		result.CleanupVerified = true
		return finish(&commandExit{code, err})
	}
	helper := filepath.Join(repository, "scripts", "agent-smoke.py")
	if info, err := os.Stat(helper); err != nil || !info.Mode().IsRegular() {
		return finish(fmt.Errorf("repository must contain scripts/agent-smoke.py"))
	}
	for _, tool := range []string{"python3", "git", "go", "rustup", "cargo", "cc", "aws", "docker"} {
		if _, err = exec.LookPath(tool); err != nil {
			return finish(fmt.Errorf("required profile tool %s: %w", tool, err))
		}
	}
	source, err := captureProfileSource(ctx, repository, helper)
	if err != nil {
		return finish(err)
	}
	if source.Dirty && !request.Development {
		return finish(errors.New("clean repository required without --development"))
	}
	if err = saveProfileJSON(filepath.Join(evidence, "cli-source.json"), source); err != nil {
		return finish(err)
	}
	python, _ := exec.LookPath("python3")
	argv := []string{helper, "--profile", request.Profile, "--evidence", filepath.Join(evidence, "run")}
	if request.Development {
		argv = append(argv, "--development")
	}
	// This is an outer ceiling, not workload credit. The helper enforces each
	// shorter declared setup/workload/recovery budget and records its first cause.
	limit := 3 * time.Hour
	if request.Profile == "smoke" {
		limit = 20 * time.Minute
	}
	commandCtx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	processErr, forced := superviseProfile(commandCtx, python, argv, repository, filepath.Join(evidence, "helper.log"), diagnostics, 150*time.Second)
	if commandCtx.Err() != nil {
		result.Status = "canceled"
		code := 130
		if errors.Is(commandCtx.Err(), context.DeadlineExceeded) {
			result.Status = "budget"
			code = 2
		}
		result.CleanupVerified = !forced && helperCleanupVerified(result.HelperReceipt)
		if failure := firstProfileFailure(result.HelperReceipt); failure != "" {
			result.Status = "failed"
			return finish(fmt.Errorf("%s: %w", failure, errors.Join(commandCtx.Err(), processErr)))
		}
		if forced {
			processErr = errors.Join(processErr, errors.New("forced helper termination: nested resource cleanup is unverified"))
		}
		return finish(&commandExit{code, errors.Join(commandCtx.Err(), processErr)})
	}
	if processErr != nil {
		result.CleanupVerified = helperCleanupVerified(result.HelperReceipt)
		if failure := firstProfileFailure(result.HelperReceipt); failure != "" {
			processErr = fmt.Errorf("%s: %w", failure, processErr)
		}
		return finish(processErr)
	}
	raw, err := readProfileReceipt(result.HelperReceipt)
	if err != nil {
		return finish(err)
	}
	var receipt struct {
		Schema        int               `json:"schema"`
		Revision      string            `json:"revision"`
		Dirty         *bool             `json:"dirty"`
		Development   *bool             `json:"development"`
		Harness       map[string]string `json:"harness_hashes"`
		Status        string            `json:"status"`
		Profile       string            `json:"profile"`
		Error         string            `json:"error"`
		CleanupErrors []string          `json:"cleanup_errors"`
	}
	if err = json.Unmarshal(raw, &receipt); err != nil {
		return finish(err)
	}
	after, err := captureProfileSource(ctx, repository, helper)
	if err != nil {
		return finish(err)
	}
	if after != source {
		return finish(errors.New("repository or helper changed during profile"))
	}
	if receipt.Schema != 1 || receipt.Revision != source.Revision || receipt.Dirty == nil || *receipt.Dirty != source.Dirty || receipt.Development == nil || *receipt.Development != request.Development || receipt.Harness["agent-smoke.py"] != source.HelperSHA256 {
		return finish(errors.New("helper receipt provenance mismatch or missing fields"))
	}
	if failure := firstProfileFailure(result.HelperReceipt); failure != "" {
		return finish(errors.New(failure))
	}
	if receipt.Profile != request.Profile {
		return finish(errors.New("helper profile receipt mismatch"))
	}
	expectedStatus := "component-passed"
	if request.Development {
		expectedStatus = "development-passed"
	}
	if receipt.Status != expectedStatus || receipt.Error != "" {
		return finish(fmt.Errorf("helper did not pass: %s %s", receipt.Status, receipt.Error))
	}
	if len(receipt.CleanupErrors) != 0 {
		return finish(fmt.Errorf("helper cleanup failed: %v", receipt.CleanupErrors))
	}
	result.Status = receipt.Status
	result.CleanupVerified = true
	return finish(nil)
}
func saveProfileJSON(path string, value any) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err = json.NewEncoder(file).Encode(value); err != nil {
		return err
	}
	return file.Sync()
}
func readProfileReceipt(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, (8<<20)+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > 8<<20 {
		return nil, errors.New("helper receipt exceeds 8 MiB")
	}
	return raw, nil
}
func helperCleanupVerified(path string) bool {
	raw, err := readProfileReceipt(path)
	if err != nil {
		return false
	}
	var value struct {
		Status  string   `json:"status"`
		Cleanup []string `json:"cleanup_errors"`
	}
	return json.Unmarshal(raw, &value) == nil && value.Status != "" && len(value.Cleanup) == 0
}

// The checkout is explicit trusted code, but its execution identity is pinned
// independently of this CLI binary and checked again before accepting a pass.
type profileSource struct {
	Revision     string `json:"revision"`
	Dirty        bool   `json:"dirty"`
	HelperSHA256 string `json:"helper_sha256"`
}

func captureProfileSource(parent context.Context, repository, helper string) (profileSource, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	revision, err := exec.CommandContext(ctx, "git", "-C", repository, "rev-parse", "HEAD").Output()
	if err != nil {
		return profileSource{}, err
	}
	value := strings.TrimSpace(string(revision))
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 20 {
		return profileSource{}, errors.New("repository revision is not a full commit identity")
	}
	dirty, err := exec.CommandContext(ctx, "git", "-C", repository, "status", "--porcelain=v1", "--untracked-files=all").Output()
	if err != nil {
		return profileSource{}, err
	}
	raw, err := readProfileReceipt(helper)
	if err != nil {
		return profileSource{}, err
	}
	digest := sha256.Sum256(raw)
	return profileSource{value, len(strings.TrimSpace(string(dirty))) != 0, hex.EncodeToString(digest[:])}, nil
}
func firstProfileFailure(resultPath string) string {
	for _, path := range []string{filepath.Join(filepath.Dir(resultPath), "first-failure.json"), resultPath} {
		raw, err := readProfileReceipt(path)
		if err != nil {
			continue
		}
		var value struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(raw, &value) == nil && value.Error != "" && !strings.HasPrefix(value.Error, "RuntimeError: scenario interrupted by signal ") {
			return value.Error
		}
	}
	return ""
}
