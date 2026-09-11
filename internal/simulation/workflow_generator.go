package simulation

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

//go:embed workflow_normalizer.go.txt
var workflowInspectorSource []byte

const WorkflowInputKind = "omes-corrected-workflow-input-v1"
const omesGeneratorCommit = "c6978ba39aa03551ce28974117e8d7ecf983d2b3"
const omesGeneratorConfig = "8a118eb7c1a86135e3e21b3799f980418b94838f986d922c1352b8eb4d735ea8"

type WorkflowTool struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}
type WorkflowGeneratorBundle struct {
	Version                    int                     `json:"version"`
	OmesCommit                 string                  `json:"omes_commit"`
	APICommit                  string                  `json:"api_commit"`
	GeneratorSourceSHA256      string                  `json:"generator_source_sha256"`
	CargoLockSHA256            string                  `json:"cargo_lock_sha256"`
	NormalizerSourceSHA256     string                  `json:"normalizer_source_sha256"`
	InspectorSourceSHA256      string                  `json:"inspector_source_sha256"`
	CompatibilityOverlaySHA256 string                  `json:"compatibility_overlay_sha256"`
	WorkerSDK                  string                  `json:"worker_sdk"`
	WorkerSHA256               string                  `json:"worker_sha256"`
	Tools                      map[string]WorkflowTool `json:"tools"`
}
type GeneratedWorkflow struct {
	Input        []byte                  `json:"input"`
	SHA256       string                  `json:"sha256"`
	Operations   int                     `json:"operations"`
	Depth        int                     `json:"depth"`
	WorkloadSeed uint64                  `json:"workload_seed"`
	Intent       GeneratedWorkflowIntent `json:"intent"`
	IntentSHA256 string                  `json:"intent_sha256"`
}

// WorkflowEffectCounts keeps execution kinds separate. In particular, an async
// Nexus operation contributes both an operation and a handler workflow.
type WorkflowEffectCounts struct {
	Roots           int `json:"roots"`
	Children        int `json:"children"`
	Continuations   int `json:"continuations"`
	Activities      int `json:"activities"`
	NexusOperations int `json:"nexus_operations"`
	NexusHandlers   int `json:"nexus_handlers"`
}

type WorkflowFanout struct {
	Children   int `json:"children"`
	Activities int `json:"activities"`
	Nexus      int `json:"nexus"`
}

// GeneratedWorkflowNode is a logical, pre-dispatch execution node. Runtime run
// IDs are deliberately absent and are bound only by the independent history
// checker. Empty Omes child workflow IDs therefore remain unambiguous.
type GeneratedWorkflowNode struct {
	ID              string   `json:"id"`
	Kind            string   `json:"kind"`
	Parent          string   `json:"parent,omitempty"`
	Previous        string   `json:"previous,omitempty"`
	InputSHA256     string   `json:"input_sha256"`
	Children        []string `json:"children,omitempty"`
	Next            string   `json:"next,omitempty"`
	Activities      int      `json:"activities"`
	NexusOperations int      `json:"nexus_operations"`
	NexusHandlers   []string `json:"nexus_handlers,omitempty"`
	Terminal        string   `json:"terminal"`
	ResultSHA256    string   `json:"result_sha256,omitempty"`
	ResultString    string   `json:"result_string,omitempty"`
}

// GeneratedWorkflowIntent is emitted by the pinned generator-side inspector
// from the exact normalized protobuf. ExpandedInput preserves the complete
// action tree for the independent real-history oracle; Nodes is its execution
// inventory and Counts/Limits make the three fan-out classes explicit.
type GeneratedWorkflowIntent struct {
	Schema        int                     `json:"schema"`
	InputSHA256   string                  `json:"input_sha256"`
	ExpandedInput json.RawMessage         `json:"expanded_input"`
	Nodes         []GeneratedWorkflowNode `json:"nodes"`
	Counts        WorkflowEffectCounts    `json:"counts"`
	MaxFanout     WorkflowFanout          `json:"max_fanout"`
	Limits        WorkflowFanout          `json:"limits"`
}

// WorkflowGenerator prepares fresh inputs only. It invokes pinned leaf generator
// tools, never Temporal, workers, storage engines or a live fault scheduler.
type WorkflowGenerator struct {
	directory string
	bundle    WorkflowGeneratorBundle
	info      GeneratorInfo
}

func NewWorkflowGenerator(directory string) (*WorkflowGenerator, error) {
	directory, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	directory, err = filepath.EvalSymlinks(directory)
	if err != nil {
		return nil, err
	}
	raw, err := readWorkflowFile(filepath.Join(directory, "generator.json"), 1<<20)
	if err != nil {
		return nil, err
	}
	if len(raw) > 1<<20 {
		return nil, errors.New("generator manifest exceeds 1 MiB")
	}
	var bundle WorkflowGeneratorBundle
	if err = strictJSON(raw, &bundle); err != nil {
		return nil, err
	}
	if bundle.Version != 1 || bundle.OmesCommit != omesGeneratorCommit || bundle.APICommit != "d96bd55e87799e9f6a33a1c40a56cfa932566bdf" || bundle.GeneratorSourceSHA256 != "c6333d94427a7bc2b55731cfa9b4f9ff9b7145017ef816a1ae5c4a9ba5c220ca" || bundle.CargoLockSHA256 != "3924e08587393d264993a5dade6e894b620ff44619f15a8d6d2f46d9ee61809a" || bundle.NormalizerSourceSHA256 != "529a56005dafde138773d420cec66a631bb4caadef50427ba575d388f87f7a32" || bundle.WorkerSDK != "v1.48.0" || len(bundle.WorkerSHA256) != 64 || bundle.CompatibilityOverlaySHA256 != "cdb70939ac3a6f0449534421dd13579984c69fcb1c5b737c72d744a63b47bc09" || bundle.InspectorSourceSHA256 != hash(workflowInspectorSource) || len(bundle.Tools) != 4 {
		return nil, errors.New("unsupported Omes generator/worker compatibility bundle")
	}
	g := &WorkflowGenerator{directory: directory, bundle: bundle, info: GeneratorInfo{"omes-seeded-corrected-v1", hash(raw), []string{WorkflowInputKind, "fresh-seeded-inputs", "corrected-signal-contract", "no-fault-exploration", "no-workflow-execution"}}}
	if err = g.verify(); err != nil {
		return nil, err
	}
	return g, nil
}
func (g *WorkflowGenerator) Info() GeneratorInfo {
	out := g.info
	out.Capabilities = append([]string(nil), out.Capabilities...)
	return out
}
func (g *WorkflowGenerator) verify() error {
	manifest, err := readWorkflowFile(filepath.Join(g.directory, "generator.json"), 1<<20)
	if err != nil {
		return err
	}
	if hash(manifest) != g.info.SHA256 {
		return errors.New("generator manifest changed")
	}
	for _, name := range []string{"generator", "normalize", "config", "worker"} {
		tool, ok := g.bundle.Tools[name]
		if !ok || !filepath.IsLocal(tool.Path) || len(tool.SHA256) != 64 {
			return errors.New("missing or unsafe generator tool")
		}
		path := filepath.Join(g.directory, tool.Path)
		actual, err := filepath.EvalSymlinks(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(g.directory, actual)
		if err != nil || !filepath.IsLocal(relative) {
			return errors.New("generator tool escapes bundle")
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		h := sha256.New()
		_, copyErr := io.Copy(h, file)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		if hex.EncodeToString(h.Sum(nil)) != tool.SHA256 {
			return fmt.Errorf("generator %s changed", name)
		}
		if name == "worker" && tool.SHA256 != g.bundle.WorkerSHA256 {
			return errors.New("compatible worker hash mismatch")
		}
		if name == "config" && tool.SHA256 != omesGeneratorConfig {
			return errors.New("unsupported generator configuration")
		}
	}
	return nil
}
func workflowBounds(l WorkloadLimits) error {
	if l.MaxOperations != 2048 || l.MaxDepth != 64 || l.MaxPayloadBytes != 1<<20 || len(l.Features) != 1 || l.Features[0] != WorkflowInputKind {
		return errors.New("unsupported workflow generation bounds/features; require registered 2048 operations/64 depth/1 MiB profile")
	}
	return nil
}

var generatedWorkflowFanoutLimits = WorkflowFanout{Children: 2048, Activities: 2048, Nexus: 2048}

func validateGeneratedIntent(intent GeneratedWorkflowIntent, input []byte) error {
	if intent.Schema != 1 || intent.InputSHA256 != hash(input) || !json.Valid(intent.ExpandedInput) || intent.Limits != generatedWorkflowFanoutLimits {
		return errors.New("invalid generated workflow intent binding")
	}
	if intent.Counts.Roots != 1 || intent.Counts.Children < 0 || intent.Counts.Continuations < 0 || intent.Counts.Activities < 0 || intent.Counts.NexusOperations < 0 || intent.Counts.NexusHandlers < 0 || intent.Counts.NexusHandlers > intent.Counts.NexusOperations ||
		intent.Counts.Children > intent.Limits.Children || intent.Counts.Activities > intent.Limits.Activities || intent.Counts.NexusOperations > intent.Limits.Nexus ||
		intent.MaxFanout.Children < 0 || intent.MaxFanout.Activities < 0 || intent.MaxFanout.Nexus < 0 || intent.MaxFanout.Children > intent.Counts.Children || intent.MaxFanout.Activities > intent.Counts.Activities || intent.MaxFanout.Nexus > intent.Counts.NexusOperations ||
		intent.MaxFanout.Children > intent.Limits.Children || intent.MaxFanout.Activities > intent.Limits.Activities || intent.MaxFanout.Nexus > intent.Limits.Nexus {
		return errors.New("generated workflow intent exceeds explicit limits")
	}
	if len(intent.Nodes) != intent.Counts.Roots+intent.Counts.Children+intent.Counts.Continuations+intent.Counts.NexusHandlers || len(intent.Nodes) > 1+3*2048 {
		return errors.New("generated workflow intent inventory mismatch")
	}
	byID := make(map[string]GeneratedWorkflowNode, len(intent.Nodes))
	observed := WorkflowEffectCounts{}
	for _, node := range intent.Nodes {
		if node.ID == "" || byID[node.ID].ID != "" || !digestPattern.MatchString(node.InputSHA256) || node.Activities < 0 || node.NexusOperations < 0 ||
			(node.ResultSHA256 != "" && !digestPattern.MatchString(node.ResultSHA256)) {
			return errors.New("invalid generated workflow intent node")
		}
		if node.Kind == "nexus-handler" {
			if node.Terminal != "completed-nexus" || node.ResultSHA256 != "" {
				return errors.New("invalid generated Nexus terminal")
			}
		} else if node.Next != "" {
			if node.Terminal != "continued-as-new" {
				return errors.New("invalid generated continuation terminal")
			}
		} else if node.Terminal != "completed" || node.ResultSHA256 == "" {
			return errors.New("invalid generated workflow terminal")
		}
		byID[node.ID] = node
		observed.Activities += node.Activities
		observed.NexusOperations += node.NexusOperations
		observed.NexusHandlers += len(node.NexusHandlers)
		switch node.Kind {
		case "root":
			observed.Roots++
		case "child":
			observed.Children++
		case "continuation":
			observed.Continuations++
		case "nexus-handler":
		default:
			return errors.New("invalid generated workflow intent node kind")
		}
	}
	if observed != intent.Counts {
		return errors.New("generated workflow intent counts mismatch")
	}
	for _, node := range intent.Nodes {
		if node.Kind == "root" {
			if node.Parent != "" || node.Previous != "" {
				return errors.New("invalid generated workflow root")
			}
		} else if node.Kind == "continuation" {
			previous, ok := byID[node.Previous]
			if !ok || previous.Next != node.ID || node.Parent != previous.Parent {
				return errors.New("invalid generated workflow continuation edge")
			}
		} else {
			if _, ok := byID[node.Parent]; !ok {
				return errors.New("invalid generated workflow parent edge")
			}
		}
		refs := map[string]bool{}
		for _, child := range append(append([]string(nil), node.Children...), node.NexusHandlers...) {
			target, ok := byID[child]
			if !ok || target.Parent != node.ID || refs[child] || child == node.ID {
				return errors.New("invalid generated workflow child edge")
			}
			refs[child] = true
		}
		if node.Next != "" {
			if next, ok := byID[node.Next]; !ok || next.Previous != node.ID {
				return errors.New("invalid generated workflow next edge")
			}
		}
	}
	seen := map[string]bool{}
	var walk func(string) error
	walk = func(id string) error {
		if seen[id] {
			return errors.New("cyclic generated workflow intent")
		}
		seen[id] = true
		node := byID[id]
		for _, next := range append(append(append([]string(nil), node.Children...), node.NexusHandlers...), node.Next) {
			if next != "" {
				if err := walk(next); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := walk("root"); err != nil || len(seen) != len(intent.Nodes) {
		return errors.New("generated workflow intent is not one connected graph")
	}
	return nil
}
func (g *WorkflowGenerator) Next(ctx context.Context, r GenerateRequest) (Scenario, error) {
	if err := workflowBounds(r.Limits); err != nil {
		return Scenario{}, err
	}
	if err := ctx.Err(); err != nil {
		return Scenario{}, err
	}
	if err := g.verify(); err != nil {
		return Scenario{}, err
	}
	scratch, err := os.MkdirTemp("", "xenon-workflow-input-")
	if err != nil {
		return Scenario{}, err
	}
	defer os.RemoveAll(scratch)
	raw, err := workflowTool(ctx, filepath.Join(g.directory, g.bundle.Tools["generator"].Path), "generate", "--explicit-seed", strconv.FormatUint(r.WorkloadSeed, 10), "--generator-config-override", filepath.Join(g.directory, g.bundle.Tools["config"].Path), "--nexus-endpoint", "xenon-fuzz")
	if err != nil {
		return Scenario{}, err
	}
	input := filepath.Join(scratch, "original.proto")
	normalized := filepath.Join(scratch, "normalized.proto")
	if err = os.WriteFile(input, raw, 0600); err != nil {
		return Scenario{}, err
	}
	stats, err := workflowTool(ctx, filepath.Join(g.directory, g.bundle.Tools["normalize"].Path), input, normalized)
	if err != nil {
		return Scenario{}, err
	}
	var measured struct {
		Operations int                     `json:"operations"`
		Depth      int                     `json:"depth"`
		Intent     GeneratedWorkflowIntent `json:"intent"`
	}
	if err = strictJSON(stats, &measured); err != nil {
		return Scenario{}, err
	}
	raw, err = readWorkflowFile(normalized, 1<<20)
	if err != nil {
		return Scenario{}, err
	}
	if len(raw) == 0 || len(raw) > r.Limits.MaxPayloadBytes || measured.Operations < 1 || measured.Operations > r.Limits.MaxOperations || measured.Depth < 1 || measured.Depth > r.Limits.MaxDepth {
		return Scenario{}, errors.New("generated input exceeds declared bounds")
	}
	if err = validateGeneratedIntent(measured.Intent, raw); err != nil {
		return Scenario{}, err
	}
	if err = g.verify(); err != nil {
		return Scenario{}, err
	}
	intentRaw, _ := json.Marshal(measured.Intent)
	workload, _ := json.Marshal(GeneratedWorkflow{Input: raw, SHA256: hash(raw), Operations: measured.Operations, Depth: measured.Depth, WorkloadSeed: r.WorkloadSeed, Intent: measured.Intent, IntentSHA256: hash(intentRaw)})
	// The fault stream identity is retained separately, but this generator does
	// not claim to generate/inject faults. A real schedule driver remains required.
	faults, _ := json.Marshal(struct {
		Seed uint64 `json:"seed"`
		Mode string `json:"mode"`
	}{r.FaultSeed, "none; fault exploration not implemented"})
	return Scenario{1, WorkflowInputKind, workload, []byte(`{"version":1,"runtime":"not-started","nexus_endpoint":"xenon-fuzz","sdk":"v1.48.0"}`), faults}, nil
}

type boundedToolOutput struct{ bytes.Buffer }

func (w *boundedToolOutput) Write(p []byte) (int, error) {
	if w.Len()+len(p) > 4<<20 {
		return 0, errors.New("generator tool output exceeds 4 MiB")
	}
	return w.Buffer.Write(p)
}
func workflowTool(parent context.Context, program string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, program, args...)
	var out, diagnostics boundedToolOutput
	command.Stdout = &out
	command.Stderr = &diagnostics
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("workflow input tool: %w: %s", err, diagnostics.String())
	}
	return out.Bytes(), nil
}

// WorkflowPreparationDriver checks saved input artifacts only. It is explicitly
// not a workflow runner: no successful preparation is a runtime correctness pass.
type WorkflowPreparationDriver struct{ prepared bool }

func (d *WorkflowPreparationDriver) Validate(s Scenario, l WorkloadLimits) error {
	if err := workflowBounds(l); err != nil {
		return err
	}
	if !bytes.Equal(s.Topology, []byte(`{"version":1,"runtime":"not-started","nexus_endpoint":"xenon-fuzz","sdk":"v1.48.0"}`)) {
		return errors.New("unsupported workflow topology")
	}
	var faults struct {
		Seed uint64 `json:"seed"`
		Mode string `json:"mode"`
	}
	if err := strictJSON(s.Faults, &faults); err != nil {
		return err
	}
	if faults.Mode != "none; fault exploration not implemented" {
		return errors.New("unsupported workflow fault schedule")
	}
	if s.Version != 1 || s.Kind != WorkflowInputKind {
		return errors.New("unsupported workflow input scenario")
	}
	var input GeneratedWorkflow
	if err := strictJSON(s.Workload, &input); err != nil {
		return err
	}
	intentRaw, _ := json.Marshal(input.Intent)
	if len(input.Input) == 0 || len(input.Input) > l.MaxPayloadBytes || hash(input.Input) != input.SHA256 || input.Operations < 1 || input.Operations > l.MaxOperations || input.Depth < 1 || input.Depth > l.MaxDepth || hash(intentRaw) != input.IntentSHA256 {
		return errors.New("invalid saved workflow input bounds/hash")
	}
	return validateGeneratedIntent(input.Intent, input.Input)
}
func (d *WorkflowPreparationDriver) Run(ctx context.Context, s Scenario, emit func(json.RawMessage) error) error {
	d.prepared = false
	if err := ctx.Err(); err != nil {
		return err
	}
	var input GeneratedWorkflow
	if err := strictJSON(s.Workload, &input); err != nil {
		return err
	}
	raw, _ := json.Marshal(struct{ Event, InputSHA256, Execution string }{"workflow-input-prepared", input.SHA256, "not-executed"})
	if err := emit(raw); err != nil {
		return err
	}
	d.prepared = true
	return nil
}
func (d *WorkflowPreparationDriver) Settle(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !d.prepared {
		return errors.New("workflow input was not prepared")
	}
	return nil
}
func (d *WorkflowPreparationDriver) Cleanup(context.Context) error { return nil }

// ArtifactDriver selects only implemented component/preparation kinds. Real
// workflow execution cannot be silently substituted with this preparation driver.
type ArtifactDriver struct{ selected Driver }

func (d *ArtifactDriver) Validate(s Scenario, l WorkloadLimits) error {
	switch s.Kind {
	case CoupledKind:
		d.selected = &CoupledDriver{}
	case WorkflowInputKind:
		d.selected = &WorkflowPreparationDriver{}
	default:
		return errors.New("unsupported artifact kind")
	}
	return d.selected.Validate(s, l)
}
func (d *ArtifactDriver) Run(ctx context.Context, s Scenario, emit func(json.RawMessage) error) error {
	if d.selected == nil {
		return errors.New("artifact not validated")
	}
	return d.selected.Run(ctx, s, emit)
}
func (d *ArtifactDriver) Settle(ctx context.Context) error {
	if d.selected == nil {
		return errors.New("artifact not validated")
	}
	return d.selected.Settle(ctx)
}
func (d *ArtifactDriver) Cleanup(ctx context.Context) error {
	if d.selected == nil {
		return nil
	}
	return d.selected.Cleanup(ctx)
}

func readWorkflowFile(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, errors.New("workflow artifact file exceeds bound")
	}
	return raw, nil
}
