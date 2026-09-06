//go:build darwin || linux

package simulation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/encoding/protojson"
)

// OmesRuntime is a resident-stack implementation. A caller explicitly supplies
// trusted prepared tools and fixture metadata; none is downloaded or built here.
// The fixture must provide the named Nexus endpoint AND its compatible worker.
// No fault, cold recovery, or full release qualification follows from this driver.
type OmesRuntime struct {
	failureMu                                                          sync.Mutex
	reportFailure                                                      func(error)
	auditedRun                                                         string
	buildPath, buildHash, fixturePath, fixtureHash, oracle, oracleHash string
	build                                                              correctedBuild
	fixture                                                            ResidentTopology
}
type correctedBuild struct {
	API string `json:"api_commit"`
	SDK struct {
		Module   string `json:"module"`
		Version  string `json:"version"`
		Checksum string `json:"checksum"`
	} `json:"effective_worker_sdk"`
	Prepared             bool              `json:"prepared"`
	CompatibilityOverlay bool              `json:"compatibility_overlay"`
	Upstream             string            `json:"upstream_commit"`
	Source               string            `json:"source"`
	Binary               string            `json:"binary"`
	BinarySHA            string            `json:"binary_sha256"`
	SourceSHA            map[string]string `json:"source_sha256"`
	PreparedSHA          map[string]string `json:"prepared_sha256"`
	PatchSHA             string            `json:"patch_sha256"`
}

func NewOmesRuntime(buildPath, fixturePath, oracle, oracleSHA string) (*OmesRuntime, error) {
	var e error
	buildPath, e = filepath.Abs(buildPath)
	if e != nil {
		return nil, e
	}
	fixturePath, e = filepath.Abs(fixturePath)
	if e != nil {
		return nil, e
	}
	oracle, e = filepath.Abs(oracle)
	if e != nil {
		return nil, e
	}
	raw, e := readWorkflowFile(buildPath, 4<<20)
	if e != nil {
		return nil, e
	}
	r := &OmesRuntime{buildPath: buildPath, buildHash: hash(raw), fixturePath: fixturePath, oracle: oracle, oracleHash: oracleSHA}
	if e = json.Unmarshal(raw, &r.build); e != nil {
		return nil, e
	}
	if r.build.API != "d96bd55e87799e9f6a33a1c40a56cfa932566bdf" || r.build.SDK.Module != "go.temporal.io/sdk" || r.build.SDK.Version != "v1.48.0" || r.build.SDK.Checksum != "h1:WDctKDVuh0Z8Nf7euAyqs/EwcPg1JTIIq1Fut8Tq118=" || len(r.build.SourceSHA) > 4096 || len(r.build.PreparedSHA) != 4 || !r.build.Prepared || !r.build.CompatibilityOverlay || r.build.Upstream != "c6978ba39aa03551ce28974117e8d7ecf983d2b3" || r.build.PatchSHA != "cdb70939ac3a6f0449534421dd13579984c69fcb1c5b737c72d744a63b47bc09" || len(r.build.SourceSHA) == 0 || len(r.build.PreparedSHA) == 0 || !digestPattern.MatchString(oracleSHA) {
		return nil, errors.New("unqualified corrected Omes tools")
	}
	if filepath.Clean(r.build.Source) != filepath.Join(filepath.Dir(buildPath), "source") || filepath.Dir(r.build.Binary) != filepath.Dir(buildPath) {
		return nil, errors.New("prepared runtime paths escape declared bundle")
	}
	raw, e = readWorkflowFile(fixturePath, 1<<20)
	if e != nil {
		return nil, e
	}
	r.fixtureHash = hash(raw)
	if e = strictJSON(raw, &r.fixture); e != nil {
		return nil, e
	}
	// Fixture identity is the hash of its exact saved bytes, not a self-referential field.
	if r.fixture.FixtureSHA256 != "" || r.fixture.RunID != "" {
		return nil, errors.New("fixture cannot embed case identity")
	}
	return r, nil
}
func (r *OmesRuntime) Topology() ResidentTopology {
	t := r.fixture
	t.FixtureSHA256 = r.fixtureHash
	return t
}
func checkFile(path, want string) error {
	got, e := hashWorkflowFile(path)
	if e != nil {
		return e
	}
	if got != want {
		return fmt.Errorf("runtime input changed: %s", path)
	}
	return nil
}
func (r *OmesRuntime) checkTools() error {
	if e := checkFile(r.buildPath, r.buildHash); e != nil {
		return e
	}
	if e := checkFile(r.fixturePath, r.fixtureHash); e != nil {
		return e
	}
	if e := checkFile(r.oracle, r.oracleHash); e != nil {
		return e
	}
	if e := checkFile(r.build.Binary, r.build.BinarySHA); e != nil {
		return e
	}
	for _, tree := range []struct {
		root  string
		files map[string]string
	}{{r.build.Source, r.build.SourceSHA}, {filepath.Join(r.build.Source, "workers/go/prepared"), r.build.PreparedSHA}} {
		for name, want := range tree.files {
			if !filepath.IsLocal(name) {
				return errors.New("tool path escapes source")
			}
			if e := checkFile(filepath.Join(tree.root, name), want); e != nil {
				return e
			}
		}
	}
	for _, v := range os.Environ() {
		if strings.HasPrefix(v, "OMES_") {
			return errors.New("ambient OMES overrides forbidden")
		}
	}
	return nil
}
func (r *OmesRuntime) Check(ctx context.Context, t ResidentTopology) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	expected := r.Topology()
	expected.RunID = t.RunID
	if expected != t {
		return errors.New("resident fixture does not match saved topology")
	}
	if e := r.checkTools(); e != nil {
		return e
	}
	c, e := client.DialContext(ctx, client.Options{HostPort: t.Address, Namespace: t.Namespace})
	if e != nil {
		return e
	}
	defer c.Close()
	// No mutation or inferred endpoint binding. Endpoint readiness/result execution
	// is still the external supervisor's job; this verifies the exact named target.
	var token []byte
	for page := 0; page < 100; page++ {
		res, e := c.OperatorService().ListNexusEndpoints(ctx, &operatorservice.ListNexusEndpointsRequest{PageSize: 100, NextPageToken: token})
		if e != nil {
			return e
		}
		for _, ep := range res.Endpoints {
			if ep.GetSpec().GetName() == t.NexusEndpoint {
				w := ep.GetSpec().GetTarget().GetWorker()
				if w == nil || w.Namespace != t.Namespace || w.TaskQueue != t.NexusTaskQueue {
					return errors.New("Nexus target mismatch")
				}
				return nil
			}
		}
		token = res.NextPageToken
		if len(token) == 0 {
			break
		}
	}
	return errors.New("required Nexus endpoint missing")
}
func queueQuery(t ResidentTopology) string { return "TaskQueue = 'omes-" + t.RunID + "'" }
func (r *OmesRuntime) Empty(ctx context.Context, t ResidentTopology) error {
	r.auditedRun = ""
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	c, e := client.DialContext(ctx, client.Options{HostPort: t.Address, Namespace: t.Namespace})
	if e != nil {
		return e
	}
	defer c.Close()
	res, e := c.WorkflowService().CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: t.Namespace, Query: queueQuery(t)})
	if e != nil {
		return e
	}
	if res == nil || res.Count != 0 {
		return errors.New("resident queue already contains executions; fresh fixture required")
	}
	return nil
}
func (r *OmesRuntime) Execute(ctx context.Context, t ResidentTopology, input, path string) error {
	r.auditedRun = ""

	argv := []string{"run-scenario-with-worker", "--scenario", "fuzzer", "--language", "go", "--version", "v1.48.0", "--dir-name", "prepared", "--namespace", t.Namespace, "--server-address", t.Address, "--run-id", t.RunID, "--iterations", "1", "--max-concurrent", "1", "--max-iteration-attempts", "1", "--timeout", "300s", "--graceful-shutdown-duration", "5s", "--option", "nexus-endpoint=" + t.NexusEndpoint, "--option", "input-file=" + input}
	inputHash, e := hashWorkflowFile(input)
	if e != nil {
		return e
	}
	if e := save(filepath.Join(path, "invocation.json"), map[string]any{"argv": argv, "build_sha256": r.buildHash, "oracle_sha256": r.oracleHash, "fixture_sha256": r.fixtureHash, "input_sha256": inputHash}); e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(ctx, 310*time.Second)
	defer cancel()
	r.failureMu.Lock()
	report := r.reportFailure
	r.failureMu.Unlock()
	return runOmesProcess(ctx, r.build.Binary, argv, r.build.Source, filepath.Join(path, "workload.log"), report)
}
func (r *OmesRuntime) Audit(ctx context.Context, t ResidentTopology, path string) (json.RawMessage, error) {
	output := filepath.Join(path, "histories")
	r.failureMu.Lock()
	report := r.reportFailure
	r.failureMu.Unlock()
	if e := runOmesProcess(ctx, r.oracle, []string{"--address", t.Address, "--namespace", t.Namespace, "--omes-run-id", t.RunID, "--minimum-runs", "1", "--output", output}, r.build.Source, filepath.Join(path, "audit.log"), report); e != nil {
		return nil, e
	}
	raw, e := readWorkflowFile(filepath.Join(output, "result.json"), 1<<20)
	if e != nil {
		return nil, e
	}
	var proof struct {
		Schema  int    `json:"schema"`
		Visible int    `json:"visible_runs"`
		Query   string `json:"query"`
		Runs    []struct {
			HistoryFile string `json:"history_file"`
			HistorySHA  string `json:"history_sha256"`
		} `json:"runs"`
	}
	if e = json.Unmarshal(raw, &proof); e != nil {
		return nil, e
	}
	if proof.Schema != 1 || proof.Visible < 1 || proof.Visible != len(proof.Runs) || proof.Query != queueQuery(t) {
		return nil, errors.New("history oracle receipt invalid")
	}
	for _, run := range proof.Runs {
		if !filepath.IsLocal(run.HistoryFile) {
			return nil, errors.New("history path escapes evidence")
		}
		if e = checkFile(filepath.Join(output, run.HistoryFile), run.HistorySHA); e != nil {
			return nil, e
		}
	}
	r.auditedRun = t.RunID
	result, _ := json.Marshal(map[string]any{"event": "resident-workflow-history-verified", "audit_sha256": hash(raw), "visible_runs": proof.Visible, "qualification": "resident-workflow-component; no faults or cold recovery"})
	return result, nil
}
func (r *OmesRuntime) Cleanup(ctx context.Context, t ResidentTopology, path string) (result error) {
	defer func() { result = errors.Join(result, r.checkTools()) }()
	c, e := client.DialContext(ctx, client.Options{HostPort: t.Address, Namespace: t.Namespace})
	if e != nil {
		return e
	}
	defer c.Close()
	query := queueQuery(t) + " AND ExecutionStatus = 'Running'"
	// The fresh queue belongs solely to this case. Repeated first-page reads avoid
	// rebasing a pagination token while termination changes the selected set.
	terminated := 0
	diagnosticBytes := 0
	for round := 0; round < 100; round++ {
		res, e := c.WorkflowService().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{Namespace: t.Namespace, Query: query, PageSize: 100})
		if e != nil {
			return e
		}
		if len(res.Executions) == 0 {
			count, e := c.WorkflowService().CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: t.Namespace, Query: query})
			if e != nil {
				return e
			}
			if count.Count != 0 {
				return errors.New("cleanup list/count disagree")
			}
			if r.auditedRun != t.RunID {
				return errors.New("remote cleanup unverified after failed case: visibility absence is not an exact workflow census; retire/reset external fixture")
			}
			return save(filepath.Join(path, "cleanup.json"), map[string]any{"running": 0, "termination_requests": terminated, "scope": "case queue only; external fixture retained"})
		}
		for _, info := range res.Executions {
			if terminated >= 128 {
				return errors.New("cleanup execution bound exceeded")
			}
			if info.Execution == nil {
				return errors.New("missing cleanup execution")
			}
			// Capture a bounded diagnostic page before termination changes history.
			h, he := c.WorkflowService().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{Namespace: t.Namespace, Execution: info.Execution, MaximumPageSize: 1000})
			if he != nil {
				return he
			}
			if h == nil || h.History == nil {
				return errors.New("missing failure history")
			}
			raw, he := protojson.Marshal(h.History)
			if he != nil {
				return he
			}
			diagnosticBytes += len(raw)
			if len(raw) > 16<<20 || diagnosticBytes > 64<<20 {
				return errors.New("failure history diagnostic exceeds16MiB")
			}
			diagnostic := filepath.Join(path, fmt.Sprintf("unfinished-%04d.json", terminated))
			if he = save(diagnostic, map[string]any{"workflow_id": info.Execution.WorkflowId, "run_id": info.Execution.RunId, "partial": len(h.NextPageToken) > 0, "history": json.RawMessage(raw)}); he != nil {
				return he
			}
			_, e = c.WorkflowService().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{Namespace: t.Namespace, WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: info.Execution.WorkflowId, RunId: info.Execution.RunId}, Reason: "Xenon generated case bounded cleanup"})
			if e != nil {
				return e
			}
			terminated++
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return errors.New("scoped cleanup observation bound exceeded")
}

func (r *OmesRuntime) SetFailureReporter(report func(error)) {
	r.failureMu.Lock()
	defer r.failureMu.Unlock()
	r.reportFailure = report
}

// Provenance returns a copy suitable for the shared pre-launch scenario envelope.
func (r *OmesRuntime) Provenance() map[string]string {
	return map[string]string{"resident_fixture_sha256": r.fixtureHash, "corrected_omes_build_sha256": r.buildHash, "omes_binary_sha256": r.build.BinarySHA, "history_oracle_sha256": r.oracleHash, "worker_sdk": "v1.48.0"}
}
