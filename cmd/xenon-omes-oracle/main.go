// Audit visible Omes runs through the unchanged Temporal API.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type runAudit struct {
	WorkflowID    string         `json:"workflow_id"`
	RunID         string         `json:"run_id"`
	Status        string         `json:"status"`
	NextRun       string         `json:"next_run,omitempty"`
	PreviousRun   string         `json:"previous_run,omitempty"`
	Events        map[string]int `json:"events"`
	HistoryFile   string         `json:"history_file"`
	HistorySHA256 string         `json:"history_sha256"`
}

func inspect(h *historypb.History, status enums.WorkflowExecutionStatus) (map[string]int, string, error) {
	if h == nil || len(h.Events) < 2 {
		return nil, "", fmt.Errorf("missing complete history")
	}
	counts := map[string]int{}
	for i, e := range h.Events {
		if e == nil || e.EventId != int64(i+1) {
			return nil, "", fmt.Errorf("history event gap or duplicate")
		}
		counts[e.EventType.String()]++
	}
	if h.Events[0].EventType != enums.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED || h.Events[0].GetWorkflowExecutionStartedEventAttributes() == nil {
		return nil, "", fmt.Errorf("history does not start at workflow start")
	}
	last := h.Events[len(h.Events)-1]
	switch status {
	case enums.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		if last.EventType != enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED || last.GetWorkflowExecutionCompletedEventAttributes() == nil {
			return nil, "", fmt.Errorf("completed visibility/history mismatch")
		}
		return counts, "", nil
	case enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		a := last.GetWorkflowExecutionContinuedAsNewEventAttributes()
		if last.EventType != enums.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW || a == nil || a.NewExecutionRunId == "" {
			return nil, "", fmt.Errorf("missing continue-as-new successor")
		}
		return counts, a.NewExecutionRunId, nil
	default:
		return nil, "", fmt.Errorf("run is not completed or continued as new: %s", status)
	}
}

func chains(runs []runAudit) error {
	byID := map[string]runAudit{}
	predecessors := map[string]int{}
	for _, r := range runs {
		if r.RunID == "" || r.WorkflowID == "" {
			return fmt.Errorf("empty execution identity")
		}
		if _, exists := byID[r.RunID]; exists {
			return fmt.Errorf("duplicate run identity")
		}
		byID[r.RunID] = r
	}
	for _, r := range runs {
		if r.PreviousRun != "" {
			previous, ok := byID[r.PreviousRun]
			if !ok || previous.WorkflowID != r.WorkflowID || previous.NextRun != r.RunID {
				return fmt.Errorf("missing or inconsistent predecessor")
			}
		}
		if r.NextRun != "" {
			next, ok := byID[r.NextRun]
			if !ok || next.WorkflowID != r.WorkflowID || next.PreviousRun != r.RunID {
				return fmt.Errorf("missing or wrong-workflow successor")
			}
			predecessors[r.NextRun]++
			if predecessors[r.NextRun] > 1 {
				return fmt.Errorf("multiple predecessors")
			}
		}
		seen := map[string]bool{}
		for current := r.RunID; current != ""; current = byID[current].NextRun {
			if seen[current] {
				return fmt.Errorf("cyclic run chain")
			}
			seen[current] = true
		}
	}
	return nil
}

func bounded(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 30*time.Second)
}

func run() error {
	address := flag.String("address", "127.0.0.1:17233", "Temporal frontend")
	namespace := flag.String("namespace", "xenon-ministack", "namespace")
	runID := flag.String("omes-run-id", "", "declared Omes run-id")
	output := flag.String("output", "", "new evidence directory")
	exact := flag.Int("exact-runs", 0, "exact visible run count, zero means minimum only")
	minimum := flag.Int("minimum-runs", 1, "minimum visible run count")
	activity := flag.Bool("require-activity-per-run", false, "require completed activity in each run")
	mixed := flag.Bool("mixed-profile", false, "validate frozen mixed40 history semantics")
	flag.Parse()
	if *mixed && *exact != 0 {
		return fmt.Errorf("mixed captures the entire queue; omit exact-runs and use internal baseline/Nexus counts")
	}
	if *mixed && *runID != "xenon-full-mixed" {
		return fmt.Errorf("mixed profile requires declared run ID")
	}
	if *output == "" || *runID == "" || strings.ContainsAny(*runID, "'\\\n\r") || *minimum < 1 || *minimum > 10000 || *exact < 0 || *exact > 10000 {
		return fmt.Errorf("invalid oracle configuration")
	}
	if err := os.Mkdir(*output, 0700); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	c, err := client.DialContext(ctx, client.Options{HostPort: *address, Namespace: *namespace})
	if err != nil {
		return err
	}
	defer c.Close()
	api := c.WorkflowService()
	query := "TaskQueue = 'omes-" + *runID + "'"
	statuses := map[string]enums.WorkflowExecutionStatus{}
	runs := []runAudit{}
	var token []byte
	seenTokens := map[string]bool{}
	for page := 0; ; page++ {
		if page >= 1000 {
			return fmt.Errorf("visibility pagination bound exceeded")
		}
		call, done := bounded(ctx)
		response, e := api.ListWorkflowExecutions(call, &workflowservice.ListWorkflowExecutionsRequest{Namespace: *namespace, Query: query, PageSize: 100, NextPageToken: token})
		done()
		if e != nil {
			return e
		}
		if response == nil {
			return fmt.Errorf("nil visibility response")
		}
		for _, info := range response.Executions {
			if info == nil || info.Execution == nil || info.Execution.RunId == "" {
				return fmt.Errorf("missing execution")
			}
			if _, exists := statuses[info.Execution.RunId]; exists {
				return fmt.Errorf("duplicate visibility run")
			}
			statuses[info.Execution.RunId] = info.Status
			runs = append(runs, runAudit{WorkflowID: info.Execution.WorkflowId, RunID: info.Execution.RunId, Status: info.Status.String()})
			if len(runs) > 10000 {
				return fmt.Errorf("run bound exceeded")
			}
		}
		token = response.NextPageToken
		if len(token) == 0 {
			break
		}
		if seenTokens[string(token)] {
			return fmt.Errorf("visibility cursor cycle")
		}
		seenTokens[string(token)] = true
	}
	if len(runs) < *minimum || (*exact != 0 && len(runs) != *exact) {
		return fmt.Errorf("visible run count %d violates declared count", len(runs))
	}
	call, done := bounded(ctx)
	count, err := api.CountWorkflowExecutions(call, &workflowservice.CountWorkflowExecutionsRequest{Namespace: *namespace, Query: query})
	done()
	if err != nil {
		return err
	}
	if count == nil || count.Count != int64(len(runs)) {
		return fmt.Errorf("list/count mismatch")
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].RunID < runs[j].RunID })
	histories := map[string]*historypb.History{}
	totalBytes := 0
	for i := range runs {
		h := &historypb.History{}
		token = nil
		seenTokens = map[string]bool{}
		size := 0
		for page := 0; ; page++ {
			if page >= 1000 {
				return fmt.Errorf("history pagination bound exceeded")
			}
			call, done := bounded(ctx)
			response, e := api.GetWorkflowExecutionHistory(call, &workflowservice.GetWorkflowExecutionHistoryRequest{Namespace: *namespace, Execution: &commonpb.WorkflowExecution{WorkflowId: runs[i].WorkflowID, RunId: runs[i].RunID}, MaximumPageSize: 1000, NextPageToken: token})
			done()
			if e != nil {
				return e
			}
			if response == nil || response.History == nil {
				return fmt.Errorf("missing history page")
			}
			for _, event := range response.History.Events {
				size += proto.Size(event)
				if size > 16*1024*1024 || len(h.Events) >= 100000 {
					return fmt.Errorf("history size bound exceeded")
				}
				h.Events = append(h.Events, event)
			}
			token = response.NextPageToken
			if len(token) == 0 {
				break
			}
			if seenTokens[string(token)] {
				return fmt.Errorf("history cursor cycle")
			}
			seenTokens[string(token)] = true
		}
		raw, e := protojson.Marshal(h)
		if e != nil {
			return e
		}
		totalBytes += len(raw)
		if totalBytes > 256*1024*1024 {
			return fmt.Errorf("total history evidence bound exceeded")
		}
		runs[i].HistoryFile = fmt.Sprintf("history-%05d.json", i)
		runs[i].HistorySHA256 = fmt.Sprintf("%x", sha256.Sum256(raw))
		if e = os.WriteFile(filepath.Join(*output, runs[i].HistoryFile), raw, 0600); e != nil {
			return e
		}
		histories[runs[i].RunID] = h
		if *mixed {
			runs[i].Events, runs[i].NextRun, err = inspectMixed(h, statuses[runs[i].RunID])
		} else {
			runs[i].Events, runs[i].NextRun, err = inspect(h, statuses[runs[i].RunID])
		}
		if err != nil {
			failure, _ := json.MarshalIndent(map[string]any{"schema": 1, "run": runs[i], "error": err.Error()}, "", "  ")
			if writeErr := os.WriteFile(filepath.Join(*output, "failure.json"), failure, 0600); writeErr != nil {
				return fmt.Errorf("%v; failure evidence: %w", err, writeErr)
			}
			return fmt.Errorf("workflow %s run %s history %s: %w", runs[i].WorkflowID, runs[i].RunID, runs[i].HistoryFile, err)
		}
		runs[i].PreviousRun = h.Events[0].GetWorkflowExecutionStartedEventAttributes().GetContinuedExecutionRunId()
		if *activity && runs[i].Events[enums.EVENT_TYPE_ACTIVITY_TASK_COMPLETED.String()] == 0 {
			return fmt.Errorf("run lacks required completed activity")
		}

	}
	if err = childLinks(runs, histories); err != nil {
		return err
	}
	if err = chains(runs); err != nil {
		return err
	}
	if *mixed {
		if err := mixedAllSemantics(runs, histories, *namespace); err != nil {
			return err
		}
	}
	report := map[string]any{"mixed_semantics_checked": *mixed, "mixed_nexus_checked": *mixed, "schema": 1, "full_acceptance": false, "query": query, "runs": runs, "visible_runs": len(runs), "exact_runs": *exact, "minimum_runs": *minimum, "activity_per_run_required": *activity, "scope": "closed visibility set, complete contiguous histories, child-parent links and continue-as-new successor graph; expected generated root graph and semantic result values not checked"}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(*output, "result.json"), raw, 0600)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
