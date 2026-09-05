package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/0x63616c/xenon/internal/ministack"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	query := flag.String("query", "", "visibility query")
	expectedCount := flag.Int("expected-count", 1, "exact expected visible rows")
	mode := flag.String("mode", "", "bootstrap, worker, start, phase, control or verify")
	address := flag.String("address", "127.0.0.1:17233", "stable Temporal endpoint")
	namespace := flag.String("namespace", "xenon-ministack", "namespace")
	id := flag.String("workflow-id", "xenon-durable-workflow-1", "stable workflow ID")
	queue := flag.String("task-queue", "xenon-ministack-worker", "task queue")
	runID := flag.String("run-id", "", "initial run ID for complete history verification")
	storage := flag.String("storage-address", "127.0.0.1:17935", "stable Xenon ingress")
	output := flag.String("output", "", "existing evidence output directory")
	readinessTimeout := flag.Duration("readiness-timeout", 60*time.Second, "bounded cold storage readiness budget")
	flag.Parse()
	if *mode == "storage-ready" {
		return storageReady(*storage, *readinessTimeout)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c, e := client.DialContext(ctx, client.Options{HostPort: *address, Namespace: *namespace})
	if e != nil {
		return e
	}
	defer c.Close()
	emit := func(value any) error { return json.NewEncoder(os.Stdout).Encode(value) }
	switch *mode {
	case "health":
		_, e = c.CheckHealth(ctx, &client.CheckHealthRequest{})
		if e != nil {
			return e
		}
		return emit(map[string]string{"health": "serving"})
	case "bootstrap":
		_, e = c.WorkflowService().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{Namespace: *namespace, WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour)})
		if _, exists := e.(*serviceerror.NamespaceAlreadyExists); exists {
			e = nil
		}
		if e != nil {
			return e
		}
		if e = seedSearchAttributes(ctx, *storage); e != nil {
			return e
		}
		_, e = c.OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{Namespace: *namespace, SearchAttributes: map[string]enumspb.IndexedValueType{"XenonProof": enumspb.INDEXED_VALUE_TYPE_KEYWORD, "OmesExecutionID": enumspb.INDEXED_VALUE_TYPE_KEYWORD, "KS_Keyword": enumspb.INDEXED_VALUE_TYPE_KEYWORD, "KS_Int": enumspb.INDEXED_VALUE_TYPE_INT}})
		if _, exists := e.(*serviceerror.AlreadyExists); exists {
			e = nil
		}
		if e != nil {
			return e
		}
		description, e := c.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: *namespace})
		if e != nil {
			return e
		}
		shard := common.WorkflowIDToHistoryShard(description.NamespaceInfo.Id, *id, 4)
		return emit(map[string]any{"namespace": *namespace, "namespace_id": description.NamespaceInfo.Id, "history_partition": fmt.Sprintf("history-%d", shard%4)})
	case "visibility-count":
		count, e := c.WorkflowService().CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: *namespace, Query: *query})
		if e != nil {
			return e
		}
		return emit(map[string]int64{"count": count.Count})
	case "visibility":
		count, e := c.WorkflowService().CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: *namespace, Query: *query})
		if e != nil {
			return e
		}
		if count.Count != int64(*expectedCount) {
			return fmt.Errorf("visibility count %d != %d", count.Count, *expectedCount)
		}
		seen := map[string]bool{}
		var token []byte
		for page := 0; ; page++ {
			if page >= 1000 {
				return fmt.Errorf("visibility pagination did not terminate")
			}
			response, e := c.WorkflowService().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{Namespace: *namespace, Query: *query, PageSize: 1, NextPageToken: token})
			if e != nil {
				return e
			}
			for _, execution := range response.Executions {
				key := execution.Execution.WorkflowId + "/" + execution.Execution.RunId
				if seen[key] {
					return fmt.Errorf("duplicate visibility row")
				}
				seen[key] = true
			}
			token = response.NextPageToken
			if len(token) == 0 {
				break
			}
		}
		if len(seen) != *expectedCount {
			return fmt.Errorf("visibility page count %d != %d", len(seen), *expectedCount)
		}
		return emit(map[string]any{"count": count.Count, "executions": seen})
	case "worker":
		w := worker.New(c, *queue, worker.Options{})
		w.RegisterWorkflow(ministack.DurableWorkflow)
		w.RegisterWorkflow(ministack.Child)
		w.RegisterActivity(ministack.RetryActivity)
		if e = w.Start(); e != nil {
			return e
		}
		defer w.Stop()
		if e = emit(map[string]string{"worker": "started"}); e != nil {
			return e
		}
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(stop)
		<-stop
		return nil
	case "start":
		run, e := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: *id, TaskQueue: *queue, WorkflowExecutionTimeout: 10 * time.Minute}, ministack.DurableWorkflow, ministack.Input{})
		if e != nil {
			return e
		}
		return emit(map[string]string{"workflow_id": run.GetID(), "run_id": run.GetRunID()})
	case "phase":
		response, e := c.QueryWorkflow(ctx, *id, "", "phase")
		if e != nil {
			return e
		}
		var phase string
		if e = response.Get(&phase); e != nil {
			return e
		}
		return emit(map[string]string{"phase": phase})
	case "control":
		handle, e := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: *id, UpdateID: *id + "-update", UpdateName: "set-proof-value", Args: []interface{}{"update-ack"}, WaitForStage: client.WorkflowUpdateStageCompleted})
		if e != nil {
			return e
		}
		var value string
		if e = handle.Get(ctx, &value); e != nil {
			return e
		}
		if value != "update-ack" {
			return fmt.Errorf("wrong update result %q", value)
		}
		if e = c.SignalWorkflow(ctx, *id, "", "proof-signal", "signal-ack"); e != nil {
			return e
		}
		return emit(map[string]string{"control": "acknowledged"})
	case "verify":
		if *runID == "" || *output == "" {
			return fmt.Errorf("verify requires initial run ID and output directory")
		}
		if _, e = c.CheckHealth(ctx, &client.CheckHealthRequest{}); e != nil {
			return e
		}
		fmt.Println("VERIFY_CLIENT_CONNECTED")
		var result ministack.Result
		if e = c.GetWorkflow(ctx, *id, *runID).Get(ctx, &result); e != nil {
			return e
		}
		expected := ministack.Result{Attempt: 2, Child: "child-completed", Signal: "signal-ack", Update: "update-ack", Continued: true}
		if result != expected {
			return fmt.Errorf("wrong workflow result: %+v", result)
		}
		counts := map[string]int{}
		next := *runID
		runs := 0
		for next != "" {
			if runs >= 3 {
				return fmt.Errorf("unexpected continued run count")
			}
			current := next
			next = ""
			runs++
			iterator := c.GetWorkflowHistory(ctx, *id, current, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
			events := []json.RawMessage{}
			for iterator.HasNext() {
				event, e := iterator.Next()
				if e != nil {
					return e
				}
				counts[event.EventType.String()]++
				raw, e := protojson.Marshal(event)
				if e != nil {
					return e
				}
				events = append(events, raw)
				if continued := event.GetWorkflowExecutionContinuedAsNewEventAttributes(); continued != nil {
					next = continued.NewExecutionRunId
				}
			}
			data, e := json.MarshalIndent(events, "", "  ")
			if e != nil {
				return e
			}
			if e = os.WriteFile(fmt.Sprintf("%s/history-%d.json", *output, runs), data, 0600); e != nil {
				return e
			}
		}
		for event, minimum := range map[string]int{"WorkflowExecutionContinuedAsNew": 1, "WorkflowExecutionCompleted": 1, "ActivityTaskCompleted": 1, "ChildWorkflowExecutionCompleted": 1, "TimerFired": 2, "WorkflowExecutionSignaled": 1, "WorkflowExecutionUpdateCompleted": 1} {
			if counts[event] < minimum {
				return fmt.Errorf("missing history event %s: counts=%v", event, counts)
			}
		}
		if runs != 2 {
			return fmt.Errorf("expected two runs, got %d", runs)
		}
		return emit(map[string]any{"result": result, "runs": runs, "history_counts": counts})
	default:
		return fmt.Errorf("unknown mode")
	}
}
