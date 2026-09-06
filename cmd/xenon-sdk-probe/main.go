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
	nexuspb "go.temporal.io/api/nexus/v1"
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
	cluster := flag.String("cluster", "active", "Temporal cluster identity for search-slot bootstrap")
	expectedCount := flag.Int("expected-count", 1, "exact expected visible rows")
	mode := flag.String("mode", "", "bootstrap, schema-ready, worker, start, phase, control or verify")
	address := flag.String("address", "127.0.0.1:17233", "stable Temporal endpoint")
	namespace := flag.String("namespace", "xenon-ministack", "namespace")
	id := flag.String("workflow-id", "xenon-durable-workflow-1", "stable workflow ID")
	queue := flag.String("task-queue", "xenon-ministack-worker", "task queue")
	runID := flag.String("run-id", "", "initial run ID for complete history verification")
	storage := flag.String("storage-address", "127.0.0.1:17935", "stable Xenon ingress")
	storagePartition := flag.String("storage-partition", "global", "logical partition for low-level routed readiness")
	output := flag.String("output", "", "existing evidence output directory")
	readinessTimeout := flag.Duration("readiness-timeout", 60*time.Second, "bounded storage/schema readiness budget")
	flag.Parse()
	if *mode == "storage-ready" {
		return storageReady(*storage, *readinessTimeout)
	}
	if *mode == "partition-ready" {
		return partitionReady(*storage, *storagePartition, *readinessTimeout)
	}
	budget := 5 * time.Minute
	if *mode == "schema-ready" || *mode == "failure-history" {
		if *readinessTimeout <= 0 {
			return fmt.Errorf("schema readiness requires a positive budget")
		}
		budget = *readinessTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	c, e := client.DialContext(ctx, client.Options{HostPort: *address, Namespace: *namespace})
	if e != nil {
		return e
	}
	defer c.Close()
	emit := func(value any) error { return json.NewEncoder(os.Stdout).Encode(value) }
	switch *mode {
	case "failure-history":
		return captureFailureHistory(ctx, c.WorkflowService(), *namespace, *id, *runID, emit)
	case "schema-ready":
		result, err := schemaReadiness(ctx, c.OperatorService(), c.WorkflowService(), *namespace)
		if err != nil {
			return err
		}
		return emit(result)
	case "movement-identity":
		description, err := c.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: *namespace})
		if err != nil {
			return err
		}
		if description == nil || description.NamespaceInfo == nil {
			return fmt.Errorf("missing namespace")
		}
		for i := 0; i < 10000; i++ {
			candidate := fmt.Sprintf("xenon-visibility-movement-%d", i)
			if common.WorkflowIDToHistoryShard(description.NamespaceInfo.Id, candidate, 4) == 4 {
				return emit(map[string]any{"workflow_id": candidate, "history_shard": 4, "history_partition": "history-0"})
			}
		}
		return fmt.Errorf("no movement identity found")
	case "mixed-inventory":
		result, err := mixedInventory(ctx, c.WorkflowService(), *namespace)
		if err != nil {
			return err
		}
		return emit(result)
	case "fuzz-endpoint-ready":
		result, err := nexusReadiness(ctx, c, *namespace)
		if err != nil {
			return err
		}
		return emit(result)
	case "fuzz-endpoint":
		response, err := c.OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{Spec: &nexuspb.EndpointSpec{Name: "xenon-fuzz", Target: &nexuspb.EndpointTarget{Variant: &nexuspb.EndpointTarget_Worker_{Worker: &nexuspb.EndpointTarget_Worker{Namespace: *namespace, TaskQueue: "omes-xenon-ministack-fuzz"}}}}})
		if err != nil {
			return err
		}
		if response == nil || response.Endpoint == nil {
			return fmt.Errorf("missing created Nexus endpoint")
		}
		return emit(map[string]any{"endpoint_id": response.Endpoint.Id, "endpoint": "xenon-fuzz"})
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
		if e = seedClusterSearchAttributes(ctx, *storage, *cluster); e != nil {
			return e
		}
		_, e = c.OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{Namespace: *namespace, SearchAttributes: requiredWorkloadSchema()})
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
		fmt.Fprintf(os.Stderr, "VISIBILITY_COUNT count=%d expected=%d\n", count.Count, *expectedCount)
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
			fmt.Fprintf(os.Stderr, "VISIBILITY_PAGE page=%d rows=%d total=%d next=%t\n", page, len(response.Executions), len(seen), len(response.NextPageToken) > 0)
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
	case "control", "update-only":
		if e = proofUpdate(ctx, c, *id); e != nil {
			return e
		}
		if *mode == "update-only" {
			return emit(map[string]string{"update": "acknowledged"})
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
