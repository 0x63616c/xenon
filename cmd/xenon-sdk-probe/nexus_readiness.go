package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common"
)

const readinessService = "XenonReadiness"
const readinessQueue = "omes-xenon-ministack-fuzz"

func nexusReadinessWorkflow(ctx workflow.Context, nonce string) (string, error) {
	var output string
	err := workflow.NewNexusClient("xenon-fuzz", readinessService).ExecuteOperation(ctx, "echo", nonce, workflow.NexusOperationOptions{ScheduleToCloseTimeout: 30 * time.Second}).Get(ctx, &output)
	return output, err
}
func readinessNexusService() (*nexus.Service, error) {
	s := nexus.NewService(readinessService)
	err := s.Register(nexus.NewSyncOperation("echo", func(_ context.Context, input string, _ nexus.StartOperationOptions) (string, error) {
		return input, nil
	}))
	return s, err
}
func readinessWorkflowIDs(namespaceID, nonce string) ([]string, error) {
	ids := make([]string, 4)
	for i := 0; i < 10000; i++ {
		id := fmt.Sprintf("xenon-nexus-readiness-%s-%d", nonce, i)
		shard := common.WorkflowIDToHistoryShard(namespaceID, id, 4)
		if shard < 1 || shard > 4 {
			return nil, fmt.Errorf("unexpected history shard")
		}
		if ids[shard-1] == "" {
			ids[shard-1] = id
		}
		complete := true
		for _, id := range ids {
			complete = complete && id != ""
		}
		if complete {
			return ids, nil
		}
	}
	return nil, fmt.Errorf("could not cover all declared history shards")
}

// All four declared history shards must resolve the endpoint by name and receive
// a real worker-side echo. Operator Create/Get acknowledgment alone is insufficient.
func nexusReadiness(ctx context.Context, c client.Client, namespace string) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	description, err := c.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: namespace})
	if err != nil {
		return nil, err
	}
	if description == nil || description.NamespaceInfo == nil {
		return nil, fmt.Errorf("missing namespace identity")
	}
	nonce := uuid.NewString()
	ids, err := readinessWorkflowIDs(description.NamespaceInfo.Id, nonce)
	if err != nil {
		return nil, err
	}
	s, err := readinessNexusService()
	if err != nil {
		return nil, err
	}
	w := worker.New(c, "xenon-nexus-readiness", readinessWorkerOptions(false))
	w.RegisterWorkflow(nexusReadinessWorkflow)
	nexusWorker := worker.New(c, readinessQueue, readinessWorkerOptions(true))
	nexusWorker.RegisterNexusService(s)
	if err = w.Start(); err != nil {
		return nil, err
	}
	defer w.Stop()
	if err = nexusWorker.Start(); err != nil {
		return nil, err
	}
	defer nexusWorker.Stop()
	runs := map[string]string{}
	for shard, id := range ids {
		readinessLog(map[string]any{"event": "nexus_readiness_start", "workflow_id": id, "history_shard": shard + 1})
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: "xenon-nexus-readiness", WorkflowExecutionTimeout: 60 * time.Second}, nexusReadinessWorkflow, nonce)
		if err != nil {
			return nil, err
		}
		readinessLog(map[string]any{"event": "nexus_readiness_started", "workflow_id": id, "run_id": run.GetRunID(), "history_shard": shard + 1})
		monitorCtx, stopMonitor := context.WithCancel(context.Background())
		monitorDone := make(chan struct{})
		go func() {
			defer close(monitorDone)
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-monitorCtx.Done():
					return
				case <-ticker.C:
					readinessLog(readinessHistory(monitorCtx, c.WorkflowService(), namespace, id, run.GetRunID()))
				}
			}
		}()
		var output string
		err = run.Get(ctx, &output)
		stopMonitor()
		<-monitorDone
		readinessLog(readinessHistory(context.Background(), c.WorkflowService(), namespace, id, run.GetRunID()))
		if err != nil {
			return nil, fmt.Errorf("Nexus readiness shard %d workflow %s run %s: %w", shard+1, id, run.GetRunID(), err)
		}
		readinessLog(map[string]any{"event": "nexus_readiness_completed", "workflow_id": id, "run_id": run.GetRunID(), "history_shard": shard + 1})
		if output != nonce {
			return nil, fmt.Errorf("Nexus readiness result mismatch")
		}
		runs[id] = run.GetRunID()
	}
	return map[string]any{"endpoint": "xenon-fuzz", "namespace_id": description.NamespaceInfo.Id, "task_queue": readinessQueue, "history_shards_verified": 4, "runs": runs, "scope": "actual named endpoint resolution and synchronous worker operation before fuzz"}, nil
}

func readinessWorkerOptions(nexusOnly bool) worker.Options {
	return worker.Options{DisableWorkflowWorker: nexusOnly, LocalActivityWorkerOnly: true}
}
func readinessLog(record map[string]any) { _ = json.NewEncoder(os.Stderr).Encode(record) }
