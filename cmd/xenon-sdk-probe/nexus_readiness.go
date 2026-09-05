package main

import (
	"context"
	"fmt"
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
	w := worker.New(c, "xenon-nexus-readiness", worker.Options{})
	w.RegisterWorkflow(nexusReadinessWorkflow)
	nexusWorker := worker.New(c, readinessQueue, worker.Options{})
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
	for _, id := range ids {
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: "xenon-nexus-readiness", WorkflowExecutionTimeout: 60 * time.Second}, nexusReadinessWorkflow, nonce)
		if err != nil {
			return nil, err
		}
		var output string
		if err = run.Get(ctx, &output); err != nil {
			return nil, err
		}
		if output != nonce {
			return nil, fmt.Errorf("Nexus readiness result mismatch")
		}
		runs[id] = run.GetRunID()
	}
	return map[string]any{"endpoint": "xenon-fuzz", "namespace_id": description.NamespaceInfo.Id, "task_queue": readinessQueue, "history_shards_verified": 4, "runs": runs, "scope": "actual named endpoint resolution and synchronous worker operation before fuzz"}, nil
}
