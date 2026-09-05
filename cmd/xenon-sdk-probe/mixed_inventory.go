package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
)

type mixedLister interface {
	ListWorkflowExecutions(context.Context, *workflowservice.ListWorkflowExecutionsRequest, ...grpc.CallOption) (*workflowservice.ListWorkflowExecutionsResponse, error)
}
type mixedRun struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
	Status     string `json:"status"`
}
type mixedInventoryResult struct {
	ParentWorkflows int        `json:"parent_workflows"`
	ParentRuns      []mixedRun `json:"parent_runs"`
	OmesExecutionID string     `json:"omes_execution_id"`
	HistoryOracle   string     `json:"history_oracle"`
}

// Inventory is scoped to the root IDs produced by pinned Omes DefaultStartWorkflowOptions.
// Intentional activity retries/cancellation and unrelated Nexus workflows are not
// interpreted as failed workflow histories here. CAN links still need history audit.
func mixedInventory(ctx context.Context, service mixedLister, namespace string) (mixedInventoryResult, error) {
	result := mixedInventoryResult{HistoryOracle: "NOT_EXECUTED"}
	parents := map[string]map[enumspb.WorkflowExecutionStatus]int{}
	runs := map[string]bool{}
	tokens := map[string]bool{}
	var token []byte
	total := 0
	for page := 0; ; page++ {
		if page >= 100 {
			return result, fmt.Errorf("mixed page count exceeds bound")
		}
		response, err := service.ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{Namespace: namespace, Query: "TaskQueue = 'omes-xenon-full-mixed'", PageSize: 100, NextPageToken: token})
		if err != nil {
			return result, err
		}
		if response == nil {
			return result, fmt.Errorf("nil mixed visibility page")
		}
		total += len(response.Executions)
		if total > 5000 {
			return result, fmt.Errorf("mixed inventory exceeds bound")
		}
		for _, info := range response.Executions {
			if info == nil || info.Execution == nil {
				return result, fmt.Errorf("nil visibility execution")
			}
			id := info.Execution.WorkflowId
			if !strings.HasPrefix(id, "w-xenon-full-mixed-") {
				continue
			}
			suffix := strings.TrimPrefix(id, "w-xenon-full-mixed-")
			if len(suffix) < 18 || suffix[16] != '-' {
				return result, fmt.Errorf("invalid mixed root identity")
			}
			_, err := hex.DecodeString(suffix[:16])
			executionID := suffix[:16]
			if err != nil {
				return result, err
			}
			iteration, err := strconv.Atoi(suffix[17:])
			if err != nil || iteration < 0 {
				return result, fmt.Errorf("invalid mixed iteration identity")
			}
			if _, err = uuid.Parse(info.Execution.RunId); err != nil {
				return result, err
			}
			if result.OmesExecutionID != "" && result.OmesExecutionID != executionID {
				return result, fmt.Errorf("multiple Omes executions in inventory")
			}
			result.OmesExecutionID = executionID
			key := id + "/" + info.Execution.RunId
			if runs[key] {
				return result, fmt.Errorf("duplicate mixed run")
			}
			runs[key] = true
			if parents[id] == nil {
				parents[id] = map[enumspb.WorkflowExecutionStatus]int{}
			}
			parents[id][info.Status]++
			result.ParentRuns = append(result.ParentRuns, mixedRun{id, info.Execution.RunId, info.Status.String()})
		}
		token = response.NextPageToken
		if len(token) > 16384 {
			return result, fmt.Errorf("mixed cursor exceeds bound")
		}
		if len(token) == 0 {
			break
		}
		if tokens[string(token)] {
			return result, fmt.Errorf("repeated mixed cursor")
		}
		tokens[string(token)] = true
	}
	if len(parents) != 40 {
		return result, fmt.Errorf("mixed inventory has %d parents; require40", len(parents))
	}
	for _, statuses := range parents {
		if len(statuses) != 2 || statuses[enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED] != 1 || statuses[enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW] != 1 {
			return result, fmt.Errorf("mixed parent lacks exact completed/CAN visibility pair")
		}
	}
	result.ParentWorkflows = len(parents)
	return result, nil
}
