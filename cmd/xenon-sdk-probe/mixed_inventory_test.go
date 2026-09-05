package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
)

type mixedPages struct {
	pages []*workflowservice.ListWorkflowExecutionsResponse
	calls int
}

func (m *mixedPages) ListWorkflowExecutions(_ context.Context, q *workflowservice.ListWorkflowExecutionsRequest, _ ...grpc.CallOption) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	if q.Query != "TaskQueue = 'omes-xenon-full-mixed'" {
		return nil, fmt.Errorf("wrong query")
	}
	if m.calls >= len(m.pages) {
		return nil, fmt.Errorf("unexpected page")
	}
	r := m.pages[m.calls]
	m.calls++
	return r, nil
}
func mixedFixture() *mixedPages {
	pages := &mixedPages{pages: []*workflowservice.ListWorkflowExecutionsResponse{{NextPageToken: []byte("next")}, {}}}
	for i := 0; i < 40; i++ {
		for _, status := range []enumspb.WorkflowExecutionStatus{enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW} {
			p := pages.pages[i%2]
			p.Executions = append(p.Executions, &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: fmt.Sprintf("w-xenon-full-mixed-0123456789abcdef-%d", i), RunId: uuid.NewString()}, Status: status})
		}
	}
	pages.pages[0].Executions = append(pages.pages[0].Executions, &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: "w-xenon-full-mixed-0123456789abcdef-0/child-1", RunId: uuid.NewString()}, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED})
	return pages
}
func TestMixedInventory(t *testing.T) {
	r, e := mixedInventory(context.Background(), mixedFixture(), "ns")
	if e != nil || r.ParentWorkflows != 40 || len(r.ParentRuns) != 80 || r.HistoryOracle != "NOT_EXECUTED" {
		t.Fatal(r, e)
	}
	for _, mutation := range []func(*mixedPages){
		func(p *mixedPages) { p.pages[0].Executions = p.pages[0].Executions[1:] },
		func(p *mixedPages) { p.pages[0].Executions[0].Status = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED },
		func(p *mixedPages) { p.pages[1].NextPageToken = []byte("next") },
		func(p *mixedPages) { p.pages[1].Executions = append(p.pages[1].Executions, p.pages[0].Executions[0]) },
	} {
		p := mixedFixture()
		mutation(p)
		if _, e := mixedInventory(context.Background(), p, "ns"); e == nil {
			t.Fatal("invalid inventory accepted")
		}
	}
}
