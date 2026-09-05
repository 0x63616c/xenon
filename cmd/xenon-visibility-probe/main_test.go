package main

import (
	"context"
	"fmt"
	"go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/payload"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"strconv"
	"testing"
)

type fakeVisibility struct {
	workflowservice.WorkflowServiceClient
	d         dataset
	duplicate bool
	fault     string
	pageCalls *int
}

func (f fakeVisibility) ListWorkflowExecutions(_ context.Context, q *workflowservice.ListWorkflowExecutionsRequest, _ ...grpc.CallOption) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	if f.pageCalls != nil {
		*f.pageCalls++
	}
	if f.fault == "nil-list" {
		return nil, nil
	}
	r := &workflowservice.ListWorkflowExecutionsResponse{}
	start := 0
	if len(q.NextPageToken) > 0 {
		start, _ = strconv.Atoi(string(q.NextPageToken))
	}
	end := start + int(q.PageSize)
	if end > f.d.count {
		end = f.d.count
	}
	if end < f.d.count {
		r.NextPageToken = []byte(strconv.Itoa(end))
	}
	for i := start; i < end; i++ {
		if f.fault == "omission" && i == 0 {
			continue
		}
		id := f.d.run(i)
		if f.duplicate {
			id = f.d.run(0)
		}
		name := fmt.Sprintf("%s-%04d", f.d.queue, i)
		if i < f.d.updated {
			name += "-updated"
		}
		r.Executions = append(r.Executions, &workflow.WorkflowExecutionInfo{Execution: &common.WorkflowExecution{RunId: id, WorkflowId: name}, Status: enumspb.WorkflowExecutionStatus(i%4 + 1)})
	}
	if f.fault == "nil-execution" {
		r.Executions = []*workflow.WorkflowExecutionInfo{{}}
	}
	return r, nil
}
func (f fakeVisibility) CountWorkflowExecutions(_ context.Context, q *workflowservice.CountWorkflowExecutionsRequest, _ ...grpc.CallOption) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	if f.fault == "nil-count" {
		return nil, nil
	}
	r := &workflowservice.CountWorkflowExecutionsResponse{Count: int64(f.d.count)}
	if f.fault == "wrong-count" {
		r.Count--
	}
	for i := 1; i <= 4; i++ {
		p, _ := payload.Encode(enumspb.WorkflowExecutionStatus(i).String())
		r.Groups = append(r.Groups, &workflowservice.CountWorkflowExecutionsResponse_AggregationGroup{GroupValues: []*common.Payload{p}, Count: int64(f.d.count / 4)})
	}
	if f.fault == "wrong-group" {
		r.Groups[0].Count--
	}
	return r, nil
}
func TestVisibilityOracleControls(t *testing.T) {
	d := dataset{namespace: "11111111-1111-1111-1111-111111111111", queue: "frozen", count: 2000}
	memo := d.record(1999, 1).Memo
	decoded := new(common.Memo)
	if memo.EncodingType != enumspb.ENCODING_TYPE_PROTO3 || proto.Unmarshal(memo.Data, decoded) != nil || len(decoded.Fields["frozen"].Data) != 1100000 {
		t.Fatal("public memo format invalid")
	}
	if len(d.expected()) != 2000 {
		t.Fatal("recipecollision")
	}
	if _, e := check(context.Background(), fakeVisibility{d: d}, "ns", d, []int{1, 7, 100}); e != nil {
		t.Fatal(e)
	}
	if _, e := check(context.Background(), fakeVisibility{d: d, duplicate: true}, "ns", d, []int{7}); e == nil {
		t.Fatal("duplicatespassed")
	}
	d.count = 20
	d.offset = 1000000
	d.updated = 20
	if _, e := check(context.Background(), fakeVisibility{d: d}, "ns", d, []int{7}); e != nil {
		t.Fatal(e)
	}
}

func TestVisibilityOracleFailureAndInterpageControls(t *testing.T) {
	d := dataset{namespace: "ns", queue: "frozen", count: 20}
	for _, fault := range []string{"omission", "wrong-group", "wrong-count", "nil-list", "nil-count", "nil-execution"} {
		if _, e := check(context.Background(), fakeVisibility{d: d, fault: fault}, "ns", d, []int{7}); e == nil {
			t.Fatal("fault passed", fault)
		}
	}
	calls := 0
	hookCalls := 0
	if _, e := checkPages(context.Background(), fakeVisibility{d: d, pageCalls: &calls}, "ns", d, []int{7}, func() error {
		hookCalls++
		if calls != 1 {
			t.Fatal("write not between pages", calls)
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if calls != 3 || hookCalls != 1 {
		t.Fatal("interpage control missing", calls, hookCalls)
	}
}
