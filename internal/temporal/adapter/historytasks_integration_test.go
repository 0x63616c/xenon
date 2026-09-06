package adapter

import (
	"context"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"
)

func TestHistoryTasksRPC(t *testing.T) {
	c := executionCase(t)
	address := startNamespaceNode(t, c.namespaceCase)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sh, e := NewShardStore(address, c.Partition, "proof")
	if e != nil {
		t.Fatal(e)
	}
	defer sh.Close()
	blob := &commonpb.DataBlob{Data: []byte{0, 255}, EncodingType: enumspb.ENCODING_TYPE_PROTO3}
	_, e = sh.GetOrCreateShard(ctx, &p.InternalGetOrCreateShardRequest{ShardID: c.ShardID, CreateShardInfo: func() (int64, *commonpb.DataBlob, error) { return c.RangeID, blob, nil }})
	if e != nil {
		t.Fatal(e)
	}
	writer, e := NewWorkflowStore(address, c.Partition)
	if e != nil {
		t.Fatal(e)
	}
	defer writer.Close()
	state := &persistencespb.WorkflowExecutionState{RunId: c.Runs[0], State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}
	raw, _ := proto.Marshal(state)
	_, e = writer.CreateWorkflowExecution(ctx, &p.InternalCreateWorkflowExecutionRequest{ShardID: c.ShardID, RangeID: c.RangeID, Mode: p.CreateWorkflowModeBrandNew, NewWorkflowSnapshot: p.InternalWorkflowSnapshot{NamespaceID: c.NamespaceID, WorkflowID: "historytasks", RunID: c.Runs[0], ExecutionInfo: &persistencespb.WorkflowExecutionInfo{}, ExecutionInfoBlob: blob, ExecutionState: state, ExecutionStateBlob: &commonpb.DataBlob{Data: raw, EncodingType: enumspb.ENCODING_TYPE_PROTO3}, NextEventID: 3, DBRecordVersion: 1, Tasks: map[tasks.Category][]p.InternalHistoryTask{tasks.CategoryTransfer: {{Key: tasks.NewImmediateKey(1), Blob: blob}, {Key: tasks.NewImmediateKey(2), Blob: blob}}}}})
	if e != nil {
		t.Fatal(e)
	}
	s, e := NewHistoryTasksStore(address, c.Partition)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	q := &p.GetHistoryTasksRequest{ShardID: c.ShardID, TaskCategory: tasks.CategoryTransfer, InclusiveMinTaskKey: tasks.NewImmediateKey(0), ExclusiveMaxTaskKey: tasks.NewImmediateKey(3), BatchSize: 1}
	r, e := s.GetHistoryTasks(ctx, q)
	if e != nil || len(r.Tasks) != 1 || r.Tasks[0].Key.TaskID != 1 || !proto.Equal(r.Tasks[0].Blob, blob) {
		t.Fatal(r, e)
	}
	q.NextPageToken = r.NextPageToken
	r, e = s.GetHistoryTasks(ctx, q)
	if e != nil || len(r.Tasks) != 1 || r.Tasks[0].Key.TaskID != 2 {
		t.Fatal(r, e)
	}
	if e = s.CompleteHistoryTask(ctx, &p.CompleteHistoryTaskRequest{ShardID: c.ShardID, TaskCategory: tasks.CategoryTransfer, TaskKey: tasks.NewImmediateKey(1)}); e != nil {
		t.Fatal(e)
	}
	if e = s.RangeCompleteHistoryTasks(ctx, &p.RangeCompleteHistoryTasksRequest{ShardID: c.ShardID, TaskCategory: tasks.CategoryTransfer, InclusiveMinTaskKey: tasks.NewImmediateKey(2), ExclusiveMaxTaskKey: tasks.NewImmediateKey(3)}); e != nil {
		t.Fatal(e)
	}
	q.NextPageToken = nil
	r, e = s.GetHistoryTasks(ctx, q)
	if e != nil || len(r.Tasks) != 0 {
		t.Fatal(r, e)
	}
}
