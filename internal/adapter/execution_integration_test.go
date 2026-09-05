package adapter

import (
	"context"
	"encoding/json"
	"errors"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

type executionFixture struct {
	namespaceCase
	NamespaceID string   `json:"namespace_id"`
	Runs        []string `json:"runs"`
	ShardID     int32    `json:"shard_id"`
	RangeID     int64    `json:"range_id"`
}

func executionCase(t *testing.T) executionFixture {
	t.Helper()
	raw, e := os.ReadFile("../../proof/execution/case.json")
	if e != nil {
		t.Fatal(e)
	}
	var c executionFixture
	if e = json.Unmarshal(raw, &c); e != nil || c.SchemaVersion != 1 || len(c.Runs) != 3 || c.Fault != "drop_first_completed_workflow_create" {
		t.Fatal("invalid execution fixture", e)
	}
	return c
}

type executionProxy struct {
	wire.UnimplementedExecutionPersistenceServer
	backend  wire.ExecutionPersistenceClient
	mu       sync.Mutex
	first    *wire.ExecutionRequest
	replayed bool
}

func (s *executionProxy) Execute(ctx context.Context, q *wire.ExecutionRequest) (*wire.ExecutionResult, error) {
	r, e := s.backend.Execute(ctx, q)
	if e != nil {
		return nil, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if q.Command.Kind == wire.ExecutionCommand_CREATE {
		if s.first == nil {
			s.first = proto.Clone(q).(*wire.ExecutionRequest)
			return nil, status.Error(codes.Unavailable, "declared lost completed workflow create")
		}
		if q.OperationId == s.first.OperationId {
			s.replayed = proto.Equal(q, s.first)
		}
	}
	return r, nil
}
func TestExecutionRPC(t *testing.T) {
	c := executionCase(t)
	address := startNamespaceNode(t, c.namespaceCase)
	conn, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	proxy := &executionProxy{backend: wire.NewExecutionPersistenceClient(conn)}
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	srv := grpc.NewServer()
	wire.RegisterExecutionPersistenceServer(srv, proxy)
	go srv.Serve(l)
	defer srv.Stop()
	s, e := NewWorkflowStore(l.Addr().String(), c.Partition)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.TestTimeoutSeconds)*time.Second)
	defer cancel()
	shard, e := NewShardStore(address, c.Partition, "proof")
	if e != nil {
		t.Fatal(e)
	}
	defer shard.Close()
	blob := &commonpb.DataBlob{Data: []byte{0, 255, 3}, EncodingType: enumspb.ENCODING_TYPE_PROTO3}
	_, e = shard.GetOrCreateShard(ctx, &p.InternalGetOrCreateShardRequest{ShardID: c.ShardID, CreateShardInfo: func() (int64, *commonpb.DataBlob, error) { return c.RangeID, blob, nil }})
	if e != nil {
		t.Fatal(e)
	}
	snapshot := func(run string, version int64) p.InternalWorkflowSnapshot {
		state := &persistencespb.WorkflowExecutionState{RunId: run, CreateRequestId: "request", State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, RequestIds: map[string]*persistencespb.RequestIDInfo{"request": {}}}
		raw, _ := proto.Marshal(state)
		return p.InternalWorkflowSnapshot{NamespaceID: c.NamespaceID, WorkflowID: "workflow/with/slashes", RunID: run, ExecutionInfo: &persistencespb.WorkflowExecutionInfo{}, ExecutionInfoBlob: blob, ExecutionState: state, ExecutionStateBlob: &commonpb.DataBlob{Data: raw, EncodingType: enumspb.ENCODING_TYPE_PROTO3}, DBRecordVersion: version, NextEventID: 11, LastWriteVersion: 9001, ActivityInfos: map[int64]*commonpb.DataBlob{1: blob}, TimerInfos: map[string]*commonpb.DataBlob{"timer": blob}, SignalRequestedIDs: map[string]struct{}{"signal": {}}, ChasmNodes: map[string]p.InternalChasmNode{"node": {Metadata: blob, Data: blob, CassandraBlob: blob}}}
	}
	first := snapshot(c.Runs[0], 1)
	first.Tasks = map[tasks.Category][]p.InternalHistoryTask{tasks.CategoryTransfer: {{Key: tasks.NewImmediateKey(71), Blob: blob}}, tasks.CategoryTimer: {{Key: tasks.NewKey(time.Unix(1700000000, 123456000), 72), Blob: blob}}}
	_, e = s.CreateWorkflowExecution(ctx, &p.InternalCreateWorkflowExecutionRequest{ShardID: c.ShardID, RangeID: c.RangeID, Mode: p.CreateWorkflowModeBrandNew, NewWorkflowSnapshot: first})
	if e != nil {
		t.Fatal(e)
	}
	proxy.mu.Lock()
	replayed := proxy.replayed
	proxy.mu.Unlock()
	if !replayed {
		t.Fatal("lost response did not replay identical request")
	}
	get := func(run string) *p.InternalGetWorkflowExecutionResponse {
		t.Helper()
		r, e := s.GetWorkflowExecution(ctx, &p.GetWorkflowExecutionRequest{ShardID: c.ShardID, NamespaceID: c.NamespaceID, WorkflowID: first.WorkflowID, RunID: run})
		if e != nil {
			t.Fatal(e)
		}
		return r
	}
	if got := get(first.RunID); got.DBRecordVersion != 1 || !proto.Equal(got.State.ExecutionInfo, blob) || !proto.Equal(got.State.ChasmNodes["node"].CassandraBlob, blob) {
		t.Fatal("image lost opaque fields", got)
	}
	second := snapshot(c.Runs[1], 1)
	_, e = s.CreateWorkflowExecution(ctx, &p.InternalCreateWorkflowExecutionRequest{ShardID: c.ShardID, RangeID: c.RangeID, Mode: p.CreateWorkflowModeBrandNew, NewWorkflowSnapshot: second})
	var currentErr *p.CurrentWorkflowConditionFailedError
	if !errors.As(e, &currentErr) || currentErr.RunID != first.RunID || currentErr.LastWriteVersion != 9001 || currentErr.RequestIDs["request"] == nil {
		t.Fatal("current conflict fields", e)
	}
	mutation := p.InternalWorkflowMutation{NamespaceID: c.NamespaceID, WorkflowID: first.WorkflowID, RunID: first.RunID, ExecutionInfo: first.ExecutionInfo, ExecutionInfoBlob: blob, ExecutionState: first.ExecutionState, ExecutionStateBlob: first.ExecutionStateBlob, DBRecordVersion: 2, NextEventID: 12, LastWriteVersion: 9002, UpsertActivityInfos: map[int64]*commonpb.DataBlob{2: blob}, DeleteActivityInfos: map[int64]struct{}{1: {}}, DeleteTimerInfos: map[string]struct{}{"timer": {}}, DeleteChasmNodes: map[string]struct{}{"node": {}}, NewBufferedEvents: blob}
	if e = s.UpdateWorkflowExecution(ctx, &p.InternalUpdateWorkflowExecutionRequest{ShardID: c.ShardID, RangeID: c.RangeID, Mode: p.UpdateWorkflowModeUpdateCurrent, UpdateWorkflowMutation: mutation}); e != nil {
		t.Fatal(e)
	}
	got := get(first.RunID)
	if got.DBRecordVersion != 2 || len(got.State.ActivityInfos) != 1 || got.State.ActivityInfos[2] == nil || len(got.State.TimerInfos) != 0 || len(got.State.ChasmNodes) != 0 || len(got.State.BufferedEvents) != 1 {
		t.Fatal("mutation", got)
	}
	e = s.UpdateWorkflowExecution(ctx, &p.InternalUpdateWorkflowExecutionRequest{ShardID: c.ShardID, RangeID: c.RangeID, Mode: p.UpdateWorkflowModeIgnoreCurrent, UpdateWorkflowMutation: mutation})
	var versionErr *p.WorkflowConditionFailedError
	if !errors.As(e, &versionErr) || versionErr.DBRecordVersion != 2 || versionErr.NextEventID != 12 {
		t.Fatal("version fields", e)
	}
	// A duplicate task occurs after staging the current pointer and state update.
	// The entire operation must roll back while its typed failure is journaled.
	mutation.DBRecordVersion = 3
	mutation.Tasks = first.Tasks
	e = s.UpdateWorkflowExecution(ctx, &p.InternalUpdateWorkflowExecutionRequest{ShardID: c.ShardID, RangeID: c.RangeID, Mode: p.UpdateWorkflowModeUpdateCurrent, UpdateWorkflowMutation: mutation, NewWorkflowSnapshot: &second})
	var unavailable *serviceerror.Unavailable
	if !errors.As(e, &unavailable) || get(first.RunID).DBRecordVersion != 2 {
		t.Fatal("duplicate task rollback", e)
	}
	cur, e := s.GetCurrentExecution(ctx, &p.GetCurrentExecutionRequest{ShardID: c.ShardID, NamespaceID: c.NamespaceID, WorkflowID: first.WorkflowID})
	if e != nil || cur.RunID != first.RunID {
		t.Fatal("current rollback", cur, e)
	}
	mutation.Tasks = nil
	mutation.ClearBufferedEvents = true
	mutation.NewBufferedEvents = nil
	if e = s.UpdateWorkflowExecution(ctx, &p.InternalUpdateWorkflowExecutionRequest{ShardID: c.ShardID, RangeID: c.RangeID, Mode: p.UpdateWorkflowModeUpdateCurrent, UpdateWorkflowMutation: mutation, NewWorkflowSnapshot: &second}); e != nil {
		t.Fatal(e)
	}
	reset := snapshot(first.RunID, 4)
	reset.ActivityInfos = nil
	current := mutation
	current.RunID = second.RunID
	current.ExecutionState = second.ExecutionState
	current.ExecutionStateBlob = second.ExecutionStateBlob
	current.DBRecordVersion = 2
	if e = s.ConflictResolveWorkflowExecution(ctx, &p.InternalConflictResolveWorkflowExecutionRequest{ShardID: c.ShardID, RangeID: c.RangeID, Mode: p.ConflictResolveWorkflowModeUpdateCurrent, ResetWorkflowSnapshot: reset, CurrentWorkflowMutation: &current}); e != nil {
		t.Fatal(e)
	}
	set := snapshot(first.RunID, 5)
	if e = s.SetWorkflowExecution(ctx, &p.InternalSetWorkflowExecutionRequest{ShardID: c.ShardID, RangeID: c.RangeID, SetWorkflowSnapshot: set}); e != nil {
		t.Fatal(e)
	}
	var token []byte
	count := 0
	for page := 0; page < 4; page++ {
		r, e := s.ListConcreteExecutions(ctx, &p.ListConcreteExecutionsRequest{ShardID: c.ShardID, PageSize: 1, PageToken: token})
		if e != nil {
			t.Fatal(e)
		}
		count += len(r.States)
		token = r.NextPageToken
		if len(token) == 0 {
			break
		}
	}
	if count != 2 {
		t.Fatal("pagination", count)
	}
	e = s.DeleteCurrentWorkflowExecution(ctx, &p.DeleteCurrentWorkflowExecutionRequest{ShardID: c.ShardID, NamespaceID: c.NamespaceID, WorkflowID: first.WorkflowID, RunID: second.RunID})
	if e != nil {
		t.Fatal(e)
	}
	cur, e = s.GetCurrentExecution(ctx, &p.GetCurrentExecutionRequest{ShardID: c.ShardID, NamespaceID: c.NamespaceID, WorkflowID: first.WorkflowID})
	if e != nil || cur.RunID != first.RunID {
		t.Fatal("conditional delete", cur, e)
	}
	if e = s.DeleteWorkflowExecution(ctx, &p.DeleteWorkflowExecutionRequest{ShardID: c.ShardID, NamespaceID: c.NamespaceID, WorkflowID: first.WorkflowID, RunID: second.RunID}); e != nil {
		t.Fatal(e)
	}
	if e = s.DeleteCurrentWorkflowExecution(ctx, &p.DeleteCurrentWorkflowExecutionRequest{ShardID: c.ShardID, NamespaceID: c.NamespaceID, WorkflowID: first.WorkflowID, RunID: first.RunID}); e != nil {
		t.Fatal(e)
	}
}
