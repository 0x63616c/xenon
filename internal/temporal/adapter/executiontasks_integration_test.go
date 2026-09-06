package adapter

import (
	"context"
	"encoding/json"
	"errors"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"math"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

type executionTasksProxy struct {
	wire.UnimplementedExecutionTasksPersistenceServer
	backend  wire.ExecutionTasksPersistenceClient
	mu       sync.Mutex
	first    *wire.ExecutionTasksRequest
	replayed bool
}

func (s *executionTasksProxy) Execute(ctx context.Context, q *wire.ExecutionTasksRequest) (*wire.ExecutionTasksResult, error) {
	r, e := s.backend.Execute(ctx, q)
	if e != nil {
		return nil, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if q.Command.Kind == wire.ExecutionTasksCommand_PUT_DLQ {
		if s.first == nil {
			s.first = proto.Clone(q).(*wire.ExecutionTasksRequest)
			return nil, status.Error(codes.Unavailable, "declared lost completed DLQ put")
		}
		if q.OperationId == s.first.OperationId {
			s.replayed = proto.Equal(q, s.first)
		}
	}
	return r, nil
}
func TestExecutionTasksRPC(t *testing.T) {
	raw, e := os.ReadFile("../../../proof/executiontasks/case.json")
	if e != nil {
		t.Fatal(e)
	}
	var fixture struct {
		namespaceCase
		Shard  int32  `json:"shard_id"`
		Source string `json:"source_cluster"`
	}
	if e = json.Unmarshal(raw, &fixture); e != nil || fixture.SchemaVersion != 1 || fixture.Fault != "drop_first_completed_dlq_put" {
		t.Fatal(e)
	}
	address := startNamespaceNode(t, fixture.namespaceCase)
	conn, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	proxy := &executionTasksProxy{backend: wire.NewExecutionTasksPersistenceClient(conn)}
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	server := grpc.NewServer()
	wire.RegisterExecutionTasksPersistenceServer(server, proxy)
	go server.Serve(l)
	defer server.Stop()
	store, e := NewExecutionTasksStore(l.Addr().String(), fixture.Partition)
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(fixture.TestTimeoutSeconds)*time.Second)
	defer cancel()
	if err := store.AddHistoryTasks(ctx, &p.InternalAddHistoryTasksRequest{ShardID: 9999, RangeID: 31}); err == nil {
		t.Fatal("missing shard accepted")
	} else {
		var unavailable *serviceerror.Unavailable
		if !errors.As(err, &unavailable) {
			t.Fatalf("missing shard type %T: %v", err, err)
		}
	}
	ids := []int64{-3, 0, 9007199254740993, math.MaxInt64}
	for _, id := range ids {
		info := &persistencespb.ReplicationTaskInfo{TaskId: id, NamespaceId: "opaque-namespace", WorkflowId: "opaque-workflow"}
		info.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01})
		if e = store.PutReplicationTaskToDLQ(ctx, &p.PutReplicationTaskToDLQRequest{ShardID: fixture.Shard, SourceClusterName: fixture.Source, TaskInfo: info}); e != nil {
			t.Fatal(e)
		}
	}
	proxy.mu.Lock()
	replayed := proxy.replayed
	proxy.mu.Unlock()
	if !replayed {
		t.Fatal("lost put did not retry identical envelope")
	}
	if e = store.PutReplicationTaskToDLQ(ctx, &p.PutReplicationTaskToDLQRequest{ShardID: fixture.Shard, SourceClusterName: fixture.Source, TaskInfo: &persistencespb.ReplicationTaskInfo{TaskId: -3, WorkflowId: "must-not-replace"}}); e != nil {
		t.Fatal(e)
	}
	q := &p.GetReplicationTasksFromDLQRequest{SourceClusterName: fixture.Source, GetHistoryTasksRequest: p.GetHistoryTasksRequest{ShardID: fixture.Shard, InclusiveMinTaskKey: tasks.NewImmediateKey(math.MinInt64), ExclusiveMaxTaskKey: tasks.NewImmediateKey(math.MaxInt64), BatchSize: 1}}
	var got []int64
	for page := 0; page < 5; page++ {
		r, e := store.GetReplicationTasksFromDLQ(ctx, q)
		if e != nil {
			t.Fatal(e)
		}
		for _, item := range r.Tasks {
			info, e := serialization.NewSerializer().ReplicationTaskInfoFromBlob(item.Blob)
			if e != nil || info.WorkflowId != "opaque-workflow" || len(info.ProtoReflect().GetUnknown()) != 3 {
				t.Fatal(info, e)
			}
			got = append(got, item.Key.TaskID)
		}
		q.NextPageToken = r.NextPageToken
		if len(q.NextPageToken) == 0 {
			break
		}
	}
	if len(got) != 3 || got[0] != -3 || got[1] != 0 || got[2] != 9007199254740993 {
		t.Fatal(got)
	}
	if e = store.RangeDeleteReplicationTaskFromDLQ(ctx, &p.RangeDeleteReplicationTaskFromDLQRequest{SourceClusterName: fixture.Source, RangeCompleteHistoryTasksRequest: p.RangeCompleteHistoryTasksRequest{ShardID: fixture.Shard, InclusiveMinTaskKey: tasks.NewImmediateKey(math.MinInt64), ExclusiveMaxTaskKey: tasks.NewImmediateKey(math.MaxInt64)}}); e != nil {
		t.Fatal(e)
	}
	// MaxInt64 is admitted by Put. The empty check must include that tail value.
	q.NextPageToken = []byte("ignored by empty check")
	q.ExclusiveMaxTaskKey = tasks.NewImmediateKey(-100)
	empty, e := store.IsReplicationDLQEmpty(ctx, q)
	if e != nil || empty {
		t.Fatal("max ID hidden from emptiness", empty, e)
	}
	if e = store.DeleteReplicationTaskFromDLQ(ctx, &p.DeleteReplicationTaskFromDLQRequest{SourceClusterName: fixture.Source, CompleteHistoryTaskRequest: p.CompleteHistoryTaskRequest{ShardID: fixture.Shard, TaskKey: tasks.NewImmediateKey(math.MaxInt64)}}); e != nil {
		t.Fatal(e)
	}
	empty, e = store.IsReplicationDLQEmpty(ctx, q)
	if e != nil || !empty {
		t.Fatal(empty, e)
	}
	other := *q
	other.SourceClusterName = "source/two"
	if e = store.PutReplicationTaskToDLQ(ctx, &p.PutReplicationTaskToDLQRequest{ShardID: fixture.Shard, SourceClusterName: other.SourceClusterName, TaskInfo: &persistencespb.ReplicationTaskInfo{TaskId: 1}}); e != nil {
		t.Fatal(e)
	}
	empty, e = store.IsReplicationDLQEmpty(ctx, q)
	if e != nil || !empty {
		t.Fatal("source isolation", empty, e)
	}
}
