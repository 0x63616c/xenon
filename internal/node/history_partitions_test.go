package node

import (
	"context"
	"fmt"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/temporal/adapter"
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
	native "slatedb.io/slatedb-go/uniffi"
	"testing"
	"time"
)

func TestGoOwnerHistoryPartitions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store := objects(t)
	names := []string{"history-0", "history-1", "history-2", "history-3"}
	owners := map[string]*Owner{}
	for _, name := range names {
		o, e := NewOwner(engine(t, store, "partition-proof-"+name, false), DefaultConfig(name))
		if e != nil {
			t.Fatal(e)
		}
		owners[name] = o
		t.Cleanup(func() { closeOwner(t, o) })
	}
	interceptor := func(ctx context.Context, q any, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
		o := owners[q.(interface{ GetPartition() string }).GetPartition()]
		if o == nil {
			return nil, status.Error(codes.InvalidArgument, "unknown partition")
		}

		shardID := int32(0)
		hasShard := true
		switch request := q.(type) {
		case *wire.ShardRequest:
			shardID = request.Command.ShardId
		case *wire.HistoryRequest:
			shardID = request.Command.ShardId
			hasShard = request.Command.Kind != wire.HistoryCommand_LIST_TREES
		case *wire.ExecutionRequest:
			shardID = request.Command.ShardId
		case *wire.HistoryTasksRequest:
			shardID = request.Command.ShardId
		case *wire.ExecutionTasksRequest:
			shardID = request.Command.ShardId
		}
		if hasShard && q.(interface{ GetPartition() string }).GetPartition() != fmt.Sprintf("history-%d", shardID%4) {
			t.Error("wrong shard placement")
		}
		switch q := q.(type) {
		case *wire.ShardRequest:
			return o.Execute(ctx, q)
		case *wire.HistoryRequest:
			return (&HistoryServer{Owner: o}).Execute(ctx, q)
		case *wire.ExecutionRequest:
			return (&ExecutionServer{Owner: o}).Execute(ctx, q)
		case *wire.HistoryTasksRequest:
			return (&HistoryTasksServer{Owner: o}).Execute(ctx, q)
		case *wire.ExecutionTasksRequest:
			return (&ExecutionTasksServer{Owner: o}).Execute(ctx, q)
		}
		return nil, status.Error(codes.Unimplemented, "test method")
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(interceptor))
	wire.RegisterShardPersistenceServer(server, &wire.UnimplementedShardPersistenceServer{})
	wire.RegisterHistoryPersistenceServer(server, &wire.UnimplementedHistoryPersistenceServer{})
	wire.RegisterExecutionPersistenceServer(server, &wire.UnimplementedExecutionPersistenceServer{})
	wire.RegisterHistoryTasksPersistenceServer(server, &wire.UnimplementedHistoryTasksPersistenceServer{})
	wire.RegisterExecutionTasksPersistenceServer(server, &wire.UnimplementedExecutionTasksPersistenceServer{})
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	shard, e := adapter.NewPartitionedShardStore(listener.Addr().String(), names, "proof")
	if e != nil {
		t.Fatal(e)
	}
	defer shard.Close()
	execution, e := adapter.NewPartitionedExecutionStore(listener.Addr().String(), names)
	if e != nil {
		t.Fatal(e)
	}
	defer execution.Close()
	// Mutation of the caller's config cannot reinterpret already constructed stores.
	names[0] = "changed"
	for id := int32(0); id < 8; id++ {
		_, e = shard.GetOrCreateShard(ctx, &p.InternalGetOrCreateShardRequest{ShardID: id, CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
			return 7, &commonpb.DataBlob{Data: []byte{byte(id)}, EncodingType: 2}, nil
		}})
		if e != nil {
			t.Fatal(e)
		}
		if e = execution.AddHistoryTasks(ctx, &p.InternalAddHistoryTasksRequest{ShardID: id, RangeID: 7, Tasks: map[tasks.Category][]p.InternalHistoryTask{tasks.CategoryTransfer: {{Key: tasks.NewImmediateKey(1), Blob: &commonpb.DataBlob{Data: []byte{byte(id)}, EncodingType: 2}}}}}); e != nil {
			t.Fatal(e)
		}
		got, e := execution.GetHistoryTasks(ctx, &p.GetHistoryTasksRequest{ShardID: id, TaskCategory: tasks.CategoryTransfer, InclusiveMinTaskKey: tasks.NewImmediateKey(0), ExclusiveMaxTaskKey: tasks.NewImmediateKey(2), BatchSize: 1})
		if e != nil || len(got.Tasks) != 1 || got.Tasks[0].Blob.Data[0] != byte(id) {
			t.Fatal(got, e)
		}
		if _, e = execution.GetCurrentExecution(ctx, &p.GetCurrentExecutionRequest{ShardID: id, NamespaceID: "00000000-0000-0000-0000-000000000001", WorkflowID: "absent"}); e == nil {
			t.Fatal("missing current execution accepted")
		}
		if e = execution.CompleteHistoryTask(ctx, &p.CompleteHistoryTaskRequest{ShardID: id, TaskCategory: tasks.CategoryTransfer, TaskKey: tasks.NewImmediateKey(1)}); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = shard.GetOrCreateShard(ctx, &p.InternalGetOrCreateShardRequest{ShardID: -1}); status.Code(e) == codes.OK {
		t.Fatal("negative shard accepted")
	}
	// Seed branch records in three owners, leaving the third partition empty.
	want := map[string]bool{}
	for index := 0; index < 4; index++ {
		if index == 2 {
			continue
		}
		o := owners[fmt.Sprintf("history-%d", index)]
		_, e = o.Run(ctx, func(db *native.Db) ([]byte, error) {
			tx, e := db.Begin(native.IsolationLevelSerializableSnapshot)
			if e != nil {
				return nil, e
			}
			defer tx.Destroy()
			for n := 0; n < 3; n++ {
				tree := uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012d", index*10+n+1))
				branch := uuid.MustParse("00000000-0000-0000-0000-000000000099")
				want[tree.String()] = true
				if e = putHistory(tx, historyTreeKey(int32(index), tree[:], branch[:]), &wire.HistoryTreeRecord{ShardId: int32(index), TreeId: tree[:], BranchId: branch[:], Info: &wire.HistoryBlob{Data: make([]byte, 1024*1024), Encoding: 2}}); e != nil {
					return nil, e
				}
			}
			return nil, commit(tx)
		})
		if e != nil {
			t.Fatal(e)
		}
	}
	seen := map[string]bool{}
	var token []byte
	for page := 0; page < 20; page++ {
		r, e := execution.GetAllHistoryTreeBranches(ctx, &p.GetAllHistoryTreeBranchesRequest{PageSize: 1, NextPageToken: token})
		if e != nil {
			t.Fatal(e)
		}
		for _, b := range r.Branches {
			if !want[b.TreeID] || seen[b.TreeID] {
				t.Fatal("wrong or duplicate branch", b.TreeID)
			}
			seen[b.TreeID] = true
		}
		token = r.NextPageToken
		if len(token) == 0 {
			break
		}
		if page == 19 {
			t.Fatal("fanout did not terminate")
		}
	}
	if len(seen) != len(want) {
		t.Fatal(seen, want)
	}
	large, e := execution.GetAllHistoryTreeBranches(ctx, &p.GetAllHistoryTreeBranchesRequest{PageSize: 1000})
	if e != nil || len(large.Branches) != 2 {
		t.Fatal("global page exceeded local byte budget", e)
	}
	first, e := execution.GetAllHistoryTreeBranches(ctx, &p.GetAllHistoryTreeBranchesRequest{PageSize: 1})
	if e != nil {
		t.Fatal(e)
	}
	altered, e := adapter.NewPartitionedExecutionStore(listener.Addr().String(), []string{"history-1", "history-0", "history-2", "history-3"})
	if e != nil {
		t.Fatal(e)
	}
	defer altered.Close()
	if _, e = altered.GetAllHistoryTreeBranches(ctx, &p.GetAllHistoryTreeBranchesRequest{PageSize: 1, NextPageToken: first.NextPageToken}); e == nil {
		t.Fatal("changed mapping token accepted")
	}
	for _, bad := range [][]string{nil, {}, {"a", "a"}, {""}} {
		if s, e := adapter.NewPartitionedExecutionStore(listener.Addr().String(), bad); e == nil {
			s.Close()
			t.Fatal("bad config accepted", bad)
		}
	}
}
