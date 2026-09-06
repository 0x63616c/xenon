package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"net"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

type historyFixture struct {
	namespaceCase
	TreeID   string     `json:"tree_id"`
	BranchID string     `json:"branch_id"`
	ForkID   string     `json:"fork_id"`
	ShardID  int32      `json:"shard_id"`
	Nodes    [][2]int64 `json:"nodes"`
}

func historyCase(t *testing.T) historyFixture {
	t.Helper()
	raw, e := os.ReadFile("../../proof/history/case.json")
	if e != nil {
		t.Fatal(e)
	}
	var c historyFixture
	if e = json.Unmarshal(raw, &c); e != nil || c.SchemaVersion != 1 || c.Backend != "memory" || len(c.Nodes) != 4 || c.PageSize != 1 || c.Fault != "drop_first_completed_history_append" {
		t.Fatal("invalid history fixture", e)
	}
	return c
}

type historyProxy struct {
	wire.UnimplementedHistoryPersistenceServer
	backend wire.HistoryPersistenceClient
	mu      sync.Mutex
	first   *wire.HistoryRequest
	retry   bool
}

func (s *historyProxy) Execute(ctx context.Context, q *wire.HistoryRequest) (*wire.HistoryResult, error) {
	r, e := s.backend.Execute(ctx, q)
	if e != nil {
		return nil, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if q.Command.Kind == wire.HistoryCommand_APPEND {
		if s.first == nil {
			s.first = proto.Clone(q).(*wire.HistoryRequest)
			return nil, status.Error(codes.Unavailable, "declared lost append reply")
		}
		if s.first.OperationId == q.OperationId {
			s.retry = proto.Equal(s.first, q)
		}
	}
	return r, nil
}
func TestHistoryRPC(t *testing.T) {
	c := historyCase(t)
	address := startNamespaceNode(t, c.namespaceCase)
	conn, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	proxy := &historyProxy{backend: wire.NewHistoryPersistenceClient(conn)}
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	srv := grpc.NewServer()
	wire.RegisterHistoryPersistenceServer(srv, proxy)
	go srv.Serve(l)
	defer srv.Stop()
	s, e := NewHistoryStore(l.Addr().String(), c.Partition)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.TestTimeoutSeconds)*time.Second)
	defer cancel()
	branch := &persistencespb.HistoryBranch{TreeId: c.TreeID, BranchId: c.BranchID}
	token, e := proto.Marshal(branch)
	if e != nil {
		t.Fatal(e)
	}
	tree := &commonpb.DataBlob{Data: []byte{0, 255, 3}, EncodingType: enumspb.ENCODING_TYPE_PROTO3}
	for i, pair := range c.Nodes {
		e = s.AppendHistoryNodes(ctx, &p.InternalAppendHistoryNodesRequest{ShardID: c.ShardID, BranchInfo: branch, BranchToken: token, IsNewBranch: i == 0, TreeInfo: tree, Node: p.InternalHistoryNode{NodeID: pair[0], TransactionID: pair[1], PrevTransactionID: pair[1] - 1, Events: &commonpb.DataBlob{Data: []byte{byte(i), 255}, EncodingType: enumspb.ENCODING_TYPE_JSON}}})
		if e != nil {
			t.Fatal(e)
		}
	}
	proxy.mu.Lock()
	observed := proxy.first != nil && proxy.retry
	proxy.mu.Unlock()
	if !observed {
		t.Fatal("lost reply/retry not observed")
	}
	for _, reverse := range []bool{false, true} {
		for _, metadata := range []bool{false, true} {
			var got [][2]int64
			var page []byte
			for turns := 0; turns < 10; turns++ {
				r, e := s.ReadHistoryBranch(ctx, &p.InternalReadHistoryBranchRequest{ShardID: c.ShardID, BranchToken: token, BranchID: c.BranchID, MinNodeID: 1, MaxNodeID: 9, PageSize: c.PageSize, NextPageToken: page, ReverseOrder: reverse, MetadataOnly: metadata})
				if e != nil {
					t.Fatal(e)
				}
				for _, n := range r.Nodes {
					got = append(got, [2]int64{n.NodeID, n.TransactionID})
					if metadata && n.Events != nil {
						t.Fatal("metadata returned payload")
					}
					if !metadata && (n.Events == nil || n.Events.EncodingType != enumspb.ENCODING_TYPE_JSON || len(n.Events.Data) != 2) {
						t.Fatal("payload lost")
					}
				}
				page = r.NextPageToken
				if len(page) == 0 {
					break
				}
				if turns == 9 {
					t.Fatal("pagination stuck")
				}
			}
			want := append([][2]int64(nil), c.Nodes...)
			if reverse {
				for i, j := 0, len(want)-1; i < j; i, j = i+1, j-1 {
					want[i], want[j] = want[j], want[i]
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatal("ordering/completeness", reverse, metadata, got, want)
			}
		}
	}
	// Exact duplicate key is PostgreSQL upsert; no extra node is created.
	if e = s.AppendHistoryNodes(ctx, &p.InternalAppendHistoryNodesRequest{ShardID: c.ShardID, BranchInfo: branch, Node: p.InternalHistoryNode{NodeID: 1, TransactionID: 9001, Events: tree}}); e != nil {
		t.Fatal(e)
	}
	r, e := s.ReadHistoryBranch(ctx, &p.InternalReadHistoryBranchRequest{ShardID: c.ShardID, BranchToken: token, BranchID: c.BranchID, MinNodeID: 1, MaxNodeID: 3, PageSize: 10})
	if e != nil || len(r.Nodes) != 2 || !proto.Equal(r.Nodes[0].Events, tree) {
		t.Fatal("upsert/range", r, e)
	}
	fork := &persistencespb.HistoryBranch{TreeId: c.TreeID, BranchId: c.ForkID, Ancestors: []*persistencespb.HistoryBranchRange{{BranchId: c.BranchID, BeginNodeId: 1, EndNodeId: 3}}}
	forkToken, _ := proto.Marshal(fork)
	if e = s.ForkHistoryBranch(ctx, &p.InternalForkHistoryBranchRequest{ShardID: c.ShardID, ForkBranchInfo: branch, NewBranchID: c.ForkID, NewBranchToken: forkToken, ForkNodeID: 3, TreeInfo: tree}); e != nil {
		t.Fatal(e)
	}
	trees, e := s.GetHistoryTreeContainingBranch(ctx, &p.InternalGetHistoryTreeContainingBranchRequest{ShardID: c.ShardID, BranchToken: token})
	if e != nil || len(trees.TreeInfos) != 2 {
		t.Fatal(trees, e)
	}
	var all []p.InternalHistoryBranchDetail
	var page []byte
	for i := 0; i < 4; i++ {
		r, e := s.GetAllHistoryTreeBranches(ctx, &p.GetAllHistoryTreeBranchesRequest{PageSize: 1, NextPageToken: page})
		if e != nil {
			t.Fatal(e)
		}
		all = append(all, r.Branches...)
		page = r.NextPageToken
		if len(page) == 0 {
			break
		}
	}
	if len(all) != 2 || all[0].TreeID != c.TreeID || !bytes.Equal(all[0].Data, tree.Data) {
		t.Fatal(all)
	}
	e = s.DeleteHistoryNodes(ctx, &p.InternalDeleteHistoryNodesRequest{ShardID: c.ShardID, BranchInfo: fork, NodeID: 1, TransactionID: 9001})
	var invalid *p.InvalidPersistenceRequestError
	if !errors.As(e, &invalid) {
		t.Fatal("ancestor delete accepted", e)
	}
	if e = s.DeleteHistoryNodes(ctx, &p.InternalDeleteHistoryNodesRequest{ShardID: c.ShardID, BranchInfo: branch, NodeID: 1, TransactionID: 1001}); e != nil {
		t.Fatal(e)
	}
	if e = s.DeleteHistoryBranch(ctx, &p.InternalDeleteHistoryBranchRequest{ShardID: c.ShardID, BranchInfo: fork, BranchRanges: []p.InternalDeleteHistoryBranchRange{{BranchId: c.BranchID, BeginNodeId: 3}}}); e != nil {
		t.Fatal(e)
	}
	r, e = s.ReadHistoryBranch(ctx, &p.InternalReadHistoryBranchRequest{ShardID: c.ShardID, BranchToken: token, BranchID: c.BranchID, MinNodeID: 1, MaxNodeID: 9, PageSize: 10})
	if e != nil || len(r.Nodes) != 1 || r.Nodes[0].TransactionID != 9001 {
		t.Fatal("range delete boundary", r, e)
	}
	trees, e = s.GetHistoryTreeContainingBranch(ctx, &p.InternalGetHistoryTreeContainingBranchRequest{ShardID: c.ShardID, BranchToken: token})
	if e != nil || len(trees.TreeInfos) != 1 {
		t.Fatal(trees, e)
	}
}

type historyCancelClient struct{ code codes.Code }

func (c historyCancelClient) Execute(ctx context.Context, _ *wire.HistoryRequest, _ ...grpc.CallOption) (*wire.HistoryResult, error) {
	if ctx.Err() != nil {
		panic("live context required")
	}
	return nil, status.Error(c.code, "remote deadline")
}
func TestHistoryTimeoutTypes(t *testing.T) {
	c := historyCase(t)
	for _, code := range []codes.Code{codes.Canceled, codes.DeadlineExceeded} {
		for _, isNew := range []bool{false, true} {
			s := &HistoryStore{client: historyCancelClient{code}, invocationTimeout: time.Minute}
			e := s.AppendHistoryNodes(context.Background(), &p.InternalAppendHistoryNodesRequest{BranchInfo: &persistencespb.HistoryBranch{TreeId: c.TreeID, BranchId: c.BranchID}, IsNewBranch: isNew, TreeInfo: &commonpb.DataBlob{}, Node: p.InternalHistoryNode{NodeID: 1, Events: &commonpb.DataBlob{}}})
			if isNew {
				var unavailable *serviceerror.Unavailable
				if !errors.As(e, &unavailable) {
					t.Fatal(e)
				}
			} else {
				var timeout *p.AppendHistoryTimeoutError
				if !errors.As(e, &timeout) {
					t.Fatal(e)
				}
			}
		}
	}
}
func TestHistoryByteBoundedPagination(t *testing.T) {
	c := historyCase(t)
	address := startNamespaceNode(t, c.namespaceCase)
	s, e := NewHistoryStore(address, c.Partition)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	b := &persistencespb.HistoryBranch{TreeId: c.TreeID, BranchId: c.BranchID}
	token, _ := proto.Marshal(b)
	for i := 0; i < 3; i++ {
		if e = s.AppendHistoryNodes(ctx, &p.InternalAppendHistoryNodesRequest{BranchInfo: b, ShardID: c.ShardID, Node: p.InternalHistoryNode{NodeID: int64(i + 1), TransactionID: 99, Events: &commonpb.DataBlob{Data: bytes.Repeat([]byte{byte(i + 1)}, c.NearLimitDataBytes), EncodingType: enumspb.ENCODING_TYPE_PROTO3}}}); e != nil {
			t.Fatal(e)
		}
	}
	first, e := s.ReadHistoryBranch(ctx, &p.InternalReadHistoryBranchRequest{ShardID: c.ShardID, BranchToken: token, BranchID: c.BranchID, MinNodeID: 1, MaxNodeID: 4, PageSize: 1000})
	if e != nil || len(first.Nodes) != 2 || len(first.NextPageToken) == 0 {
		t.Fatal(first, e)
	}
	last, e := s.ReadHistoryBranch(ctx, &p.InternalReadHistoryBranchRequest{ShardID: c.ShardID, BranchToken: token, BranchID: c.BranchID, MinNodeID: 1, MaxNodeID: 4, PageSize: 1000, NextPageToken: first.NextPageToken})
	if e != nil || len(last.Nodes) != 1 || last.Nodes[0].NodeID != 3 || len(last.NextPageToken) != 0 {
		t.Fatal(last, e)
	}
}
