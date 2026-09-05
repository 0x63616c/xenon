package adapter

import (
	"context"
	"crypto/sha256"
	"errors"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"time"
)

// HistoryStore is only the seven-operation history component of ExecutionStore.
// It deliberately does not claim to implement unfinished workflow operations.
type HistoryStore struct {
	connection        *grpc.ClientConn
	client            wire.HistoryPersistenceClient
	partition         string
	invocationTimeout time.Duration
	*p.HistoryBranchUtilImpl
}

func NewHistoryStore(address, partition string) (*HistoryStore, error) {
	conn, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		return nil, e
	}
	return &HistoryStore{connection: conn, client: wire.NewHistoryPersistenceClient(conn), partition: partition, invocationTimeout: 30 * time.Second, HistoryBranchUtilImpl: p.NewHistoryBranchUtil(serialization.NewSerializer())}, nil
}
func (s *HistoryStore) Close() {
	if s.connection != nil {
		_ = s.connection.Close()
	}
}
func (s *HistoryStore) GetName() string                           { return "xenon" }
func (s *HistoryStore) GetHistoryBranchUtil() p.HistoryBranchUtil { return s.HistoryBranchUtilImpl }
func (s *HistoryStore) invokeHistory(ctx context.Context, c *wire.HistoryCommand) (*wire.HistoryResult, error) {
	ctx, cancel := context.WithTimeout(ctx, s.invocationTimeout)
	defer cancel()
	raw, e := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if e != nil {
		return nil, e
	}
	d := sha256.Sum256(raw)
	q := &wire.HistoryRequest{ProtocolVersion: 1, Partition: s.partition, OperationId: uuid.NewString(), CommandSha256: d[:], Command: c}
	for attempt := 0; attempt < 3; attempt++ {
		r, e := s.client.Execute(ctx, q)
		if e == nil {
			if r == nil {
				return nil, serviceerror.NewInternal("nil history result")
			}
			switch r.Error {
			case wire.HistoryResult_NONE:
				return r, nil
			case wire.HistoryResult_UNAVAILABLE:
				return nil, serviceerror.NewUnavailable(r.Message)
			case wire.HistoryResult_CONDITION_FAILED:
				return nil, &p.ConditionFailedError{Msg: r.Message}
			case wire.HistoryResult_INVALID_REQUEST:
				return nil, &p.InvalidPersistenceRequestError{Msg: r.Message}
			case wire.HistoryResult_APPEND_TIMEOUT:
				return nil, &p.AppendHistoryTimeoutError{Msg: r.Message}
			default:
				return nil, serviceerror.NewInternal(r.Message)
			}
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		switch status.Code(e) {
		case codes.Canceled:
			return nil, context.Canceled
		case codes.DeadlineExceeded:
			return nil, context.DeadlineExceeded
		}
		if status.Code(e) != codes.Unavailable || attempt == 2 {
			return nil, serviceerror.FromStatus(status.Convert(e))
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 20 * time.Millisecond):
		}
	}
	panic("unreachable")
}
func historyIDs(b *persistencespb.HistoryBranch) ([]byte, []byte, []byte, error) {
	if b == nil {
		return nil, nil, nil, serviceerror.NewInvalidArgument("nil history branch")
	}
	tree, e := uuid.Parse(b.TreeId)
	if e != nil {
		return nil, nil, nil, e
	}
	branch, e := uuid.Parse(b.BranchId)
	if e != nil {
		return nil, nil, nil, e
	}
	raw, e := proto.Marshal(b)
	return tree[:], branch[:], raw, e
}
func historyBlob(b *commonpb.DataBlob) *wire.HistoryBlob {
	if b == nil {
		return nil
	}
	return &wire.HistoryBlob{Data: b.Data, Encoding: int32(b.EncodingType)}
}
func fromHistoryBlob(b *wire.HistoryBlob) *commonpb.DataBlob {
	if b == nil {
		return nil
	}
	return &commonpb.DataBlob{Data: b.Data, EncodingType: enumspb.EncodingType(b.Encoding)}
}
func historyCommand(k wire.HistoryCommand_Kind, shard int32, b *persistencespb.HistoryBranch) (*wire.HistoryCommand, error) {
	t, id, raw, e := historyIDs(b)
	if e != nil {
		return nil, e
	}
	return &wire.HistoryCommand{Kind: k, ShardId: shard, TreeId: t, BranchId: id, BranchInfo: raw}, nil
}
func (s *HistoryStore) AppendHistoryNodes(ctx context.Context, q *p.InternalAppendHistoryNodesRequest) error {
	if q == nil || q.Node.Events == nil || (q.IsNewBranch && q.TreeInfo == nil) {
		return serviceerror.NewInvalidArgument("nil history append/blob")
	}
	c, e := historyCommand(wire.HistoryCommand_APPEND, q.ShardID, q.BranchInfo)
	if e != nil {
		return e
	}
	c.BranchToken = q.BranchToken
	c.IsNewBranch = q.IsNewBranch
	c.Info = q.Info
	c.TreeInfo = historyBlob(q.TreeInfo)
	c.Node = &wire.HistoryNodeRecord{NodeId: q.Node.NodeID, TransactionId: q.Node.TransactionID, PreviousTransactionId: q.Node.PrevTransactionID, Events: historyBlob(q.Node.Events)}
	_, e = s.invokeHistory(ctx, c)
	if errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
		if q.IsNewBranch {
			return serviceerror.NewUnavailable(e.Error())
		}
		return &p.AppendHistoryTimeoutError{Msg: e.Error()}
	}
	return e
}
func (s *HistoryStore) DeleteHistoryNodes(ctx context.Context, q *p.InternalDeleteHistoryNodesRequest) error {
	if q == nil {
		return serviceerror.NewInvalidArgument("nil history delete")
	}
	c, e := historyCommand(wire.HistoryCommand_DELETE_NODE, q.ShardID, q.BranchInfo)
	if e != nil {
		return e
	}
	c.BranchToken = q.BranchToken
	c.Node = &wire.HistoryNodeRecord{NodeId: q.NodeID, TransactionId: q.TransactionID}
	_, e = s.invokeHistory(ctx, c)
	return e
}
func (s *HistoryStore) ReadHistoryBranch(ctx context.Context, q *p.InternalReadHistoryBranchRequest) (*p.InternalReadHistoryBranchResponse, error) {
	if q == nil {
		return nil, serviceerror.NewInvalidArgument("nil history read")
	}
	b, e := s.ParseHistoryBranchInfo(q.BranchToken)
	if e != nil {
		return nil, e
	}
	b.BranchId = q.BranchID
	c, e := historyCommand(wire.HistoryCommand_READ, q.ShardID, b)
	if e != nil {
		return nil, e
	}
	c.BranchToken = q.BranchToken
	c.MinNodeId = q.MinNodeID
	c.MaxNodeId = q.MaxNodeID
	c.PageSize = int64(q.PageSize)
	c.NextPageToken = q.NextPageToken
	c.MetadataOnly = q.MetadataOnly
	c.ReverseOrder = q.ReverseOrder
	r, e := s.invokeHistory(ctx, c)
	if e != nil {
		return nil, e
	}
	out := &p.InternalReadHistoryBranchResponse{NextPageToken: r.NextPageToken}
	for _, n := range r.Nodes {
		if n == nil {
			return nil, serviceerror.NewInternal("nil history node")
		}
		out.Nodes = append(out.Nodes, p.InternalHistoryNode{NodeID: n.NodeId, TransactionID: n.TransactionId, PrevTransactionID: n.PreviousTransactionId, Events: fromHistoryBlob(n.Events)})
	}
	return out, nil
}
func (s *HistoryStore) ForkHistoryBranch(ctx context.Context, q *p.InternalForkHistoryBranchRequest) error {
	if q == nil || q.TreeInfo == nil {
		return serviceerror.NewInvalidArgument("nil history fork/blob")
	}
	c, e := historyCommand(wire.HistoryCommand_FORK, q.ShardID, q.ForkBranchInfo)
	if e != nil {
		return e
	}
	id, e := uuid.Parse(q.NewBranchID)
	if e != nil {
		return e
	}
	c.BranchId = id[:]
	c.NewBranchToken = q.NewBranchToken
	c.ForkNodeId = q.ForkNodeID
	c.Info = q.Info
	c.TreeInfo = historyBlob(q.TreeInfo)
	_, e = s.invokeHistory(ctx, c)
	return e
}
func (s *HistoryStore) DeleteHistoryBranch(ctx context.Context, q *p.InternalDeleteHistoryBranchRequest) error {
	if q == nil {
		return serviceerror.NewInvalidArgument("nil history branch delete")
	}
	c, e := historyCommand(wire.HistoryCommand_DELETE_BRANCH, q.ShardID, q.BranchInfo)
	if e != nil {
		return e
	}
	c.BranchToken = q.BranchToken
	for _, r := range q.BranchRanges {
		id, e := uuid.Parse(r.BranchId)
		if e != nil {
			return e
		}
		c.Ranges = append(c.Ranges, &wire.HistoryDeleteRange{BranchId: id[:], BeginNodeId: r.BeginNodeId})
	}
	_, e = s.invokeHistory(ctx, c)
	return e
}
func (s *HistoryStore) GetAllHistoryTreeBranches(ctx context.Context, q *p.GetAllHistoryTreeBranchesRequest) (*p.InternalGetAllHistoryTreeBranchesResponse, error) {
	if q == nil {
		return nil, serviceerror.NewInvalidArgument("nil history list")
	}
	r, e := s.invokeHistory(ctx, &wire.HistoryCommand{Kind: wire.HistoryCommand_LIST_TREES, PageSize: int64(q.PageSize), NextPageToken: q.NextPageToken})
	if e != nil {
		return nil, e
	}
	out := &p.InternalGetAllHistoryTreeBranchesResponse{NextPageToken: r.NextPageToken}
	for _, b := range r.Trees {
		if b == nil || b.Info == nil {
			return nil, serviceerror.NewInternal("nil history tree")
		}
		tree, e := uuid.FromBytes(b.TreeId)
		if e != nil {
			return nil, serviceerror.NewInternal("invalid tree ID")
		}
		branch, e := uuid.FromBytes(b.BranchId)
		if e != nil {
			return nil, serviceerror.NewInternal("invalid branch ID")
		}
		out.Branches = append(out.Branches, p.InternalHistoryBranchDetail{TreeID: tree.String(), BranchID: branch.String(), Data: b.Info.Data, Encoding: enumspb.EncodingType(b.Info.Encoding).String()})
	}
	return out, nil
}
func (s *HistoryStore) GetHistoryTreeContainingBranch(ctx context.Context, q *p.InternalGetHistoryTreeContainingBranchRequest) (*p.InternalGetHistoryTreeContainingBranchResponse, error) {
	if q == nil {
		return nil, serviceerror.NewInvalidArgument("nil history tree request")
	}
	b, e := s.ParseHistoryBranchInfo(q.BranchToken)
	if e != nil {
		return nil, e
	}
	c, e := historyCommand(wire.HistoryCommand_GET_TREE, q.ShardID, b)
	if e != nil {
		return nil, e
	}
	c.BranchToken = q.BranchToken
	r, e := s.invokeHistory(ctx, c)
	if e != nil {
		return nil, e
	}
	out := new(p.InternalGetHistoryTreeContainingBranchResponse)
	for _, tree := range r.Trees {
		if tree == nil || tree.Info == nil {
			return nil, serviceerror.NewInternal("nil tree info")
		}
		out.TreeInfos = append(out.TreeInfos, fromHistoryBlob(tree.Info))
	}
	return out, nil
}
