package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	native "slatedb.io/slatedb-go/uniffi"
)

type HistoryServer struct {
	wire.UnimplementedHistoryPersistenceServer
	Owner *Owner
}

func (s *HistoryServer) Execute(ctx context.Context, q *wire.HistoryRequest) (*wire.HistoryResult, error) {
	if q == nil || q.ProtocolVersion != 1 || q.Partition != s.Owner.config.Partition || !operationID.MatchString(q.OperationId) || q.Command == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid history envelope")
	}
	q = proto.Clone(q).(*wire.HistoryRequest)
	c := q.Command
	if c.Kind < wire.HistoryCommand_APPEND || c.Kind > wire.HistoryCommand_GET_TREE || c.ShardId < 0 || len(c.BranchInfo) > 1024*1024 || len(c.BranchToken) > 1024*1024 || len(c.NewBranchToken) > 1024*1024 || len(c.Info) > 1024*1024 || len(c.NextPageToken) > 1024 || len(c.Ranges) > 1000 {
		return nil, status.Error(codes.InvalidArgument, "invalid history command")
	}
	if c.Kind != wire.HistoryCommand_LIST_TREES && (len(c.TreeId) != 16 || len(c.BranchId) != 16) {
		return nil, status.Error(codes.InvalidArgument, "invalid history UUID")
	}
	if (c.Kind == wire.HistoryCommand_READ || c.Kind == wire.HistoryCommand_LIST_TREES) && (c.PageSize < 1 || c.PageSize > 1000) {
		return nil, status.Error(codes.InvalidArgument, "history page size must be 1..1000")
	}
	if c.Node != nil && (c.Node.NodeId < 1 || (c.Node.Events != nil && len(c.Node.Events.Data) > 1024*1024)) {
		return nil, status.Error(codes.InvalidArgument, "invalid history node")
	}
	if c.TreeInfo != nil && len(c.TreeInfo.Data) > 1024*1024 {
		return nil, status.Error(codes.InvalidArgument, "history tree blob too large")
	}
	if c.Kind == wire.HistoryCommand_APPEND && (c.Node == nil || c.Node.Events == nil || (c.IsNewBranch && c.TreeInfo == nil)) {
		return nil, status.Error(codes.InvalidArgument, "missing append node/tree")
	}
	if c.Kind == wire.HistoryCommand_DELETE_NODE && c.Node == nil {
		return nil, status.Error(codes.InvalidArgument, "missing delete node")
	}
	if c.Kind == wire.HistoryCommand_FORK && c.TreeInfo == nil {
		return nil, status.Error(codes.InvalidArgument, "missing fork tree")
	}
	for _, r := range c.Ranges {
		if r == nil || len(r.BranchId) != 16 {
			return nil, status.Error(codes.InvalidArgument, "invalid branch range")
		}
	}
	encoded, e := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if e != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid command encoding")
	}
	d := sha256.Sum256(encoded)
	if !bytes.Equal(d[:], q.CommandSha256) {
		return nil, status.Error(codes.InvalidArgument, "digest mismatch")
	}
	raw, e := s.Owner.Run(ctx, func(*native.Db) ([]byte, error) {
		o, e := s.Owner.journal(q.OperationId, d[:], historyFamily, func(tx *native.DbTransaction) (*wire.StoredOutcome, error) {
			r, e := applyHistory(tx, c)
			if e != nil {
				return nil, e
			}
			return &wire.StoredOutcome{Result: &wire.StoredOutcome_HistoryResult{HistoryResult: r}}, nil
		})
		if e != nil {
			return nil, e
		}
		return proto.Marshal(o.GetHistoryResult())
	})
	if e != nil {
		return nil, e
	}
	r := new(wire.HistoryResult)
	if e = proto.Unmarshal(raw, r); e != nil {
		return nil, backend(e)
	}
	return r, nil
}
func historyTreePrefix(shard int32, tree []byte) string {
	return fmt.Sprintf("v1/history/tree/%010d/%x/", shard, tree)
}
func historyTreeKey(shard int32, tree, branch []byte) string {
	return fmt.Sprintf("%s%x", historyTreePrefix(shard, tree), branch)
}
func historyNodePrefix(shard int32, tree, branch []byte) string {
	return fmt.Sprintf("v1/history/node/%010d/%x/%x/", shard, tree, branch)
}
func historyNodeKey(shard int32, tree, branch []byte, n *wire.HistoryNodeRecord) string {
	return fmt.Sprintf("%s%016x/%016x", historyNodePrefix(shard, tree, branch), uint64(n.NodeId)^1<<63, ^(uint64(n.TransactionId) ^ 1<<63))
}
func putHistory(tx *native.DbTransaction, key string, m proto.Message) error {
	raw, e := proto.Marshal(m)
	if e != nil {
		return backend(e)
	}
	return put(tx, key, raw)
}
func historyScan(tx *native.DbTransaction, prefix string, reverse bool, visit func([]byte, []byte) (bool, error)) error {
	order := native.IterationOrderAscending
	if reverse {
		order = native.IterationOrderDescending
	}
	it, e := tx.ScanPrefixWithOptions([]byte(prefix), native.KeyRange{}, native.ScanOptions{DurabilityFilter: native.DurabilityLevelRemote, ReadAheadBytes: 1 << 20, CacheBlocks: true, MaxFetchTasks: 1, Order: &order})
	if e != nil {
		return backend(e)
	}
	defer it.Destroy()
	for {
		row, e := it.Next()
		if e != nil {
			return backend(e)
		}
		if row == nil {
			return nil
		}
		stop, e := visit(row.Key, row.Value)
		if e != nil || stop {
			return e
		}
	}
}

type historyCursor struct {
	LastNodeID int64
	LastTxnID  int64
}

func applyHistory(tx *native.DbTransaction, c *wire.HistoryCommand) (*wire.HistoryResult, error) {
	r := new(wire.HistoryResult)
	switch c.Kind {
	case wire.HistoryCommand_APPEND:
		if e := putHistory(tx, historyNodeKey(c.ShardId, c.TreeId, c.BranchId, c.Node), c.Node); e != nil {
			return nil, e
		}
		if c.IsNewBranch {
			if e := putHistory(tx, historyTreeKey(c.ShardId, c.TreeId, c.BranchId), &wire.HistoryTreeRecord{ShardId: c.ShardId, TreeId: c.TreeId, BranchId: c.BranchId, Info: c.TreeInfo}); e != nil {
				return nil, e
			}
		}
	case wire.HistoryCommand_DELETE_NODE:
		b := new(persistencespb.HistoryBranch)
		if e := proto.Unmarshal(c.BranchInfo, b); e != nil {
			return &wire.HistoryResult{Error: wire.HistoryResult_INVALID_REQUEST, Message: "invalid branch info"}, nil
		}
		if c.Node.NodeId < p.GetBeginNodeID(b) {
			return &wire.HistoryResult{Error: wire.HistoryResult_INVALID_REQUEST, Message: "cannot append to ancestors' nodes"}, nil
		}
		if e := tx.Delete([]byte(historyNodeKey(c.ShardId, c.TreeId, c.BranchId, c.Node))); e != nil {
			return nil, backend(e)
		}
	case wire.HistoryCommand_FORK:
		if e := putHistory(tx, historyTreeKey(c.ShardId, c.TreeId, c.BranchId), &wire.HistoryTreeRecord{ShardId: c.ShardId, TreeId: c.TreeId, BranchId: c.BranchId, Info: c.TreeInfo}); e != nil {
			return nil, e
		}
	case wire.HistoryCommand_DELETE_BRANCH:
		if e := tx.Delete([]byte(historyTreeKey(c.ShardId, c.TreeId, c.BranchId))); e != nil {
			return nil, backend(e)
		}
		for _, br := range c.Ranges {
			e := historyScan(tx, historyNodePrefix(c.ShardId, c.TreeId, br.BranchId), false, func(key, value []byte) (bool, error) {
				n := new(wire.HistoryNodeRecord)
				if e := proto.Unmarshal(value, n); e != nil {
					return false, backend(e)
				}
				if n.NodeId >= br.BeginNodeId {
					return false, backend(tx.Delete(key))
				}
				return false, nil
			})
			if e != nil {
				return nil, e
			}
		}
	case wire.HistoryCommand_READ:
		var cursor *historyCursor
		if len(c.NextPageToken) > 0 {
			cursor = new(historyCursor)
			if e := json.Unmarshal(c.NextPageToken, cursor); e != nil {
				return &wire.HistoryResult{Error: wire.HistoryResult_INTERNAL, Message: "invalid history page token"}, nil
			}
		}
		truncated := false
		e := historyScan(tx, historyNodePrefix(c.ShardId, c.TreeId, c.BranchId), c.ReverseOrder, func(key, value []byte) (bool, error) {
			n := new(wire.HistoryNodeRecord)
			if e := proto.Unmarshal(value, n); e != nil {
				return false, backend(e)
			}
			if string(key) != historyNodeKey(c.ShardId, c.TreeId, c.BranchId, n) {
				return false, status.Error(codes.Unavailable, "history node key mismatch")
			}
			if n.NodeId < c.MinNodeId || n.NodeId >= c.MaxNodeId {
				return false, nil
			}
			if cursor != nil {
				after := n.NodeId > cursor.LastNodeID || (n.NodeId == cursor.LastNodeID && n.TransactionId < cursor.LastTxnID)
				if c.ReverseOrder {
					after = n.NodeId < cursor.LastNodeID || (n.NodeId == cursor.LastNodeID && n.TransactionId > cursor.LastTxnID)
				}
				if !after {
					return false, nil
				}
			}
			if c.MetadataOnly {
				n.Events = nil
			}
			r.Nodes = append(r.Nodes, n)
			r.NextPageToken, _ = json.Marshal(historyCursor{n.NodeId, n.TransactionId})
			if proto.Size(r) > 3*1024*1024 {
				r.Nodes = r.Nodes[:len(r.Nodes)-1]
				if len(r.Nodes) == 0 {
					return true, status.Error(codes.ResourceExhausted, "history node exceeds response budget")
				}
				truncated = true
				return true, nil
			}
			if int64(len(r.Nodes)) == c.PageSize {
				truncated = true
				return true, nil
			}
			return false, nil
		})
		if e != nil {
			return nil, e
		}
		if truncated {
			last := r.Nodes[len(r.Nodes)-1]
			r.NextPageToken, _ = json.Marshal(historyCursor{last.NodeId, last.TransactionId})
		} else {
			r.NextPageToken = nil
		}
	case wire.HistoryCommand_GET_TREE, wire.HistoryCommand_LIST_TREES:
		prefix := "v1/history/tree/"
		if c.Kind == wire.HistoryCommand_GET_TREE {
			prefix = historyTreePrefix(c.ShardId, c.TreeId)
		}
		var lastKey string
		if len(c.NextPageToken) > 0 {
			if e := json.Unmarshal(c.NextPageToken, &lastKey); e != nil {
				return &wire.HistoryResult{Error: wire.HistoryResult_INTERNAL, Message: "invalid history tree token"}, nil
			}
		}
		truncated := false
		var keys []string
		e := historyScan(tx, prefix, false, func(key, value []byte) (bool, error) {
			if string(key) <= lastKey {
				return false, nil
			}
			tree := new(wire.HistoryTreeRecord)
			if e := proto.Unmarshal(value, tree); e != nil {
				return false, backend(e)
			}
			if tree.Info == nil || string(key) != historyTreeKey(tree.ShardId, tree.TreeId, tree.BranchId) {
				return false, status.Error(codes.Unavailable, "history tree key mismatch")
			}
			r.Trees = append(r.Trees, tree)
			keys = append(keys, string(key))
			if c.Kind == wire.HistoryCommand_LIST_TREES {
				r.NextPageToken, _ = json.Marshal(string(key))
			}
			if proto.Size(r) > 3*1024*1024 {
				if c.Kind == wire.HistoryCommand_GET_TREE {
					return true, status.Error(codes.ResourceExhausted, "complete tree result exceeds response budget")
				}
				r.Trees = r.Trees[:len(r.Trees)-1]
				keys = keys[:len(keys)-1]
				if len(r.Trees) == 0 {
					return true, status.Error(codes.ResourceExhausted, "history tree exceeds response budget")
				}
				truncated = true
				return true, nil
			}
			if c.Kind == wire.HistoryCommand_LIST_TREES && int64(len(r.Trees)) == c.PageSize {
				truncated = true
				return true, nil
			}
			return false, nil
		})
		if e != nil {
			return nil, e
		}
		if truncated {
			r.NextPageToken, _ = json.Marshal(keys[len(keys)-1])
		} else {
			r.NextPageToken = nil
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "unknown history operation")
	}
	return r, nil
}
