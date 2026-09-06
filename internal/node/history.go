package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/persistence"
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
	if err := persistence.ValidateHistoryCommand(c); err != nil {
		return nil, err
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

// Owner.Run and journal retain legacy whole-operation serialization/durability.
func applyHistory(tx *native.DbTransaction, c *wire.HistoryCommand) (*wire.HistoryResult, error) {
	out, err := persistence.ApplyHistory(context.Background(), legacyClusterTransaction{legacyShardTransaction{tx}}, c)
	if err != nil {
		return nil, err
	}
	return out.GetHistoryResult(), nil
}
