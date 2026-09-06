package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/persistence"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	native "slatedb.io/slatedb-go/uniffi"
)

type HistoryTasksServer struct {
	wire.UnimplementedHistoryTasksPersistenceServer
	Owner *Owner
}

func (s *HistoryTasksServer) Execute(ctx context.Context, q *wire.HistoryTasksRequest) (*wire.HistoryTasksResult, error) {
	if q == nil || q.ProtocolVersion != 1 || q.Partition != s.Owner.config.Partition || !operationID.MatchString(q.OperationId) || q.Command == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid history-task envelope")
	}
	q = proto.Clone(q).(*wire.HistoryTasksRequest)
	c := q.Command
	if err := persistence.ValidateHistoryTasksCommand(c); err != nil {
		return nil, err
	}
	b, e := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if e != nil {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	d := sha256.Sum256(b)
	if !bytes.Equal(d[:], q.CommandSha256) {
		return nil, status.Error(codes.InvalidArgument, "digest mismatch")
	}
	raw, e := s.Owner.Run(ctx, func(*native.Db) ([]byte, error) {
		out, e := s.Owner.journal(q.OperationId, d[:], historyTasksFamily, func(tx *native.DbTransaction) (*wire.StoredOutcome, error) {
			r, e := applyHistoryTasks(tx, c)
			if e != nil {
				return nil, e
			}
			return &wire.StoredOutcome{Result: &wire.StoredOutcome_HistoryTasksResult{HistoryTasksResult: r}}, nil
		})
		if e != nil {
			return nil, e
		}
		return proto.Marshal(out.GetHistoryTasksResult())
	})
	if e != nil {
		return nil, e
	}
	r := new(wire.HistoryTasksResult)
	if e = proto.Unmarshal(raw, r); e != nil {
		return nil, backend(e)
	}
	return r, nil
}

func applyHistoryTasks(tx *native.DbTransaction, c *wire.HistoryTasksCommand) (*wire.HistoryTasksResult, error) {
	out, err := persistence.ApplyHistoryTasks(context.Background(), legacyClusterTransaction{legacyShardTransaction{tx}}, c)
	if err != nil {
		return nil, legacyExecutionError(err)
	}
	return out.GetHistoryTasksResult(), nil
}
