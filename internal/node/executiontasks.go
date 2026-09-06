package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/persistence"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	native "slatedb.io/slatedb-go/uniffi"
)

type ExecutionTasksServer struct {
	wire.UnimplementedExecutionTasksPersistenceServer
	Owner *Owner
}

func (s *ExecutionTasksServer) Execute(ctx context.Context, q *wire.ExecutionTasksRequest) (*wire.ExecutionTasksResult, error) {
	if q == nil || q.ProtocolVersion != 1 || q.Partition != s.Owner.config.Partition || !operationID.MatchString(q.OperationId) || q.Command == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid execution task envelope")
	}
	q = proto.Clone(q).(*wire.ExecutionTasksRequest)
	c := q.Command
	if err := persistence.ValidateExecutionTasksCommand(c); err != nil {
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
		out, e := s.Owner.journal(q.OperationId, d[:], executionTasksFamily, func(tx *native.DbTransaction) (*wire.StoredOutcome, error) {
			r, e := applyExecutionTasks(tx, c)
			if e != nil {
				return nil, e
			}
			return executionTasksOutcome(r), nil
		})
		var failure *executionFailure
		if errors.As(e, &failure) {
			r := &wire.ExecutionTasksResult{Message: failure.result.Message, Error: wire.ExecutionTasksResult_UNAVAILABLE}
			if failure.result.Error == wire.ExecutionResult_INTERNAL {
				r.Error = wire.ExecutionTasksResult_INTERNAL
			}
			out, e = s.Owner.journal(q.OperationId, d[:], executionTasksFamily, func(*native.DbTransaction) (*wire.StoredOutcome, error) { return executionTasksOutcome(r), nil })
		}
		if e != nil {
			return nil, e
		}
		return proto.Marshal(out.GetExecutionTasksResult())
	})
	if e != nil {
		return nil, e
	}
	r := new(wire.ExecutionTasksResult)
	if e = proto.Unmarshal(raw, r); e != nil {
		return nil, backend(e)
	}
	return r, nil
}
func applyExecutionTasks(tx *native.DbTransaction, c *wire.ExecutionTasksCommand) (*wire.ExecutionTasksResult, error) {
	out, err := persistence.ApplyExecutionTasks(context.Background(), legacyClusterTransaction{legacyShardTransaction{tx}}, c)
	if err != nil {
		return nil, legacyExecutionError(err)
	}
	return out.GetExecutionTasksResult(), nil
}
func executionTasksOutcome(r *wire.ExecutionTasksResult) *wire.StoredOutcome {
	return &wire.StoredOutcome{Result: &wire.StoredOutcome_ExecutionTasksResult{ExecutionTasksResult: r}}
}

func replicationKey(c *wire.ExecutionTasksCommand, id int64) string {
	return persistence.ReplicationTaskKey(c, id)
}
