package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/persistence"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// executionFailure aborts all staged changes before its outcome is journaled.
type executionFailure struct{ result *wire.ExecutionResult }

func (e *executionFailure) Error() string { return e.result.Message }

type ExecutionServer struct {
	wire.UnimplementedExecutionPersistenceServer
	Owner *Owner
}

func (s *ExecutionServer) Execute(ctx context.Context, q *wire.ExecutionRequest) (*wire.ExecutionResult, error) {
	if q == nil || q.ProtocolVersion != 1 || q.Partition != s.Owner.config.Partition || !operationID.MatchString(q.OperationId) || q.Command == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid execution envelope")
	}
	q = proto.Clone(q).(*wire.ExecutionRequest)
	c := q.Command
	if e := validateExecution(c); e != nil {
		return nil, e
	}
	encoded, e := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if e != nil {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	d := sha256.Sum256(encoded)
	if !bytes.Equal(d[:], q.CommandSha256) {
		return nil, status.Error(codes.InvalidArgument, "digest mismatch")
	}
	raw, e := s.runExecutionResult(ctx, q, d[:])
	if e != nil {
		return nil, e
	}
	r := new(wire.ExecutionResult)
	if e = proto.Unmarshal(raw, r); e != nil {
		return nil, backend(e)
	}
	return r, nil
}

// runExecutionResult owns the entire gate callback: replay lookup and history
// prewrites precede the final root journal. Only that durable root outcome is
// returned; no caller callback or database read can run after its commit. This
// gives the same fencing point for fresh, replayed and logical-error results.
func (s *ExecutionServer) runExecutionResult(ctx context.Context, q *wire.ExecutionRequest, digest []byte) ([]byte, error) {
	c := q.Command
	return s.Owner.run(ctx, func(partitions.Writer) ([]byte, error) {
		// Check the root before independent history prewrites. A replay must never
		// recreate events removed after the original completed operation.
		tx, e := s.Owner.writer.Begin(context.Background())
		if e != nil {
			return nil, backend(e)
		}
		saved, e := get(tx, "v1/outcome/"+q.OperationId)
		_ = tx.Abort()
		if e != nil {
			return nil, e
		}
		if saved == nil {
			for index, h := range c.HistoryPrewrites {
				b, e := proto.MarshalOptions{Deterministic: true}.Marshal(h)
				if e != nil {
					return nil, backend(e)
				}
				hd := sha256.Sum256(b)
				_, e = s.Owner.journal(fmt.Sprintf("%s-h-%d", q.OperationId, index), hd[:], historyFamily, func(tx partitions.Transaction) (*wire.StoredOutcome, error) {
					r, e := applyHistory(tx, h)
					if e != nil {
						return nil, e
					}
					return &wire.StoredOutcome{Result: &wire.StoredOutcome_HistoryResult{HistoryResult: r}}, nil
				})
				if e != nil {
					return nil, e
				}
			}
		}
		outcome, e := s.Owner.journal(q.OperationId, digest, executionFamily, func(tx partitions.Transaction) (*wire.StoredOutcome, error) {
			r, e := applyExecution(tx, c)
			if e != nil {
				return nil, e
			}
			return executionOutcome(r), nil
		})
		var logical *executionFailure
		if errors.As(e, &logical) {
			outcome, e = s.Owner.journal(q.OperationId, digest, executionFamily, func(partitions.Transaction) (*wire.StoredOutcome, error) {
				return executionOutcome(logical.result), nil
			})
		}
		if e != nil {
			return nil, e
		}
		return proto.Marshal(outcome.GetExecutionResult())
	}, true)
}

func executionOutcome(r *wire.ExecutionResult) *wire.StoredOutcome {
	return &wire.StoredOutcome{Result: &wire.StoredOutcome_ExecutionResult{ExecutionResult: r}}
}
func validateExecution(c *wire.ExecutionCommand) error {
	return persistence.ValidateExecutionCommand(c)
}
func legacyExecutionError(err error) error {
	var failure *persistence.ExecutionFailure
	if errors.As(err, &failure) {
		return &executionFailure{failure.Result}
	}
	return err
}
func applyExecution(tx partitions.Transaction, c *wire.ExecutionCommand) (*wire.ExecutionResult, error) {
	out, err := persistence.ApplyExecution(context.Background(), tx, c)
	if err != nil {
		return nil, legacyExecutionError(err)
	}
	return out.GetExecutionResult(), nil
}
func stageExecutionTasks(tx partitions.Transaction, shard int32, tasks []*wire.ExecutionTask) error {
	return legacyExecutionError(persistence.StageExecutionTasks(context.Background(), tx, shard, tasks))
}
func executionTaskKey(shard int32, task *wire.ExecutionTask) string {
	return persistence.ExecutionTaskKey(shard, task)
}
func execKey(shard int32, ns, wf, run string) string {
	return persistence.ExecutionKey(shard, ns, wf, run)
}
func loadImage(tx partitions.Transaction, key string) (*wire.ExecutionImage, error) {
	return persistence.LoadExecutionImage(context.Background(), tx, key)
}
