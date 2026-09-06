package persistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/replay"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ExecutionService uses the same borrowed writer and replay namespace as all
// other families. One explicit operation excludes them across every child.
type ExecutionService struct {
	wire.UnimplementedExecutionPersistenceServer
	service *Service
}

var _ wire.ExecutionPersistenceServer = (*ExecutionService)(nil)

func NewExecutionService(service *Service) (*ExecutionService, error) {
	if service == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid execution service configuration")
	}
	return &ExecutionService{service: service}, nil
}
func (s *ExecutionService) Execute(ctx context.Context, q *wire.ExecutionRequest) (result *wire.ExecutionResult, err error) {
	if q == nil || q.ProtocolVersion != 1 || q.Partition != string(s.service.partition) || identity.ValidateOperationReference(q.OperationId) != nil || q.Command == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid execution envelope")
	}
	q = proto.Clone(q).(*wire.ExecutionRequest)
	if err = ValidateExecutionCommand(q.Command); err != nil {
		return nil, err
	}
	encoded, e := proto.MarshalOptions{Deterministic: true}.Marshal(q.Command)
	if e != nil {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	digest := sha256.Sum256(encoded)
	if !bytes.Equal(digest[:], q.CommandSha256) {
		return nil, status.Error(codes.InvalidArgument, "digest mismatch")
	}
	defer func() {
		if err != nil {
			s.service.failure(err)
		}
	}()
	operation, err := s.service.writer.BeginOperation(ctx)
	if err != nil {
		return nil, err
	}
	defer operation.Release()
	if err = s.service.authority(ctx); err != nil {
		return nil, err
	}
	// Root lookup is aborted before history children, but the operation admission
	// remains held. A replay skips prewrites, preserving post-completion deletion.
	lookup, err := operation.Begin(ctx)
	if err != nil {
		return nil, err
	}
	saved, err := lookup.Get(ctx, []byte("v1/outcome/"+q.OperationId))
	abortErr := lookup.Abort()
	if err != nil {
		return nil, err
	}
	if abortErr != nil {
		return nil, abortErr
	}
	if saved == nil {
		for index, h := range q.Command.HistoryPrewrites {
			encoded, e := proto.MarshalOptions{Deterministic: true}.Marshal(h)
			if e != nil {
				return nil, clusterEncodingError(e)
			}
			childDigest := sha256.Sum256(encoded)
			// Private durable identities predate canonical public IDs. Keep these exact
			// keys; public envelope validation must never be applied to child journals.
			_, err = s.journal(ctx, operation, fmt.Sprintf("%s-h-%d", q.OperationId, index), childDigest[:], func(out *wire.StoredOutcome) bool { return out.GetHistoryResult() != nil }, func(tx ClusterTransaction) (*wire.StoredOutcome, error) { return ApplyHistory(ctx, tx, h) })
			if err != nil {
				return nil, err
			}
		}
	}
	belongs := func(out *wire.StoredOutcome) bool { return out.GetExecutionResult() != nil }
	outcome, err := s.journal(ctx, operation, q.OperationId, digest[:], belongs, func(tx ClusterTransaction) (*wire.StoredOutcome, error) { return ApplyExecution(ctx, tx, q.Command) })
	var logical *ExecutionFailure
	if errors.As(err, &logical) {
		// journal has aborted the root, including any staged tasks/current pointer.
		// The logical failure is durable in a fresh transaction under the SAME scope.
		outcome, err = s.journal(ctx, operation, q.OperationId, digest[:], belongs, func(ClusterTransaction) (*wire.StoredOutcome, error) { return executionOutcome(logical.Result), nil })
	}
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return outcome.GetExecutionResult(), nil
}
func (s *ExecutionService) journal(ctx context.Context, operation partitions.Operation, id string, digest []byte, belongs func(*wire.StoredOutcome) bool, apply func(ClusterTransaction) (*wire.StoredOutcome, error)) (*wire.StoredOutcome, error) {
	tx, err := operation.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Abort()
	return replay.Run(replay.Effects{
		Get:     func(key string) ([]byte, error) { return tx.Get(ctx, []byte(key)) },
		Put:     func(key string, value []byte) error { return tx.Put([]byte(key), value) },
		Apply:   func() (*wire.StoredOutcome, error) { return apply(tx) },
		Account: func(out *wire.StoredOutcome, size int) error { return AccountOutcome(ctx, tx, out, size) },
		Belongs: belongs,
		Commit: func(*wire.StoredOutcome) error {
			receipt, err := tx.Commit(ctx)
			if err != nil {
				return err
			}
			return s.service.writer.AwaitDurable(ctx, receipt)
		},
	}, id, digest, s.service.limit)
}
