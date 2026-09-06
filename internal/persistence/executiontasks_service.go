package persistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/identity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ExecutionTasksService exposes another family on the SAME borrowed writer, authority
// callbacks and replay namespace as Service. An explicit operation retains
// admission across task staging, rollback and a fresh logical-error journal.
type ExecutionTasksService struct {
	wire.UnimplementedExecutionTasksPersistenceServer
	service *Service
}

var _ wire.ExecutionTasksPersistenceServer = (*ExecutionTasksService)(nil)

func NewExecutionTasksService(service *Service) (*ExecutionTasksService, error) {
	if service == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid execution tasks service configuration")
	}
	return &ExecutionTasksService{service: service}, nil
}

func (s *ExecutionTasksService) Execute(ctx context.Context, request *wire.ExecutionTasksRequest) (result *wire.ExecutionTasksResult, err error) {
	if request == nil || request.ProtocolVersion != 1 || request.Partition != string(s.service.partition) || identity.ValidateOperationReference(request.OperationId) != nil || request.Command == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid execution tasks envelope")
	}
	request = proto.Clone(request).(*wire.ExecutionTasksRequest)
	if err := ValidateExecutionTasksCommand(request.Command); err != nil {
		return nil, err
	}
	encoded, e := proto.MarshalOptions{Deterministic: true}.Marshal(request.Command)
	if e != nil {
		return nil, e
	}
	digest := sha256.Sum256(encoded)
	if !bytes.Equal(digest[:], request.CommandSha256) {
		return nil, status.Error(codes.InvalidArgument, "command digest mismatch")
	}
	// Typed native failure reaches lifecycle notification before an RPC edge can
	// translate it. The callback filters terminal errors using the borrowed token.
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
	belongs := func(out *wire.StoredOutcome) bool { return out.GetExecutionTasksResult() != nil }
	stored, err := s.service.operationJournal(ctx, operation, request.OperationId, request.CommandSha256, belongs, func(tx ClusterTransaction) (*wire.StoredOutcome, error) {
		return ApplyExecutionTasks(ctx, tx, request.Command)
	})
	var failure *ExecutionFailure
	if errors.As(err, &failure) {
		result := &wire.ExecutionTasksResult{Message: failure.Result.Message, Error: wire.ExecutionTasksResult_UNAVAILABLE}
		if failure.Result.Error == wire.ExecutionResult_INTERNAL {
			result.Error = wire.ExecutionTasksResult_INTERNAL
		}
		stored, err = s.service.operationJournal(ctx, operation, request.OperationId, request.CommandSha256, belongs, func(ClusterTransaction) (*wire.StoredOutcome, error) { return executionTasksOutcome(result), nil })
	}
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return stored.GetExecutionTasksResult(), nil
}
