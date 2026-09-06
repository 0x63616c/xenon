package adapter

import (
	"context"
	"crypto/sha256"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/rpctrace"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"time"
)

// HistoryTasksStore implements history-task reads and completion.
type HistoryTasksStore struct {
	operations        *operationIDs
	connection        *grpc.ClientConn
	client            wire.HistoryTasksPersistenceClient
	partition         string
	invocationTimeout time.Duration
	historyPartitions []string
}

func NewHistoryTasksStore(address, partition string, options ...StoreOption) (*HistoryTasksStore, error) {
	c, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithChainUnaryInterceptor(rpctrace.Unary))
	if e != nil {
		return nil, e
	}
	return &HistoryTasksStore{newOperationIDs(options...), c, wire.NewHistoryTasksPersistenceClient(c), partition, 30 * time.Second, nil}, nil
}
func (s *HistoryTasksStore) Close() {
	if s.connection != nil {
		_ = s.connection.Close()
	}
}
func (s *HistoryTasksStore) GetName() string { return "xenon" }
func (s *HistoryTasksStore) invokeHistoryTasks(ctx context.Context, c *wire.HistoryTasksCommand) (traceResult *wire.HistoryTasksResult, traceErr error) {
	ctx, traceFinish, traceBeginErr := rpctrace.BeginObserved(ctx, "historytasks")
	if traceBeginErr != nil {
		return nil, traceBeginErr
	}
	defer func() { traceErr = traceFinish(traceErr) }()
	ctx, cancel := context.WithTimeout(ctx, s.invocationTimeout)
	defer cancel()
	raw, e := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if e != nil {
		return nil, e
	}
	d := sha256.Sum256(raw)
	partition, routeErr := historyPartition(s.historyPartitions, s.partition, c.ShardId)
	if routeErr != nil {
		return nil, routeErr
	}
	operation, operationErr := s.operations.next()
	if operationErr != nil {
		return nil, operationErr
	}
	q := &wire.HistoryTasksRequest{ProtocolVersion: 1, Partition: partition, OperationId: operation, CommandSha256: d[:], Command: c}
	r, e := retryOperation(ctx, func(ctx context.Context) (*wire.HistoryTasksResult, error) { return s.client.Execute(ctx, q) })
	if e != nil {
		return nil, e
	}

	if r == nil {
		return nil, serviceerror.NewInternal("nil execution result")
	}
	return r, historyTasksError(r)

}
func historyTasksError(r *wire.HistoryTasksResult) error {
	if r.Error == wire.HistoryTasksResult_NONE {
		return nil
	}
	return serviceerror.NewInternal(r.Message)
}
func historyTaskBound(k tasks.Key) *wire.ExecutionTask {
	return &wire.ExecutionTask{TaskId: k.TaskID, FireSeconds: k.FireTime.Unix(), FireNanos: int32(k.FireTime.Nanosecond())}
}
func (s *HistoryTasksStore) GetHistoryTasks(ctx context.Context, q *p.GetHistoryTasksRequest) (*p.InternalGetHistoryTasksResponse, error) {
	if q == nil {
		return nil, serviceerror.NewInvalidArgument("nil history-task request")
	}
	r, e := s.invokeHistoryTasks(ctx, &wire.HistoryTasksCommand{Kind: wire.HistoryTasksCommand_READ, ShardId: q.ShardID, CategoryId: int32(q.TaskCategory.ID()), CategoryType: int32(q.TaskCategory.Type()), Minimum: historyTaskBound(q.InclusiveMinTaskKey), Maximum: historyTaskBound(q.ExclusiveMaxTaskKey), PageSize: int64(q.BatchSize), NextPageToken: q.NextPageToken})
	if e != nil {
		return nil, e
	}
	out := &p.InternalGetHistoryTasksResponse{Tasks: make([]p.InternalHistoryTask, 0, len(r.Tasks)), NextPageToken: r.NextPageToken}
	for _, t := range r.Tasks {
		key := tasks.NewImmediateKey(t.TaskId)
		if t.CategoryType == 2 {
			key = tasks.NewKey(time.Unix(t.FireSeconds, int64(t.FireNanos)).UTC(), t.TaskId)
		}
		if t.Blob == nil {
			return nil, serviceerror.NewInternal("missing history task blob")
		}
		out.Tasks = append(out.Tasks, p.InternalHistoryTask{Key: key, Blob: fromHistoryBlob(t.Blob)})
	}
	return out, nil
}
func (s *HistoryTasksStore) CompleteHistoryTask(ctx context.Context, q *p.CompleteHistoryTaskRequest) error {
	if q == nil {
		return serviceerror.NewInvalidArgument("nil completion request")
	}
	_, e := s.invokeHistoryTasks(ctx, &wire.HistoryTasksCommand{Kind: wire.HistoryTasksCommand_COMPLETE, ShardId: q.ShardID, CategoryId: int32(q.TaskCategory.ID()), CategoryType: int32(q.TaskCategory.Type()), Minimum: historyTaskBound(q.TaskKey), Maximum: historyTaskBound(q.TaskKey), BestEffort: q.BestEffort})
	return e
}
func (s *HistoryTasksStore) RangeCompleteHistoryTasks(ctx context.Context, q *p.RangeCompleteHistoryTasksRequest) error {
	if q == nil {
		return serviceerror.NewInvalidArgument("nil range completion request")
	}
	_, e := s.invokeHistoryTasks(ctx, &wire.HistoryTasksCommand{Kind: wire.HistoryTasksCommand_RANGE_COMPLETE, ShardId: q.ShardID, CategoryId: int32(q.TaskCategory.ID()), CategoryType: int32(q.TaskCategory.Type()), Minimum: historyTaskBound(q.InclusiveMinTaskKey), Maximum: historyTaskBound(q.ExclusiveMaxTaskKey)})
	return e
}
