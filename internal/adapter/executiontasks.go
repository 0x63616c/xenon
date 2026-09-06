package adapter

import (
	"context"
	"crypto/sha256"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/rpctrace"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"time"
)

// ExecutionTasksStore implements explicit task insertion and replication DLQ.
type ExecutionTasksStore struct {
	operations        *operationIDs
	connection        *grpc.ClientConn
	client            wire.ExecutionTasksPersistenceClient
	partition         string
	invocationTimeout time.Duration
	historyPartitions []string
}

func NewExecutionTasksStore(address, partition string, options ...StoreOption) (*ExecutionTasksStore, error) {
	c, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithChainUnaryInterceptor(rpctrace.Unary))
	if e != nil {
		return nil, e
	}
	return &ExecutionTasksStore{newOperationIDs(options...), c, wire.NewExecutionTasksPersistenceClient(c), partition, 30 * time.Second, nil}, nil
}
func (s *ExecutionTasksStore) Close() {
	if s.connection != nil {
		_ = s.connection.Close()
	}
}
func (s *ExecutionTasksStore) GetName() string { return "xenon" }
func (s *ExecutionTasksStore) invokeExecutionTasks(ctx context.Context, c *wire.ExecutionTasksCommand) (traceResult *wire.ExecutionTasksResult, traceErr error) {
	ctx, traceFinish, traceBeginErr := rpctrace.BeginObserved(ctx, "executiontasks")
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
	q := &wire.ExecutionTasksRequest{ProtocolVersion: 1, Partition: partition, OperationId: operation, CommandSha256: d[:], Command: c}
	for attempt := 0; attempt < 3; attempt++ {
		r, e := s.client.Execute(ctx, q)
		if e == nil {
			if r == nil {
				return nil, serviceerror.NewInternal("nil execution result")
			}
			return r, executionTasksError(r)
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
func executionTasksError(r *wire.ExecutionTasksResult) error {
	switch r.Error {
	case wire.ExecutionTasksResult_NONE:
		return nil
	case wire.ExecutionTasksResult_UNAVAILABLE:
		return serviceerror.NewUnavailable(r.Message)
	case wire.ExecutionTasksResult_OWNERSHIP_LOST:
		return &p.ShardOwnershipLostError{Msg: r.Message, ShardID: r.ShardId}
	default:
		return serviceerror.NewInternal(r.Message)
	}
}
func (s *ExecutionTasksStore) AddHistoryTasks(ctx context.Context, q *p.InternalAddHistoryTasksRequest) error {
	if q == nil {
		return serviceerror.NewInvalidArgument("nil add tasks")
	}
	_, e := s.invokeExecutionTasks(ctx, &wire.ExecutionTasksCommand{Kind: wire.ExecutionTasksCommand_ADD, ShardId: q.ShardID, RangeId: q.RangeID, NamespaceId: q.NamespaceID, WorkflowId: q.WorkflowID, ArchetypeId: uint32(q.ArchetypeID), Tasks: executionTasks(q.Tasks)})
	return e
}
func (s *ExecutionTasksStore) PutReplicationTaskToDLQ(ctx context.Context, q *p.PutReplicationTaskToDLQRequest) error {
	if q == nil || q.TaskInfo == nil {
		return serviceerror.NewInvalidArgument("nil replication task")
	}
	b, e := serialization.NewSerializer().ReplicationTaskInfoToBlob(q.TaskInfo)
	if e != nil {
		return e
	}
	_, e = s.invokeExecutionTasks(ctx, &wire.ExecutionTasksCommand{Kind: wire.ExecutionTasksCommand_PUT_DLQ, ShardId: q.ShardID, SourceCluster: q.SourceClusterName, TaskId: q.TaskInfo.TaskId, TaskInfo: historyBlob(b)})
	return e
}
func (s *ExecutionTasksStore) GetReplicationTasksFromDLQ(ctx context.Context, q *p.GetReplicationTasksFromDLQRequest) (*p.InternalGetReplicationTasksFromDLQResponse, error) {
	if q == nil {
		return nil, serviceerror.NewInvalidArgument("nil replication read")
	}
	r, e := s.invokeExecutionTasks(ctx, &wire.ExecutionTasksCommand{Kind: wire.ExecutionTasksCommand_READ_DLQ, ShardId: q.ShardID, SourceCluster: q.SourceClusterName, MinimumId: q.InclusiveMinTaskKey.TaskID, MaximumId: q.ExclusiveMaxTaskKey.TaskID, PageSize: int64(q.BatchSize), NextPageToken: q.NextPageToken})
	if e != nil {
		return nil, e
	}
	out := &p.InternalGetReplicationTasksFromDLQResponse{NextPageToken: r.NextPageToken}
	for _, t := range r.Tasks {
		if t == nil || t.Blob == nil {
			return nil, serviceerror.NewInternal("missing replication blob")
		}
		out.Tasks = append(out.Tasks, p.InternalHistoryTask{Key: tasks.NewImmediateKey(t.TaskId), Blob: fromHistoryBlob(t.Blob)})
	}
	return out, nil
}
func (s *ExecutionTasksStore) DeleteReplicationTaskFromDLQ(ctx context.Context, q *p.DeleteReplicationTaskFromDLQRequest) error {
	if q == nil {
		return serviceerror.NewInvalidArgument("nil replication delete")
	}
	_, e := s.invokeExecutionTasks(ctx, &wire.ExecutionTasksCommand{Kind: wire.ExecutionTasksCommand_DELETE_DLQ, ShardId: q.ShardID, SourceCluster: q.SourceClusterName, TaskId: q.TaskKey.TaskID})
	return e
}
func (s *ExecutionTasksStore) RangeDeleteReplicationTaskFromDLQ(ctx context.Context, q *p.RangeDeleteReplicationTaskFromDLQRequest) error {
	if q == nil {
		return serviceerror.NewInvalidArgument("nil replication range delete")
	}
	_, e := s.invokeExecutionTasks(ctx, &wire.ExecutionTasksCommand{Kind: wire.ExecutionTasksCommand_RANGE_DELETE_DLQ, ShardId: q.ShardID, SourceCluster: q.SourceClusterName, MinimumId: q.InclusiveMinTaskKey.TaskID, MaximumId: q.ExclusiveMaxTaskKey.TaskID})
	return e
}
func (s *ExecutionTasksStore) IsReplicationDLQEmpty(ctx context.Context, q *p.GetReplicationTasksFromDLQRequest) (bool, error) {
	if q == nil {
		return false, serviceerror.NewInvalidArgument("nil replication empty check")
	}
	r, e := s.invokeExecutionTasks(ctx, &wire.ExecutionTasksCommand{Kind: wire.ExecutionTasksCommand_IS_EMPTY_DLQ, ShardId: q.ShardID, SourceCluster: q.SourceClusterName, MinimumId: q.InclusiveMinTaskKey.TaskID})
	if e != nil {
		return false, e
	}
	return r.Empty, nil
}
