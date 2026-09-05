package adapter

import (
	"context"
	"crypto/sha256"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"time"
)

type MatchingStore struct {
	connection        *grpc.ClientConn
	client            wire.MatchingPersistenceClient
	partition         string
	invocationTimeout time.Duration
}

// The remaining namespace user-data family is tracked separately in issue40.

func NewMatchingStore(address, partition string) (*MatchingStore, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &MatchingStore{connection: conn, client: wire.NewMatchingPersistenceClient(conn), partition: partition, invocationTimeout: 30 * time.Second}, nil
}
func (s *MatchingStore) Close() {
	if s.connection != nil {
		_ = s.connection.Close()
	}
}
func (s *MatchingStore) GetName() string { return "xenon" }
func (s *MatchingStore) invokeMatching(ctx context.Context, c *wire.MatchingCommand) (*wire.MatchingResult, error) {
	ctx, cancel := context.WithTimeout(ctx, s.invocationTimeout)
	defer cancel()
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(data)
	req := &wire.MatchingRequest{ProtocolVersion: 1, Partition: s.partition, OperationId: uuid.NewString(), CommandSha256: hash[:], Command: c}
	for attempt := 0; attempt < 3; attempt++ {
		r, e := s.client.Execute(ctx, req)
		if e == nil {
			if r == nil {
				return nil, serviceerror.NewInternal("nil matching RPC response")
			}
			return r, matchingError(r)
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		switch status.Code(e) {
		case codes.DeadlineExceeded:
			return nil, context.DeadlineExceeded
		case codes.Canceled:
			return nil, context.Canceled
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

func matchingError(r *wire.MatchingResult) error {
	switch r.Error {
	case wire.MatchingResult_NONE:
		return nil
	case wire.MatchingResult_NOT_FOUND:
		return serviceerror.NewNotFound(r.Message)
	case wire.MatchingResult_CONDITION_FAILED:
		return &p.ConditionFailedError{Msg: r.Message}
	case wire.MatchingResult_UNAVAILABLE:
		return serviceerror.NewUnavailable(r.Message)
	}
	return serviceerror.NewInternal("unknown matching error")
}
func matchingCommand(kind wire.MatchingCommand_Kind, namespace, queue string, typ enumspb.TaskQueueType) (*wire.MatchingCommand, error) {
	id, e := uuid.Parse(namespace)
	if e != nil {
		return nil, serviceerror.NewInternal("invalid namespace UUID")
	}
	return &wire.MatchingCommand{Kind: kind, NamespaceId: id[:], Queue: queue, TaskType: int32(typ)}, nil
}
func matchingBlob(data []byte, encoding int32) *commonpb.DataBlob {
	return &commonpb.DataBlob{Data: data, EncodingType: enumspb.EncodingType(encoding)}
}
func (s *MatchingStore) CreateTaskQueue(ctx context.Context, q *p.InternalCreateTaskQueueRequest) error {
	c, e := matchingCommand(wire.MatchingCommand_CREATE_QUEUE, q.NamespaceID, q.TaskQueue, q.TaskType)
	if e != nil {
		return e
	}
	if q.TaskQueueInfo == nil {
		return serviceerror.NewInvalidArgument("missing queue blob")
	}
	c.RangeId = q.RangeID
	c.Data = q.TaskQueueInfo.Data
	c.Encoding = int32(q.TaskQueueInfo.EncodingType)
	_, e = s.invokeMatching(ctx, c)
	return e
}
func (s *MatchingStore) GetTaskQueue(ctx context.Context, q *p.InternalGetTaskQueueRequest) (*p.InternalGetTaskQueueResponse, error) {
	c, e := matchingCommand(wire.MatchingCommand_GET_QUEUE, q.NamespaceID, q.TaskQueue, q.TaskType)
	if e != nil {
		return nil, e
	}
	r, e := s.invokeMatching(ctx, c)
	if e != nil {
		return nil, e
	}
	if len(r.Queues) != 1 {
		return nil, serviceerror.NewInternal("invalid matching record count")
	}
	v := r.Queues[0]
	return &p.InternalGetTaskQueueResponse{RangeID: v.RangeId, TaskQueueInfo: matchingBlob(v.Data, v.Encoding)}, nil
}
func (s *MatchingStore) UpdateTaskQueue(ctx context.Context, q *p.InternalUpdateTaskQueueRequest) (*p.UpdateTaskQueueResponse, error) {
	c, e := matchingCommand(wire.MatchingCommand_UPDATE_QUEUE, q.NamespaceID, q.TaskQueue, q.TaskType)
	if e != nil {
		return nil, e
	}
	if q.TaskQueueInfo == nil {
		return nil, serviceerror.NewInvalidArgument("missing queue blob")
	}
	c.RangeId = q.RangeID
	c.PreviousRangeId = q.PrevRangeID
	c.Data = q.TaskQueueInfo.Data
	c.Encoding = int32(q.TaskQueueInfo.EncodingType)
	_, e = s.invokeMatching(ctx, c)
	if e != nil {
		return nil, e
	}
	return &p.UpdateTaskQueueResponse{}, nil
}
func (s *MatchingStore) DeleteTaskQueue(ctx context.Context, q *p.DeleteTaskQueueRequest) error {
	c, e := matchingCommand(wire.MatchingCommand_DELETE_QUEUE, q.TaskQueue.NamespaceID, q.TaskQueue.TaskQueueName, q.TaskQueue.TaskQueueType)
	if e != nil {
		return e
	}
	c.RangeId = q.RangeID
	_, e = s.invokeMatching(ctx, c)
	return e
}
func (s *MatchingStore) ListTaskQueue(ctx context.Context, q *p.ListTaskQueueRequest) (*p.InternalListTaskQueueResponse, error) {
	if q.PageSize < 1 || q.PageSize > 1000 {
		return nil, serviceerror.NewInvalidArgument("invalid page size")
	}
	r, e := s.invokeMatching(ctx, &wire.MatchingCommand{Kind: wire.MatchingCommand_LIST_QUEUES, PageSize: int32(q.PageSize), Token: q.PageToken})
	if e != nil {
		return nil, e
	}
	out := &p.InternalListTaskQueueResponse{NextPageToken: r.Token}
	for _, v := range r.Queues {
		out.Items = append(out.Items, &p.InternalListTaskQueueItem{RangeID: v.RangeId, TaskQueue: matchingBlob(v.Data, v.Encoding)})
	}
	return out, nil
}
func (s *MatchingStore) CreateTasks(ctx context.Context, q *p.InternalCreateTasksRequest) (*p.CreateTasksResponse, error) {
	c, e := matchingCommand(wire.MatchingCommand_CREATE_TASKS, q.NamespaceID, q.TaskQueue, q.TaskType)
	if e != nil {
		return nil, e
	}
	c.RangeId = q.RangeID
	for _, v := range q.Tasks {
		if v == nil || v.Task == nil || v.Subqueue < 0 || v.Subqueue > 2147483647 {
			return nil, serviceerror.NewInvalidArgument("invalid task")
		}
		c.Tasks = append(c.Tasks, &wire.MatchingTask{Id: v.TaskId, Subqueue: int32(v.Subqueue), Data: v.Task.Data, Encoding: int32(v.Task.EncodingType)})
	}
	_, e = s.invokeMatching(ctx, c)
	if e != nil {
		return nil, e
	}
	return &p.CreateTasksResponse{UpdatedMetadata: false}, nil
}
func (s *MatchingStore) GetTasks(ctx context.Context, q *p.GetTasksRequest) (*p.InternalGetTasksResponse, error) {
	if q.InclusiveMinPass != 0 {
		return nil, serviceerror.NewInternal("InclusiveMinPass is not supported")
	}
	if q.PageSize < 1 || q.PageSize > 1000 || q.Subqueue < 0 || q.Subqueue > 2147483647 {
		return nil, serviceerror.NewInvalidArgument("invalid task page")
	}
	c, e := matchingCommand(wire.MatchingCommand_GET_TASKS, q.NamespaceID, q.TaskQueue, q.TaskType)
	if e != nil {
		return nil, e
	}
	c.MinId = q.InclusiveMinTaskID
	c.MaxId = q.ExclusiveMaxTaskID
	c.PageSize = int32(q.PageSize)
	c.Subqueue = int32(q.Subqueue)
	c.Token = q.NextPageToken
	r, e := s.invokeMatching(ctx, c)
	if e != nil {
		return nil, e
	}
	out := &p.InternalGetTasksResponse{NextPageToken: r.Token}
	for _, v := range r.Tasks {
		out.Tasks = append(out.Tasks, matchingBlob(v.Data, v.Encoding))
	}
	return out, nil
}
func (s *MatchingStore) CompleteTasksLessThan(ctx context.Context, q *p.CompleteTasksLessThanRequest) (int, error) {
	if q.ExclusiveMaxPass != 0 {
		return 0, serviceerror.NewInternal("ExclusiveMaxPass is not supported")
	}
	if q.Limit < 1 || q.Limit > 1000 || q.Subqueue < 0 || q.Subqueue > 2147483647 {
		return 0, serviceerror.NewInvalidArgument("invalid completion limit")
	}
	c, e := matchingCommand(wire.MatchingCommand_COMPLETE_TASKS, q.NamespaceID, q.TaskQueueName, q.TaskType)
	if e != nil {
		return 0, e
	}
	c.MaxId = q.ExclusiveMaxTaskID
	c.Subqueue = int32(q.Subqueue)
	c.PageSize = int32(q.Limit)
	r, e := s.invokeMatching(ctx, c)
	if e != nil {
		return 0, e
	}
	return int(r.Completed), nil
}
