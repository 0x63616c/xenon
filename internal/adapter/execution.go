package adapter

import (
	"context"
	"crypto/sha256"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"time"
)

// WorkflowStore implements the mutable-state component; history task retrieval
// and the complete ExecutionStore composition are separate delivery gates.
type WorkflowStore struct {
	connection        *grpc.ClientConn
	client            wire.ExecutionPersistenceClient
	partition         string
	invocationTimeout time.Duration
}

func NewWorkflowStore(address, partition string) (*WorkflowStore, error) {
	c, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		return nil, e
	}
	return &WorkflowStore{c, wire.NewExecutionPersistenceClient(c), partition, 30 * time.Second}, nil
}
func (s *WorkflowStore) Close() {
	if s.connection != nil {
		_ = s.connection.Close()
	}
}
func (s *WorkflowStore) GetName() string { return "xenon" }
func (s *WorkflowStore) invokeExecution(ctx context.Context, c *wire.ExecutionCommand) (*wire.ExecutionResult, error) {
	ctx, cancel := context.WithTimeout(ctx, s.invocationTimeout)
	defer cancel()
	raw, e := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if e != nil {
		return nil, e
	}
	d := sha256.Sum256(raw)
	q := &wire.ExecutionRequest{ProtocolVersion: 1, Partition: s.partition, OperationId: uuid.NewString(), CommandSha256: d[:], Command: c}
	for attempt := 0; attempt < 3; attempt++ {
		r, e := s.client.Execute(ctx, q)
		if e == nil {
			if r == nil {
				return nil, serviceerror.NewInternal("nil execution result")
			}
			return r, executionError(r)
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
func executionError(r *wire.ExecutionResult) error {
	switch r.Error {
	case wire.ExecutionResult_NONE:
		return nil
	case wire.ExecutionResult_NOT_FOUND:
		return serviceerror.NewNotFound(r.Message)
	case wire.ExecutionResult_UNAVAILABLE:
		return serviceerror.NewUnavailable(r.Message)
	case wire.ExecutionResult_CONDITION_FAILED:
		return &p.ConditionFailedError{Msg: r.Message}
	case wire.ExecutionResult_WORKFLOW_CONDITION_FAILED:
		return &p.WorkflowConditionFailedError{Msg: r.Message, NextEventID: r.ActualNextEventId, DBRecordVersion: r.ActualDbRecordVersion}
	case wire.ExecutionResult_OWNERSHIP_LOST:
		return &p.ShardOwnershipLostError{Msg: r.Message, ShardID: r.ShardId}
	case wire.ExecutionResult_CURRENT_CONDITION_FAILED:
		e := &p.CurrentWorkflowConditionFailedError{Msg: r.Message}
		if r.Current != nil {
			state := new(persistencespb.WorkflowExecutionState)
			if err := proto.Unmarshal(r.Current.StateProto, state); err != nil {
				return serviceerror.NewInternal(err.Error())
			}
			e.RunID = r.Current.RunId
			e.LastWriteVersion = r.Current.LastWriteVersion
			e.RequestIDs = state.RequestIds
			e.State = state.State
			e.Status = state.Status
			e.StartTime = executionStartTime(state)
		}
		return e
	default:
		return serviceerror.NewInternal(r.Message)
	}
}
func executionEvents(lists ...[]*p.InternalAppendHistoryNodesRequest) ([]*wire.HistoryCommand, error) {
	var out []*wire.HistoryCommand
	for _, list := range lists {
		for _, q := range list {
			if q == nil || q.Node.Events == nil || (q.IsNewBranch && q.TreeInfo == nil) {
				return nil, serviceerror.NewInvalidArgument("nil history prewrite")
			}
			c, e := historyCommand(wire.HistoryCommand_APPEND, q.ShardID, q.BranchInfo)
			if e != nil {
				return nil, e
			}
			c.BranchToken = q.BranchToken
			c.IsNewBranch = q.IsNewBranch
			c.Info = q.Info
			c.TreeInfo = historyBlob(q.TreeInfo)
			c.Node = &wire.HistoryNodeRecord{NodeId: q.Node.NodeID, TransactionId: q.Node.TransactionID, PreviousTransactionId: q.Node.PrevTransactionID, Events: historyBlob(q.Node.Events)}
			out = append(out, c)
		}
	}
	return out, nil
}
func (s *WorkflowStore) CreateWorkflowExecution(ctx context.Context, q *p.InternalCreateWorkflowExecutionRequest) (*p.InternalCreateWorkflowExecutionResponse, error) {
	if q == nil {
		return nil, serviceerror.NewInvalidArgument("nil create")
	}
	i, e := executionSnapshot(&q.NewWorkflowSnapshot)
	if e != nil {
		return nil, e
	}
	h, e := executionEvents(q.NewWorkflowNewEvents)
	if e != nil {
		return nil, e
	}
	_, e = s.invokeExecution(ctx, &wire.ExecutionCommand{Kind: wire.ExecutionCommand_CREATE, ShardId: q.ShardID, RangeId: q.RangeID, Mode: int32(q.Mode), ArchetypeId: uint32(q.ArchetypeID), PreviousRunId: q.PreviousRunID, PreviousLastWriteVersion: q.PreviousLastWriteVersion, Snapshot: i, HistoryPrewrites: h})
	if e != nil {
		return nil, e
	}
	return &p.InternalCreateWorkflowExecutionResponse{}, nil
}
func (s *WorkflowStore) UpdateWorkflowExecution(ctx context.Context, q *p.InternalUpdateWorkflowExecutionRequest) error {
	if q == nil {
		return serviceerror.NewInvalidArgument("nil update")
	}
	m, e := executionMutation(&q.UpdateWorkflowMutation)
	if e != nil {
		return e
	}
	n, e := executionSnapshot(q.NewWorkflowSnapshot)
	if e != nil {
		return e
	}
	h, e := executionEvents(q.UpdateWorkflowNewEvents, q.NewWorkflowNewEvents)
	if e != nil {
		return e
	}
	_, e = s.invokeExecution(ctx, &wire.ExecutionCommand{Kind: wire.ExecutionCommand_UPDATE, ShardId: q.ShardID, RangeId: q.RangeID, Mode: int32(q.Mode), ArchetypeId: uint32(q.ArchetypeID), Mutation: m, NewSnapshot: n, HistoryPrewrites: h})
	return e
}
func (s *WorkflowStore) ConflictResolveWorkflowExecution(ctx context.Context, q *p.InternalConflictResolveWorkflowExecutionRequest) error {
	if q == nil {
		return serviceerror.NewInvalidArgument("nil conflict resolve")
	}
	i, e := executionSnapshot(&q.ResetWorkflowSnapshot)
	if e != nil {
		return e
	}
	n, e := executionSnapshot(q.NewWorkflowSnapshot)
	if e != nil {
		return e
	}
	m, e := executionMutation(q.CurrentWorkflowMutation)
	if e != nil {
		return e
	}
	h, e := executionEvents(q.CurrentWorkflowEventsNewEvents, q.ResetWorkflowEventsNewEvents, q.NewWorkflowEventsNewEvents)
	if e != nil {
		return e
	}
	_, e = s.invokeExecution(ctx, &wire.ExecutionCommand{Kind: wire.ExecutionCommand_CONFLICT_RESOLVE, ShardId: q.ShardID, RangeId: q.RangeID, Mode: int32(q.Mode), ArchetypeId: uint32(q.ArchetypeID), Snapshot: i, NewSnapshot: n, CurrentMutation: m, HistoryPrewrites: h})
	return e
}
func (s *WorkflowStore) SetWorkflowExecution(ctx context.Context, q *p.InternalSetWorkflowExecutionRequest) error {
	if q == nil {
		return serviceerror.NewInvalidArgument("nil set")
	}
	i, e := executionSnapshot(&q.SetWorkflowSnapshot)
	if e != nil {
		return e
	}
	_, e = s.invokeExecution(ctx, &wire.ExecutionCommand{Kind: wire.ExecutionCommand_SET, ShardId: q.ShardID, RangeId: q.RangeID, ArchetypeId: uint32(q.ArchetypeID), Snapshot: i})
	return e
}
func (s *WorkflowStore) GetWorkflowExecution(ctx context.Context, q *p.GetWorkflowExecutionRequest) (*p.InternalGetWorkflowExecutionResponse, error) {
	if q == nil {
		return nil, serviceerror.NewInvalidArgument("nil get")
	}
	r, e := s.invokeExecution(ctx, &wire.ExecutionCommand{Kind: wire.ExecutionCommand_GET, ShardId: q.ShardID, NamespaceId: q.NamespaceID, WorkflowId: q.WorkflowID, RunId: q.RunID, ArchetypeId: uint32(q.ArchetypeID)})
	if e != nil {
		return nil, e
	}
	if r.Image == nil {
		return nil, serviceerror.NewInternal("missing workflow image")
	}
	return &p.InternalGetWorkflowExecutionResponse{State: executionMutableState(r.Image), DBRecordVersion: r.Image.DbRecordVersion}, nil
}
func (s *WorkflowStore) GetCurrentExecution(ctx context.Context, q *p.GetCurrentExecutionRequest) (*p.InternalGetCurrentExecutionResponse, error) {
	if q == nil {
		return nil, serviceerror.NewInvalidArgument("nil get current")
	}
	r, e := s.invokeExecution(ctx, &wire.ExecutionCommand{Kind: wire.ExecutionCommand_GET_CURRENT, ShardId: q.ShardID, NamespaceId: q.NamespaceID, WorkflowId: q.WorkflowID, ArchetypeId: uint32(q.ArchetypeID)})
	if e != nil {
		return nil, e
	}
	if r.Current == nil {
		return nil, serviceerror.NewInternal("missing current workflow")
	}
	state := new(persistencespb.WorkflowExecutionState)
	if e = proto.Unmarshal(r.Current.StateProto, state); e != nil {
		return nil, serviceerror.NewInternal(e.Error())
	}
	return &p.InternalGetCurrentExecutionResponse{RunID: r.Current.RunId, ExecutionState: state}, nil
}
func (s *WorkflowStore) DeleteWorkflowExecution(ctx context.Context, q *p.DeleteWorkflowExecutionRequest) error {
	if q == nil {
		return serviceerror.NewInvalidArgument("nil delete")
	}
	_, e := s.invokeExecution(ctx, &wire.ExecutionCommand{Kind: wire.ExecutionCommand_DELETE, ShardId: q.ShardID, NamespaceId: q.NamespaceID, WorkflowId: q.WorkflowID, RunId: q.RunID, ArchetypeId: uint32(q.ArchetypeID)})
	return e
}
func (s *WorkflowStore) DeleteCurrentWorkflowExecution(ctx context.Context, q *p.DeleteCurrentWorkflowExecutionRequest) error {
	if q == nil {
		return serviceerror.NewInvalidArgument("nil delete current")
	}
	_, e := s.invokeExecution(ctx, &wire.ExecutionCommand{Kind: wire.ExecutionCommand_DELETE_CURRENT, ShardId: q.ShardID, NamespaceId: q.NamespaceID, WorkflowId: q.WorkflowID, RunId: q.RunID, ArchetypeId: uint32(q.ArchetypeID)})
	return e
}
func (s *WorkflowStore) ListConcreteExecutions(ctx context.Context, q *p.ListConcreteExecutionsRequest) (*p.InternalListConcreteExecutionsResponse, error) {
	if q == nil {
		return nil, serviceerror.NewInvalidArgument("nil list")
	}
	r, e := s.invokeExecution(ctx, &wire.ExecutionCommand{Kind: wire.ExecutionCommand_LIST, ShardId: q.ShardID, PageSize: int64(q.PageSize), NextPageToken: q.PageToken})
	if e != nil {
		return nil, e
	}
	out := &p.InternalListConcreteExecutionsResponse{NextPageToken: r.NextPageToken}
	for _, i := range r.Images {
		out.States = append(out.States, executionMutableState(i))
	}
	return out, nil
}
