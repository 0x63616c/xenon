package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	native "slatedb.io/slatedb-go/uniffi"
	"sort"
	"strings"
)

// executionFailure aborts all staged changes before its outcome is journaled.
type executionFailure struct{ result *wire.ExecutionResult }

func (e *executionFailure) Error() string { return e.result.Message }
func execFail(kind wire.ExecutionResult_Error, msg string) error {
	return &executionFailure{&wire.ExecutionResult{Error: kind, Message: msg}}
}

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
	return s.Owner.run(ctx, func(*native.Db) ([]byte, error) {
		// Check the root before independent history prewrites. A replay must never
		// recreate events removed after the original completed operation.
		tx, e := s.Owner.db.Begin(native.IsolationLevelSerializableSnapshot)
		if e != nil {
			return nil, backend(e)
		}
		saved, e := get(tx, "v1/outcome/"+q.OperationId)
		tx.Destroy()
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
				_, e = s.Owner.journal(fmt.Sprintf("%s-h-%d", q.OperationId, index), hd[:], historyFamily, func(tx *native.DbTransaction) (*wire.StoredOutcome, error) {
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
		outcome, e := s.Owner.journal(q.OperationId, digest, executionFamily, func(tx *native.DbTransaction) (*wire.StoredOutcome, error) {
			r, e := applyExecution(tx, c)
			if e != nil {
				return nil, e
			}
			return executionOutcome(r), nil
		})
		var logical *executionFailure
		if errors.As(e, &logical) {
			outcome, e = s.Owner.journal(q.OperationId, digest, executionFamily, func(*native.DbTransaction) (*wire.StoredOutcome, error) { return executionOutcome(logical.result), nil })
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
	bad := func() error { return status.Error(codes.InvalidArgument, "invalid execution command") }
	if c.Kind < wire.ExecutionCommand_CREATE || c.Kind > wire.ExecutionCommand_LIST || c.ShardId < 0 || len(c.NextPageToken) > 4096 || len(c.HistoryPrewrites) > 1000 {
		return bad()
	}
	if c.Kind == wire.ExecutionCommand_LIST {
		if c.PageSize < 1 || c.PageSize > 1000 {
			return bad()
		}
	} else if c.Kind >= wire.ExecutionCommand_GET {
		if _, e := uuid.Parse(c.NamespaceId); e != nil || c.WorkflowId == "" {
			return bad()
		}
		if c.Kind != wire.ExecutionCommand_GET_CURRENT {
			if _, e := uuid.Parse(c.RunId); e != nil {
				return bad()
			}
		}
	}
	if (c.Kind == wire.ExecutionCommand_CREATE || c.Kind == wire.ExecutionCommand_SET || c.Kind == wire.ExecutionCommand_CONFLICT_RESOLVE) && c.Snapshot == nil {
		return bad()
	}
	if c.Kind == wire.ExecutionCommand_UPDATE && (c.Mutation == nil || c.Mutation.Upsert == nil) {
		return bad()
	}
	if ((c.Kind == wire.ExecutionCommand_CREATE || c.Kind == wire.ExecutionCommand_UPDATE) && (c.Mode < 0 || c.Mode > 2)) || (c.Kind == wire.ExecutionCommand_CONFLICT_RESOLVE && (c.Mode < 0 || c.Mode > 1)) {
		return bad()
	}
	images := []*wire.ExecutionImage{c.Snapshot, c.NewSnapshot}
	for _, m := range []*wire.ExecutionMutation{c.Mutation, c.CurrentMutation} {
		if m != nil {
			if m.Upsert == nil {
				return bad()
			}
			images = append(images, m.Upsert)
		}
	}
	for _, i := range images {
		if i == nil {
			continue
		}
		state := new(persistencespb.WorkflowExecutionState)
		if e := proto.Unmarshal(i.ExecutionStateProto, state); e != nil {
			return bad()
		}
		if _, e := uuid.Parse(state.RunId); e != nil {
			return bad()
		}
		if _, e := uuid.Parse(i.NamespaceId); e != nil || i.WorkflowId == "" || i.ExecutionInfoBlob == nil {
			return bad()
		}
		if i.RunId != "" {
			if _, e := uuid.Parse(i.RunId); e != nil {
				return bad()
			}
		}
		for _, t := range i.Tasks {
			if t == nil || t.Blob == nil || t.FireNanos < 0 || t.FireNanos >= 1e9 {
				return bad()
			}
		}
	}
	for _, h := range c.HistoryPrewrites {
		if h == nil || h.Kind != wire.HistoryCommand_APPEND || h.ShardId != c.ShardId || len(h.TreeId) != 16 || len(h.BranchId) != 16 || h.Node == nil || h.Node.Events == nil || h.Node.NodeId < 1 || (h.IsNewBranch && h.TreeInfo == nil) {
			return bad()
		}
	}
	return nil
}
func execPrefix(shard int32) string { return fmt.Sprintf("v1/execution/%010d/", shard) }
func canonicalExecutionID(id string) string {
	if u, e := uuid.Parse(id); e == nil {
		return u.String()
	}
	return id
}
func execKey(shard int32, ns, wf, run string) string {
	ns = canonicalExecutionID(ns)
	run = canonicalExecutionID(run)
	return fmt.Sprintf("%s%s/%s/%s", execPrefix(shard), ns, hex.EncodeToString([]byte(wf)), run)
}
func currentKey(c *wire.ExecutionCommand, ns, wf string) string {
	return fmt.Sprintf("v1/current/%010d/%s/%s/%08x", c.ShardId, canonicalExecutionID(ns), hex.EncodeToString([]byte(wf)), c.ArchetypeId)
}
func loadImage(tx *native.DbTransaction, key string) (*wire.ExecutionImage, error) {
	raw, e := get(tx, key)
	if e != nil || raw == nil {
		return nil, e
	}
	i := new(wire.ExecutionImage)
	if e = proto.Unmarshal(raw, i); e != nil {
		return nil, backend(e)
	}
	return i, nil
}
func loadCurrent(tx *native.DbTransaction, key string) (*wire.ExecutionCurrent, error) {
	raw, e := get(tx, key)
	if e != nil || raw == nil {
		return nil, e
	}
	i := new(wire.ExecutionCurrent)
	if e = proto.Unmarshal(raw, i); e != nil {
		return nil, backend(e)
	}
	return i, nil
}
func currentFailure(current *wire.ExecutionCurrent) error {
	return &executionFailure{&wire.ExecutionResult{Error: wire.ExecutionResult_CURRENT_CONDITION_FAILED, Message: "current workflow condition failed", Current: current}}
}
func stateOf(i *wire.ExecutionImage) *persistencespb.WorkflowExecutionState {
	s := new(persistencespb.WorkflowExecutionState)
	_ = proto.Unmarshal(i.ExecutionStateProto, s)
	s.RunId = canonicalExecutionID(s.RunId)
	return s
}
func workflowCheck(old, i *wire.ExecutionImage) error {
	if old == nil {
		return execFail(wire.ExecutionResult_CONDITION_FAILED, "workflow does not exist")
	}
	if (i.DbRecordVersion == 0 && old.NextEventId != i.Condition) || (i.DbRecordVersion != 0 && old.DbRecordVersion != i.DbRecordVersion-1) {
		return &executionFailure{&wire.ExecutionResult{Error: wire.ExecutionResult_WORKFLOW_CONDITION_FAILED, Message: "workflow version condition failed", ActualNextEventId: old.NextEventId, ActualDbRecordVersion: old.DbRecordVersion}}
	}
	return nil
}
func stageSnapshot(tx *native.DbTransaction, c *wire.ExecutionCommand, i *wire.ExecutionImage, isNew bool) error {
	state := stateOf(i)
	key := execKey(c.ShardId, i.NamespaceId, i.WorkflowId, state.RunId)
	old, e := loadImage(tx, key)
	if e != nil {
		return e
	}
	if isNew {
		if old != nil {
			return execFail(wire.ExecutionResult_WORKFLOW_CONDITION_FAILED, "workflow already exists")
		}
	} else if e = workflowCheck(old, i); e != nil {
		return e
	}
	image := proto.Clone(i).(*wire.ExecutionImage)
	image.RunId = state.RunId
	image.ExecutionStateBlob = &wire.HistoryBlob{Data: image.ExecutionStateProto, Encoding: int32(enumspb.ENCODING_TYPE_PROTO3)}
	image.Tasks = nil
	if e = stageExecutionTasks(tx, c.ShardId, i.Tasks); e != nil {
		return e
	}
	if proto.Size(image) > 2<<20 {
		return status.Error(codes.ResourceExhausted, "workflow image exceeds 2 MiB")
	}
	return putHistory(tx, key, image)
}
func mergeExecutionMap[K comparable, V any](old, up map[K]V, del []K) map[K]V {
	if old == nil {
		old = make(map[K]V)
	}
	for k, v := range up {
		old[k] = v
	}
	for _, k := range del {
		delete(old, k)
	}
	return old
}
func stageMutation(tx *native.DbTransaction, c *wire.ExecutionCommand, m *wire.ExecutionMutation) error {
	i := m.Upsert
	key := execKey(c.ShardId, i.NamespaceId, i.WorkflowId, stateOf(i).RunId)
	old, e := loadImage(tx, key)
	if e != nil {
		return e
	}
	if e = workflowCheck(old, i); e != nil {
		return e
	}
	next := proto.Clone(i).(*wire.ExecutionImage)
	next.Activities = mergeExecutionMap(old.Activities, i.Activities, m.DeleteActivities)
	next.Timers = mergeExecutionMap(old.Timers, i.Timers, m.DeleteTimers)
	next.Children = mergeExecutionMap(old.Children, i.Children, m.DeleteChildren)
	next.Cancels = mergeExecutionMap(old.Cancels, i.Cancels, m.DeleteCancels)
	next.Signals = mergeExecutionMap(old.Signals, i.Signals, m.DeleteSignals)
	next.Chasm = mergeExecutionMap(old.Chasm, i.Chasm, m.DeleteChasm)
	ids := map[string]bool{}
	for _, id := range old.SignalRequestedIds {
		ids[id] = true
	}
	for _, id := range i.SignalRequestedIds {
		ids[id] = true
	}
	for _, id := range m.DeleteSignalRequestedIds {
		delete(ids, id)
	}
	next.SignalRequestedIds = nil
	for id := range ids {
		next.SignalRequestedIds = append(next.SignalRequestedIds, id)
	}
	sort.Strings(next.SignalRequestedIds)
	if !m.ClearBufferedEvents {
		next.BufferedEvents = old.BufferedEvents
	}
	if m.NewBufferedEvents != nil {
		next.BufferedEvents = append(next.BufferedEvents, m.NewBufferedEvents)
	}
	// stageSnapshot performs the same version guard and atomically inserts tasks.
	return stageSnapshot(tx, c, next, false)
}
func executionTaskKey(shard int32, t *wire.ExecutionTask) string {
	prefix := fmt.Sprintf("v1/history-task/%d/%010d/%010d/", t.CategoryType, shard, t.CategoryId)
	if t.CategoryType == 2 {
		return fmt.Sprintf("%s%016x/%08x/%016x", prefix, uint64(t.FireSeconds)^1<<63, uint32(t.FireNanos/1000*1000), uint64(t.TaskId)^1<<63)
	}
	return fmt.Sprintf("%s%016x", prefix, uint64(t.TaskId)^1<<63)
}
func stageExecutionTasks(tx *native.DbTransaction, shard int32, list []*wire.ExecutionTask) error {
	for _, t := range list {
		if t.CategoryType != 1 && t.CategoryType != 2 {
			return execFail(wire.ExecutionResult_INTERNAL, "unknown history task category type")
		}
		key := executionTaskKey(shard, t)
		old, e := get(tx, key)
		if e != nil {
			return e
		}
		if old != nil {
			return execFail(wire.ExecutionResult_UNAVAILABLE, "history task already exists")
		}
		stored := proto.Clone(t).(*wire.ExecutionTask)
		if stored.CategoryType == 2 {
			stored.FireNanos = stored.FireNanos / 1000 * 1000
		}
		if e = putHistory(tx, key, stored); e != nil {
			return e
		}
	}
	return nil
}
func applyExecution(tx *native.DbTransaction, c *wire.ExecutionCommand) (*wire.ExecutionResult, error) {
	r := new(wire.ExecutionResult)
	if c.Kind == wire.ExecutionCommand_LIST {
		var token struct {
			Shard int32
			Key   string
		}
		if len(c.NextPageToken) > 0 {
			if e := json.Unmarshal(c.NextPageToken, &token); e != nil || token.Shard != c.ShardId || !strings.HasPrefix(token.Key, execPrefix(c.ShardId)) {
				return nil, status.Error(codes.InvalidArgument, "invalid workflow cursor")
			}
		}
		last := ""
		e := historyScan(tx, execPrefix(c.ShardId), false, func(k, v []byte) (bool, error) {
			if string(k) <= token.Key {
				return false, nil
			}
			i := new(wire.ExecutionImage)
			if e := proto.Unmarshal(v, i); e != nil {
				return false, backend(e)
			}
			r.Images = append(r.Images, i)
			next, _ := json.Marshal(struct {
				Shard int32
				Key   string
			}{c.ShardId, string(k)})
			r.NextPageToken = next
			if proto.Size(r) > 3<<20 {
				r.Images = r.Images[:len(r.Images)-1]
				if last == "" {
					return false, status.Error(codes.ResourceExhausted, "workflow exceeds page budget")
				}
				r.NextPageToken, _ = json.Marshal(struct {
					Shard int32
					Key   string
				}{c.ShardId, last})
				return true, nil
			}
			last = string(k)
			return int64(len(r.Images)) >= c.PageSize, nil
		})
		return r, e
	}
	if c.Kind == wire.ExecutionCommand_GET || c.Kind == wire.ExecutionCommand_DELETE {
		key := execKey(c.ShardId, c.NamespaceId, c.WorkflowId, c.RunId)
		if c.Kind == wire.ExecutionCommand_DELETE {
			return r, backend(tx.Delete([]byte(key)))
		}
		i, e := loadImage(tx, key)
		if e != nil {
			return nil, e
		}
		if i == nil {
			return nil, execFail(wire.ExecutionResult_NOT_FOUND, "workflow does not exist")
		}
		r.Image = i
		return r, nil
	}
	if c.Kind == wire.ExecutionCommand_GET_CURRENT || c.Kind == wire.ExecutionCommand_DELETE_CURRENT {
		key := currentKey(c, c.NamespaceId, c.WorkflowId)
		cur, e := loadCurrent(tx, key)
		if e != nil {
			return nil, e
		}
		if c.Kind == wire.ExecutionCommand_DELETE_CURRENT {
			if cur != nil && cur.RunId == canonicalExecutionID(c.RunId) {
				return r, backend(tx.Delete([]byte(key)))
			}
			return r, nil
		}
		if cur == nil {
			return nil, execFail(wire.ExecutionResult_NOT_FOUND, "current workflow does not exist")
		}
		state := new(persistencespb.WorkflowExecutionState)
		if e = proto.Unmarshal(cur.StateProto, state); e != nil {
			return nil, backend(e)
		}
		cur.StateProto, e = proto.Marshal(&persistencespb.WorkflowExecutionState{CreateRequestId: state.CreateRequestId, State: state.State, Status: state.Status})
		r.Current = cur
		return r, e
	}
	shardRaw, e := get(tx, fmt.Sprintf("v1/shard/%010d", c.ShardId))
	if e != nil {
		return nil, e
	}
	shard := new(wire.StoredShard)
	if shardRaw != nil {
		if e = proto.Unmarshal(shardRaw, shard); e != nil {
			return nil, backend(e)
		}
	}
	if shardRaw == nil {
		return nil, execFail(wire.ExecutionResult_UNAVAILABLE, "shard does not exist")
	}
	if shard.RangeId != c.RangeId {
		return nil, &executionFailure{&wire.ExecutionResult{Error: wire.ExecutionResult_OWNERSHIP_LOST, ShardId: c.ShardId, Message: "shard range mismatch"}}
	}
	if c.Kind == wire.ExecutionCommand_SET {
		return r, stageSnapshot(tx, c, c.Snapshot, false)
	}
	image := c.Snapshot
	if c.Kind == wire.ExecutionCommand_UPDATE {
		image = c.Mutation.Upsert
	}
	key := currentKey(c, image.NamespaceId, image.WorkflowId)
	physical, e := loadCurrent(tx, key)
	if e != nil {
		return nil, e
	}
	cur := physical
	// Create's SQL guard joins the current pointer to its execution row.
	if c.Kind == wire.ExecutionCommand_CREATE && cur != nil {
		old, e := loadImage(tx, execKey(c.ShardId, image.NamespaceId, image.WorkflowId, cur.RunId))
		if e != nil {
			return nil, e
		}
		if old == nil {
			cur = nil
		} else {
			cur = proto.Clone(cur).(*wire.ExecutionCurrent)
			cur.LastWriteVersion = old.LastWriteVersion
		}
	}
	updateCurrent := false
	replacement := image
	expected := stateOf(image).RunId
	switch c.Kind {
	case wire.ExecutionCommand_CREATE:
		switch c.Mode {
		case 0:
			if cur != nil && cur.RunId != canonicalExecutionID(c.PreviousRunId) {
				return nil, currentFailure(cur)
			}
			if physical != nil {
				return nil, execFail(wire.ExecutionResult_UNAVAILABLE, "current workflow already exists")
			}
			updateCurrent = true
		case 1:
			if cur == nil || cur.RunId != canonicalExecutionID(c.PreviousRunId) || cur.LastWriteVersion != c.PreviousLastWriteVersion {
				return nil, currentFailure(cur)
			}
			state := new(persistencespb.WorkflowExecutionState)
			if e = proto.Unmarshal(cur.StateProto, state); e != nil {
				return nil, backend(e)
			}
			if state.State != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
				return nil, currentFailure(cur)
			}
			updateCurrent = true
		case 2:
			if cur != nil && cur.RunId == expected {
				return nil, currentFailure(cur)
			}
		}
	case wire.ExecutionCommand_UPDATE:
		if c.NewSnapshot != nil {

			replacement = c.NewSnapshot
		}
		switch c.Mode {
		case 0:
			if c.NewSnapshot != nil && canonicalExecutionID(c.NewSnapshot.NamespaceId) != canonicalExecutionID(image.NamespaceId) {
				return nil, execFail(wire.ExecutionResult_UNAVAILABLE, "new workflow namespace mismatch")
			}
			if cur == nil {
				return nil, execFail(wire.ExecutionResult_UNAVAILABLE, "current workflow does not exist")
			}
			if cur.RunId != expected {
				return nil, currentFailure(cur)
			}
			updateCurrent = true
		case 1:
			if cur != nil && cur.RunId == expected {
				return nil, currentFailure(cur)
			}
		}
	case wire.ExecutionCommand_CONFLICT_RESOLVE:
		if c.NewSnapshot != nil {
			replacement = c.NewSnapshot
		}
		if c.CurrentMutation != nil {
			expected = stateOf(c.CurrentMutation.Upsert).RunId
		}
		switch c.Mode {
		case 0:
			if cur == nil {
				return nil, execFail(wire.ExecutionResult_UNAVAILABLE, "current workflow does not exist")
			}
			if cur.RunId != expected {
				return nil, currentFailure(cur)
			}
			updateCurrent = true
		case 1:
			if cur != nil && cur.RunId == stateOf(image).RunId {
				return nil, currentFailure(cur)
			}
		}
	}
	if updateCurrent {
		run := stateOf(replacement).RunId
		if c.Kind == wire.ExecutionCommand_CREATE {
			run = canonicalExecutionID(replacement.RunId)
		}
		currentState := stateOf(replacement)
		currentState.RequestIds = nil
		if b := replacement.ExecutionStateBlob; b != nil {
			decoded, err := serialization.NewSerializer().WorkflowExecutionStateFromBlob(&commonpb.DataBlob{Data: b.Data, EncodingType: enumspb.EncodingType(b.Encoding)})
			if err != nil {
				return nil, execFail(wire.ExecutionResult_UNAVAILABLE, "invalid current execution state blob: "+err.Error())
			}
			currentState.RequestIds = decoded.RequestIds
		}
		currentRaw, err := proto.Marshal(currentState)
		if err != nil {
			return nil, backend(err)
		}
		newCurrent := &wire.ExecutionCurrent{RunId: run, StateProto: currentRaw, StateBlob: replacement.ExecutionStateBlob, LastWriteVersion: replacement.LastWriteVersion}
		if e = putHistory(tx, key, newCurrent); e != nil {
			return nil, e
		}
	}
	switch c.Kind {
	case wire.ExecutionCommand_CREATE:
		e = stageSnapshot(tx, c, image, true)
	case wire.ExecutionCommand_UPDATE:
		e = stageMutation(tx, c, c.Mutation)
		if e == nil && c.NewSnapshot != nil {
			e = stageSnapshot(tx, c, c.NewSnapshot, true)
		}
	case wire.ExecutionCommand_CONFLICT_RESOLVE:
		e = stageSnapshot(tx, c, image, false)
		if e == nil && c.CurrentMutation != nil {
			e = stageMutation(tx, c, c.CurrentMutation)
		}
		if e == nil && c.NewSnapshot != nil {
			e = stageSnapshot(tx, c, c.NewSnapshot, true)
		}
	}
	return r, e
}
