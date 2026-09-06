package persistence

import (
	"context"
	"encoding/hex"
	"encoding/json"
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
	"sort"
	"strings"
)

// ExecutionFailure requires rollback before journaling the logical result.
type ExecutionFailure struct{ Result *wire.ExecutionResult }

func (e *ExecutionFailure) Error() string { return e.Result.Message }
func execFail(kind wire.ExecutionResult_Error, msg string) error {
	return &ExecutionFailure{&wire.ExecutionResult{Error: kind, Message: msg}}
}
func getExecution(ctx context.Context, tx ClusterTransaction, key string) ([]byte, error) {
	return tx.Get(ctx, []byte(key))
}
func ApplyExecution(ctx context.Context, tx ClusterTransaction, c *wire.ExecutionCommand) (*wire.StoredOutcome, error) {
	r, err := applyExecution(ctx, tx, c)
	if err != nil {
		return nil, err
	}
	return executionOutcome(r), nil
}

// The following adapters keep existing task families on these same semantics
// while their native handlers await extraction.
func StageExecutionTasks(ctx context.Context, tx ClusterTransaction, shard int32, tasks []*wire.ExecutionTask) error {
	return stageExecutionTasks(ctx, tx, shard, tasks)
}
func ExecutionTaskKey(shard int32, task *wire.ExecutionTask) string {
	return executionTaskKey(shard, task)
}
func ExecutionKey(shard int32, ns, wf, run string) string { return execKey(shard, ns, wf, run) }
func LoadExecutionImage(ctx context.Context, tx ClusterTransaction, key string) (*wire.ExecutionImage, error) {
	return loadImage(ctx, tx, key)
}
func executionOutcome(r *wire.ExecutionResult) *wire.StoredOutcome {
	return &wire.StoredOutcome{Result: &wire.StoredOutcome_ExecutionResult{ExecutionResult: r}}
}
func ValidateExecutionCommand(c *wire.ExecutionCommand) error {
	bad := func() error { return status.Error(codes.InvalidArgument, "invalid execution command") }
	if c == nil {
		return bad()
	}
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
func loadImage(ctx context.Context, tx ClusterTransaction, key string) (*wire.ExecutionImage, error) {
	raw, e := getExecution(ctx, tx, key)
	if e != nil || raw == nil {
		return nil, e
	}
	i := new(wire.ExecutionImage)
	if e = proto.Unmarshal(raw, i); e != nil {
		return nil, clusterEncodingError(e)
	}
	return i, nil
}
func loadCurrent(ctx context.Context, tx ClusterTransaction, key string) (*wire.ExecutionCurrent, error) {
	raw, e := getExecution(ctx, tx, key)
	if e != nil || raw == nil {
		return nil, e
	}
	i := new(wire.ExecutionCurrent)
	if e = proto.Unmarshal(raw, i); e != nil {
		return nil, clusterEncodingError(e)
	}
	return i, nil
}
func currentFailure(current *wire.ExecutionCurrent) error {
	return &ExecutionFailure{&wire.ExecutionResult{Error: wire.ExecutionResult_CURRENT_CONDITION_FAILED, Message: "current workflow condition failed", Current: current}}
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
		return &ExecutionFailure{&wire.ExecutionResult{Error: wire.ExecutionResult_WORKFLOW_CONDITION_FAILED, Message: "workflow version condition failed", ActualNextEventId: old.NextEventId, ActualDbRecordVersion: old.DbRecordVersion}}
	}
	return nil
}
func stageSnapshot(ctx context.Context, tx ClusterTransaction, c *wire.ExecutionCommand, i *wire.ExecutionImage, isNew bool) error {
	state := stateOf(i)
	key := execKey(c.ShardId, i.NamespaceId, i.WorkflowId, state.RunId)
	old, e := loadImage(ctx, tx, key)
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
	if e = stageExecutionTasks(ctx, tx, c.ShardId, i.Tasks); e != nil {
		return e
	}
	if proto.Size(image) > 2<<20 {
		return status.Error(codes.ResourceExhausted, "workflow image exceeds 2 MiB")
	}
	return saveCluster(tx, key, image)
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
func stageMutation(ctx context.Context, tx ClusterTransaction, c *wire.ExecutionCommand, m *wire.ExecutionMutation) error {
	i := m.Upsert
	key := execKey(c.ShardId, i.NamespaceId, i.WorkflowId, stateOf(i).RunId)
	old, e := loadImage(ctx, tx, key)
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
	return stageSnapshot(ctx, tx, c, next, false)
}
func executionTaskKey(shard int32, t *wire.ExecutionTask) string {
	prefix := fmt.Sprintf("v1/history-task/%d/%010d/%010d/", t.CategoryType, shard, t.CategoryId)
	if t.CategoryType == 2 {
		return fmt.Sprintf("%s%016x/%08x/%016x", prefix, uint64(t.FireSeconds)^1<<63, uint32(t.FireNanos/1000*1000), uint64(t.TaskId)^1<<63)
	}
	return fmt.Sprintf("%s%016x", prefix, uint64(t.TaskId)^1<<63)
}
func stageExecutionTasks(ctx context.Context, tx ClusterTransaction, shard int32, list []*wire.ExecutionTask) error {
	for _, t := range list {
		if t.CategoryType != 1 && t.CategoryType != 2 {
			return execFail(wire.ExecutionResult_INTERNAL, "unknown history task category type")
		}
		key := executionTaskKey(shard, t)
		old, e := getExecution(ctx, tx, key)
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
		if e = saveCluster(tx, key, stored); e != nil {
			return e
		}
	}
	return nil
}
func applyExecution(ctx context.Context, tx ClusterTransaction, c *wire.ExecutionCommand) (*wire.ExecutionResult, error) {
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
		e := historyScan(ctx, tx, execPrefix(c.ShardId), false, func(k, v []byte) (bool, error) {
			if string(k) <= token.Key {
				return false, nil
			}
			i := new(wire.ExecutionImage)
			if e := proto.Unmarshal(v, i); e != nil {
				return false, clusterEncodingError(e)
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
			return r, tx.Delete([]byte(key))
		}
		i, e := loadImage(ctx, tx, key)
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
		cur, e := loadCurrent(ctx, tx, key)
		if e != nil {
			return nil, e
		}
		if c.Kind == wire.ExecutionCommand_DELETE_CURRENT {
			if cur != nil && cur.RunId == canonicalExecutionID(c.RunId) {
				return r, tx.Delete([]byte(key))
			}
			return r, nil
		}
		if cur == nil {
			return nil, execFail(wire.ExecutionResult_NOT_FOUND, "current workflow does not exist")
		}
		state := new(persistencespb.WorkflowExecutionState)
		if e = proto.Unmarshal(cur.StateProto, state); e != nil {
			return nil, clusterEncodingError(e)
		}
		cur.StateProto, e = proto.Marshal(&persistencespb.WorkflowExecutionState{CreateRequestId: state.CreateRequestId, State: state.State, Status: state.Status})
		r.Current = cur
		return r, e
	}
	shardRaw, e := getExecution(ctx, tx, fmt.Sprintf("v1/shard/%010d", c.ShardId))
	if e != nil {
		return nil, e
	}
	shard := new(wire.StoredShard)
	if shardRaw != nil {
		if e = proto.Unmarshal(shardRaw, shard); e != nil {
			return nil, clusterEncodingError(e)
		}
	}
	if shardRaw == nil {
		return nil, execFail(wire.ExecutionResult_UNAVAILABLE, "shard does not exist")
	}
	if shard.RangeId != c.RangeId {
		return nil, &ExecutionFailure{&wire.ExecutionResult{Error: wire.ExecutionResult_OWNERSHIP_LOST, ShardId: c.ShardId, Message: "shard range mismatch"}}
	}
	if c.Kind == wire.ExecutionCommand_SET {
		return r, stageSnapshot(ctx, tx, c, c.Snapshot, false)
	}
	image := c.Snapshot
	if c.Kind == wire.ExecutionCommand_UPDATE {
		image = c.Mutation.Upsert
	}
	key := currentKey(c, image.NamespaceId, image.WorkflowId)
	physical, e := loadCurrent(ctx, tx, key)
	if e != nil {
		return nil, e
	}
	cur := physical
	// Create's SQL guard joins the current pointer to its execution row.
	if c.Kind == wire.ExecutionCommand_CREATE && cur != nil {
		old, e := loadImage(ctx, tx, execKey(c.ShardId, image.NamespaceId, image.WorkflowId, cur.RunId))
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
				return nil, clusterEncodingError(e)
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
			return nil, clusterEncodingError(err)
		}
		newCurrent := &wire.ExecutionCurrent{RunId: run, StateProto: currentRaw, StateBlob: replacement.ExecutionStateBlob, LastWriteVersion: replacement.LastWriteVersion}
		if e = saveCluster(tx, key, newCurrent); e != nil {
			return nil, e
		}
	}
	switch c.Kind {
	case wire.ExecutionCommand_CREATE:
		e = stageSnapshot(ctx, tx, c, image, true)
	case wire.ExecutionCommand_UPDATE:
		e = stageMutation(ctx, tx, c, c.Mutation)
		if e == nil && c.NewSnapshot != nil {
			e = stageSnapshot(ctx, tx, c, c.NewSnapshot, true)
		}
	case wire.ExecutionCommand_CONFLICT_RESOLVE:
		e = stageSnapshot(ctx, tx, c, image, false)
		if e == nil && c.CurrentMutation != nil {
			e = stageMutation(ctx, tx, c, c.CurrentMutation)
		}
		if e == nil && c.NewSnapshot != nil {
			e = stageSnapshot(ctx, tx, c, c.NewSnapshot, true)
		}
	}
	return r, e
}
