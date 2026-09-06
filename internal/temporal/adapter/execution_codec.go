package adapter

import (
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
	"sort"
	"time"
)

func executionBlobs[K comparable](in map[K]*commonpb.DataBlob) map[K]*wire.HistoryBlob {
	if in == nil {
		return nil
	}
	out := make(map[K]*wire.HistoryBlob, len(in))
	for k, v := range in {
		out[k] = historyBlob(v)
	}
	return out
}
func fromExecutionBlobs[K comparable](in map[K]*wire.HistoryBlob) map[K]*commonpb.DataBlob {
	if in == nil {
		return nil
	}
	out := make(map[K]*commonpb.DataBlob, len(in))
	for k, v := range in {
		out[k] = fromHistoryBlob(v)
	}
	return out
}
func executionChasm(in map[string]p.InternalChasmNode) map[string]*wire.ExecutionChasmNode {
	out := make(map[string]*wire.ExecutionChasmNode, len(in))
	for k, v := range in {
		out[k] = &wire.ExecutionChasmNode{Metadata: historyBlob(v.Metadata), Data: historyBlob(v.Data), CassandraBlob: historyBlob(v.CassandraBlob)}
	}
	return out
}
func executionKeys[K ~int64 | ~string](m map[K]struct{}) []K {
	out := make([]K, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
func executionTasks(in map[tasks.Category][]p.InternalHistoryTask) []*wire.ExecutionTask {
	var out []*wire.ExecutionTask
	for category, list := range in {
		for _, t := range list {
			out = append(out, &wire.ExecutionTask{CategoryId: int32(category.ID()), CategoryType: int32(category.Type()), TaskId: t.Key.TaskID, FireSeconds: t.Key.FireTime.Unix(), FireNanos: int32(t.Key.FireTime.Nanosecond()), Blob: historyBlob(t.Blob)})
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.CategoryId != b.CategoryId {
			return a.CategoryId < b.CategoryId
		}
		if a.CategoryType != b.CategoryType {
			return a.CategoryType < b.CategoryType
		}
		if a.FireSeconds != b.FireSeconds {
			return a.FireSeconds < b.FireSeconds
		}
		if a.FireNanos != b.FireNanos {
			return a.FireNanos < b.FireNanos
		}
		return a.TaskId < b.TaskId
	})
	return out
}
func executionSnapshot(s *p.InternalWorkflowSnapshot) (*wire.ExecutionImage, error) {
	if s == nil {
		return nil, nil
	}
	info, e := proto.Marshal(s.ExecutionInfo)
	if e != nil {
		return nil, e
	}
	state, e := proto.Marshal(s.ExecutionState)
	if e != nil {
		return nil, e
	}
	return &wire.ExecutionImage{NamespaceId: s.NamespaceID, WorkflowId: s.WorkflowID, RunId: s.RunID, ExecutionInfoProto: info, ExecutionStateProto: state, ExecutionInfoBlob: historyBlob(s.ExecutionInfoBlob), ExecutionStateBlob: historyBlob(s.ExecutionStateBlob), NextEventId: s.NextEventID, StartVersion: s.StartVersion, LastWriteVersion: s.LastWriteVersion, DbRecordVersion: s.DBRecordVersion, Condition: s.Condition, Activities: executionBlobs(s.ActivityInfos), Timers: executionBlobs(s.TimerInfos), Children: executionBlobs(s.ChildExecutionInfos), Cancels: executionBlobs(s.RequestCancelInfos), Signals: executionBlobs(s.SignalInfos), Chasm: executionChasm(s.ChasmNodes), SignalRequestedIds: executionKeys(s.SignalRequestedIDs), Checksum: historyBlob(s.Checksum), Tasks: executionTasks(s.Tasks)}, nil
}
func executionMutation(m *p.InternalWorkflowMutation) (*wire.ExecutionMutation, error) {
	if m == nil {
		return nil, nil
	}
	i, e := executionSnapshot(&p.InternalWorkflowSnapshot{NamespaceID: m.NamespaceID, WorkflowID: m.WorkflowID, RunID: m.RunID, ExecutionInfo: m.ExecutionInfo, ExecutionState: m.ExecutionState, ExecutionInfoBlob: m.ExecutionInfoBlob, ExecutionStateBlob: m.ExecutionStateBlob, NextEventID: m.NextEventID, StartVersion: m.StartVersion, LastWriteVersion: m.LastWriteVersion, DBRecordVersion: m.DBRecordVersion, Condition: m.Condition, ActivityInfos: m.UpsertActivityInfos, TimerInfos: m.UpsertTimerInfos, ChildExecutionInfos: m.UpsertChildExecutionInfos, RequestCancelInfos: m.UpsertRequestCancelInfos, SignalInfos: m.UpsertSignalInfos, ChasmNodes: m.UpsertChasmNodes, SignalRequestedIDs: m.UpsertSignalRequestedIDs, Checksum: m.Checksum, Tasks: m.Tasks})
	if e != nil {
		return nil, e
	}
	return &wire.ExecutionMutation{Upsert: i, DeleteActivities: executionKeys(m.DeleteActivityInfos), DeleteTimers: executionKeys(m.DeleteTimerInfos), DeleteChildren: executionKeys(m.DeleteChildExecutionInfos), DeleteCancels: executionKeys(m.DeleteRequestCancelInfos), DeleteSignals: executionKeys(m.DeleteSignalInfos), DeleteChasm: executionKeys(m.DeleteChasmNodes), DeleteSignalRequestedIds: executionKeys(m.DeleteSignalRequestedIDs), ClearBufferedEvents: m.ClearBufferedEvents, NewBufferedEvents: historyBlob(m.NewBufferedEvents)}, nil
}
func executionMutableState(i *wire.ExecutionImage) *p.InternalWorkflowMutableState {
	out := &p.InternalWorkflowMutableState{ExecutionInfo: fromHistoryBlob(i.ExecutionInfoBlob), ExecutionState: fromHistoryBlob(i.ExecutionStateBlob), NextEventID: i.NextEventId, DBRecordVersion: i.DbRecordVersion, Checksum: fromHistoryBlob(i.Checksum), ActivityInfos: fromExecutionBlobs(i.Activities), TimerInfos: fromExecutionBlobs(i.Timers), ChildExecutionInfos: fromExecutionBlobs(i.Children), RequestCancelInfos: fromExecutionBlobs(i.Cancels), SignalInfos: fromExecutionBlobs(i.Signals), SignalRequestedIDs: i.SignalRequestedIds, ChasmNodes: make(map[string]p.InternalChasmNode, len(i.Chasm))}
	for k, v := range i.Chasm {
		if v != nil {
			out.ChasmNodes[k] = p.InternalChasmNode{Metadata: fromHistoryBlob(v.Metadata), Data: fromHistoryBlob(v.Data), CassandraBlob: fromHistoryBlob(v.CassandraBlob)}
		}
	}
	for _, b := range i.BufferedEvents {
		out.BufferedEvents = append(out.BufferedEvents, fromHistoryBlob(b))
	}
	return out
}
func executionStartTime(s *persistencespb.WorkflowExecutionState) *time.Time {
	if s.StartTime == nil {
		return nil
	}
	t := s.StartTime.AsTime()
	return &t
}
