package persistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"strings"
)

func ValidateExecutionTasksCommand(c *wire.ExecutionTasksCommand) error {
	if c == nil {
		return status.Error(codes.InvalidArgument, "invalid task command")
	}
	if c.Kind < wire.ExecutionTasksCommand_ADD || c.Kind > wire.ExecutionTasksCommand_IS_EMPTY_DLQ || c.ShardId < 0 || len(c.SourceCluster) > 1024 || len(c.NextPageToken) > 4096 {
		return status.Error(codes.InvalidArgument, "invalid execution task command")
	}
	if c.Kind == wire.ExecutionTasksCommand_READ_DLQ && (c.PageSize < 1 || c.PageSize > 1000) {
		return status.Error(codes.InvalidArgument, "DLQ page size must be 1..1000")
	}
	for _, t := range c.Tasks {
		if t == nil || t.Blob == nil || t.FireNanos < 0 || t.FireNanos >= 1e9 {
			return status.Error(codes.InvalidArgument, "invalid history task")
		}
	}
	if c.Kind == wire.ExecutionTasksCommand_PUT_DLQ {
		if c.TaskInfo == nil {
			return status.Error(codes.InvalidArgument, "missing replication info")
		}
		info, e := serialization.NewSerializer().ReplicationTaskInfoFromBlob(&commonpb.DataBlob{Data: c.TaskInfo.Data, EncodingType: enumspb.EncodingType(c.TaskInfo.Encoding)})
		if e != nil || info.TaskId != c.TaskId {
			return status.Error(codes.InvalidArgument, "invalid replication task info")
		}
	}
	return nil
}
func ApplyExecutionTasks(ctx context.Context, tx ClusterTransaction, c *wire.ExecutionTasksCommand) (*wire.StoredOutcome, error) {
	r, err := applyExecutionTasks(ctx, tx, c)
	if err != nil {
		return nil, err
	}
	return &wire.StoredOutcome{Result: &wire.StoredOutcome_ExecutionTasksResult{ExecutionTasksResult: r}}, nil
}
func executionTasksOutcome(r *wire.ExecutionTasksResult) *wire.StoredOutcome {
	return &wire.StoredOutcome{Result: &wire.StoredOutcome_ExecutionTasksResult{ExecutionTasksResult: r}}
}
func replicationPrefix(c *wire.ExecutionTasksCommand) string {
	return fmt.Sprintf("v1/replication-dlq/%s/%010d/", hex.EncodeToString([]byte(c.SourceCluster)), c.ShardId)
}
func replicationKey(c *wire.ExecutionTasksCommand, id int64) string {
	return fmt.Sprintf("%s%016x", replicationPrefix(c), uint64(id)^1<<63)
}

type replicationCursor struct {
	Version int
	Binding []byte
	Last    string
}

func applyExecutionTasks(ctx context.Context, tx ClusterTransaction, c *wire.ExecutionTasksCommand) (*wire.ExecutionTasksResult, error) {
	r := new(wire.ExecutionTasksResult)
	switch c.Kind {
	case wire.ExecutionTasksCommand_ADD:
		b, e := getExecution(ctx, tx, fmt.Sprintf("v1/shard/%010d", c.ShardId))
		if e != nil {
			return nil, e
		}
		shard := new(wire.StoredShard)
		if b != nil {
			if e = proto.Unmarshal(b, shard); e != nil {
				return nil, clusterEncodingError(e)
			}
		}
		if b == nil {
			return &wire.ExecutionTasksResult{Error: wire.ExecutionTasksResult_UNAVAILABLE, Message: "shard does not exist"}, nil
		}
		if shard.RangeId != c.RangeId {
			return &wire.ExecutionTasksResult{Error: wire.ExecutionTasksResult_OWNERSHIP_LOST, ShardId: c.ShardId, Message: "shard range mismatch"}, nil
		}
		return r, stageExecutionTasks(ctx, tx, c.ShardId, c.Tasks)
	case wire.ExecutionTasksCommand_PUT_DLQ:
		key := replicationKey(c, c.TaskId)
		old, e := getExecution(ctx, tx, key)
		if e != nil {
			return nil, e
		}
		if old != nil {
			return r, nil
		}
		return r, saveCluster(tx, key, &wire.ExecutionTask{CategoryId: 3, CategoryType: 1, TaskId: c.TaskId, Blob: c.TaskInfo})
	case wire.ExecutionTasksCommand_DELETE_DLQ:
		return r, tx.Delete([]byte(replicationKey(c, c.TaskId)))
	}
	prefix := replicationPrefix(c)
	low := replicationKey(c, c.MinimumId)
	high := replicationKey(c, c.MaximumId)
	normalized := proto.Clone(c).(*wire.ExecutionTasksCommand)
	normalized.NextPageToken = nil
	normalized.PageSize = 0
	b, e := proto.MarshalOptions{Deterministic: true}.Marshal(normalized)
	if e != nil {
		return nil, clusterEncodingError(e)
	}
	binding := sha256.Sum256(b)
	after := ""
	if len(c.NextPageToken) > 0 {
		var cursor replicationCursor
		if e = json.Unmarshal(c.NextPageToken, &cursor); e != nil || cursor.Version != 1 || !bytes.Equal(cursor.Binding, binding[:]) || !strings.HasPrefix(cursor.Last, prefix) || cursor.Last < low || cursor.Last >= high {
			return &wire.ExecutionTasksResult{Error: wire.ExecutionTasksResult_INTERNAL, Message: "invalid replication cursor"}, nil
		}
		after = cursor.Last
	}
	r.Empty = true
	last := ""
	e = historyScan(ctx, tx, prefix, false, func(k, v []byte) (bool, error) {
		key := string(k)
		if key < low || key <= after {
			return false, nil
		}
		if c.Kind != wire.ExecutionTasksCommand_IS_EMPTY_DLQ && key >= high {
			return true, nil
		}
		if c.Kind == wire.ExecutionTasksCommand_IS_EMPTY_DLQ {
			r.Empty = false
			return true, nil
		}
		if c.Kind == wire.ExecutionTasksCommand_RANGE_DELETE_DLQ {
			return false, tx.Delete(k)
		}
		task := new(wire.ExecutionTask)
		if e := proto.Unmarshal(v, task); e != nil {
			return false, clusterEncodingError(e)
		}
		if replicationKey(c, task.TaskId) != key || task.Blob == nil {
			return false, status.Error(codes.Unavailable, "corrupt replication task")
		}
		r.Tasks = append(r.Tasks, task)
		r.NextPageToken, _ = json.Marshal(replicationCursor{1, binding[:], key})
		if proto.Size(r) > 3<<20 {
			r.Tasks = r.Tasks[:len(r.Tasks)-1]
			if last == "" {
				return false, status.Error(codes.ResourceExhausted, "replication task exceeds page budget")
			}
			r.NextPageToken, _ = json.Marshal(replicationCursor{1, binding[:], last})
			return true, nil
		}
		last = key
		return int64(len(r.Tasks)) >= c.PageSize, nil
	})
	return r, e
}

func ReplicationTaskKey(c *wire.ExecutionTasksCommand, id int64) string { return replicationKey(c, id) }
