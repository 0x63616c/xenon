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
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	native "slatedb.io/slatedb-go/uniffi"
	"strings"
)

type ExecutionTasksServer struct {
	wire.UnimplementedExecutionTasksPersistenceServer
	Owner *Owner
}

func (s *ExecutionTasksServer) Execute(ctx context.Context, q *wire.ExecutionTasksRequest) (*wire.ExecutionTasksResult, error) {
	if q == nil || q.ProtocolVersion != 1 || q.Partition != s.Owner.config.Partition || !operationID.MatchString(q.OperationId) || q.Command == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid execution task envelope")
	}
	q = proto.Clone(q).(*wire.ExecutionTasksRequest)
	c := q.Command
	if c.Kind < wire.ExecutionTasksCommand_ADD || c.Kind > wire.ExecutionTasksCommand_IS_EMPTY_DLQ || c.ShardId < 0 || len(c.SourceCluster) > 1024 || len(c.NextPageToken) > 4096 {
		return nil, status.Error(codes.InvalidArgument, "invalid execution task command")
	}
	if c.Kind == wire.ExecutionTasksCommand_READ_DLQ && (c.PageSize < 1 || c.PageSize > 1000) {
		return nil, status.Error(codes.InvalidArgument, "DLQ page size must be 1..1000")
	}
	for _, t := range c.Tasks {
		if t == nil || t.Blob == nil || t.FireNanos < 0 || t.FireNanos >= 1e9 {
			return nil, status.Error(codes.InvalidArgument, "invalid history task")
		}
	}
	if c.Kind == wire.ExecutionTasksCommand_PUT_DLQ {
		if c.TaskInfo == nil {
			return nil, status.Error(codes.InvalidArgument, "missing replication info")
		}
		info, e := serialization.NewSerializer().ReplicationTaskInfoFromBlob(&commonpb.DataBlob{Data: c.TaskInfo.Data, EncodingType: enumspb.EncodingType(c.TaskInfo.Encoding)})
		if e != nil || info.TaskId != c.TaskId {
			return nil, status.Error(codes.InvalidArgument, "invalid replication task info")
		}
	}
	b, e := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if e != nil {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	d := sha256.Sum256(b)
	if !bytes.Equal(d[:], q.CommandSha256) {
		return nil, status.Error(codes.InvalidArgument, "digest mismatch")
	}
	raw, e := s.Owner.Run(ctx, func(*native.Db) ([]byte, error) {
		out, e := s.Owner.journal(q.OperationId, d[:], executionTasksFamily, func(tx *native.DbTransaction) (*wire.StoredOutcome, error) {
			r, e := applyExecutionTasks(tx, c)
			if e != nil {
				return nil, e
			}
			return executionTasksOutcome(r), nil
		})
		var failure *executionFailure
		if errors.As(e, &failure) {
			r := &wire.ExecutionTasksResult{Message: failure.result.Message, Error: wire.ExecutionTasksResult_UNAVAILABLE}
			if failure.result.Error == wire.ExecutionResult_INTERNAL {
				r.Error = wire.ExecutionTasksResult_INTERNAL
			}
			out, e = s.Owner.journal(q.OperationId, d[:], executionTasksFamily, func(*native.DbTransaction) (*wire.StoredOutcome, error) { return executionTasksOutcome(r), nil })
		}
		if e != nil {
			return nil, e
		}
		return proto.Marshal(out.GetExecutionTasksResult())
	})
	if e != nil {
		return nil, e
	}
	r := new(wire.ExecutionTasksResult)
	if e = proto.Unmarshal(raw, r); e != nil {
		return nil, backend(e)
	}
	return r, nil
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

func applyExecutionTasks(tx *native.DbTransaction, c *wire.ExecutionTasksCommand) (*wire.ExecutionTasksResult, error) {
	r := new(wire.ExecutionTasksResult)
	switch c.Kind {
	case wire.ExecutionTasksCommand_ADD:
		b, e := get(tx, fmt.Sprintf("v1/shard/%010d", c.ShardId))
		if e != nil {
			return nil, e
		}
		shard := new(wire.StoredShard)
		if b != nil {
			if e = proto.Unmarshal(b, shard); e != nil {
				return nil, backend(e)
			}
		}
		if b == nil {
			return &wire.ExecutionTasksResult{Error: wire.ExecutionTasksResult_UNAVAILABLE, Message: "shard does not exist"}, nil
		}
		if shard.RangeId != c.RangeId {
			return &wire.ExecutionTasksResult{Error: wire.ExecutionTasksResult_OWNERSHIP_LOST, ShardId: c.ShardId, Message: "shard range mismatch"}, nil
		}
		return r, stageExecutionTasks(tx, c.ShardId, c.Tasks)
	case wire.ExecutionTasksCommand_PUT_DLQ:
		key := replicationKey(c, c.TaskId)
		old, e := get(tx, key)
		if e != nil {
			return nil, e
		}
		if old != nil {
			return r, nil
		}
		return r, putHistory(tx, key, &wire.ExecutionTask{CategoryId: 3, CategoryType: 1, TaskId: c.TaskId, Blob: c.TaskInfo})
	case wire.ExecutionTasksCommand_DELETE_DLQ:
		return r, backend(tx.Delete([]byte(replicationKey(c, c.TaskId))))
	}
	prefix := replicationPrefix(c)
	low := replicationKey(c, c.MinimumId)
	high := replicationKey(c, c.MaximumId)
	normalized := proto.Clone(c).(*wire.ExecutionTasksCommand)
	normalized.NextPageToken = nil
	normalized.PageSize = 0
	b, e := proto.MarshalOptions{Deterministic: true}.Marshal(normalized)
	if e != nil {
		return nil, backend(e)
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
	e = historyScan(tx, prefix, false, func(k, v []byte) (bool, error) {
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
			return false, backend(tx.Delete(k))
		}
		task := new(wire.ExecutionTask)
		if e := proto.Unmarshal(v, task); e != nil {
			return false, backend(e)
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
