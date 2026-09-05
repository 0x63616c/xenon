package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	native "slatedb.io/slatedb-go/uniffi"
	"strings"
)

type HistoryTasksServer struct {
	wire.UnimplementedHistoryTasksPersistenceServer
	Owner *Owner
}

func (s *HistoryTasksServer) Execute(ctx context.Context, q *wire.HistoryTasksRequest) (*wire.HistoryTasksResult, error) {
	if q == nil || q.ProtocolVersion != 1 || q.Partition != s.Owner.config.Partition || !operationID.MatchString(q.OperationId) || q.Command == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid history-task envelope")
	}
	q = proto.Clone(q).(*wire.HistoryTasksRequest)
	c := q.Command
	if c.Kind < wire.HistoryTasksCommand_READ || c.Kind > wire.HistoryTasksCommand_RANGE_COMPLETE || c.ShardId < 0 || c.Minimum == nil || c.Maximum == nil || len(c.NextPageToken) > 2048 {
		return nil, status.Error(codes.InvalidArgument, "invalid history-task command")
	}
	if c.Kind == wire.HistoryTasksCommand_READ && (c.PageSize < 1 || c.PageSize > 1000) {
		return nil, status.Error(codes.InvalidArgument, "history-task page size must be 1..1000")
	}
	for _, t := range []*wire.ExecutionTask{c.Minimum, c.Maximum} {
		if t.FireNanos < 0 || t.FireNanos >= 1000000000 {
			return nil, status.Error(codes.InvalidArgument, "invalid task timestamp")
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
		out, e := s.Owner.journal(q.OperationId, d[:], historyTasksFamily, func(tx *native.DbTransaction) (*wire.StoredOutcome, error) {
			r, e := applyHistoryTasks(tx, c)
			if e != nil {
				return nil, e
			}
			return &wire.StoredOutcome{Result: &wire.StoredOutcome_HistoryTasksResult{HistoryTasksResult: r}}, nil
		})
		if e != nil {
			return nil, e
		}
		return proto.Marshal(out.GetHistoryTasksResult())
	})
	if e != nil {
		return nil, e
	}
	r := new(wire.HistoryTasksResult)
	if e = proto.Unmarshal(raw, r); e != nil {
		return nil, backend(e)
	}
	return r, nil
}

type historyTasksCursor struct {
	Version int
	Binding []byte
	Last    string
}

func taskBound(c *wire.HistoryTasksCommand, t *wire.ExecutionTask) string {
	v := proto.Clone(t).(*wire.ExecutionTask)
	v.CategoryId = c.CategoryId
	v.CategoryType = c.CategoryType
	k := executionTaskKey(c.ShardId, v)
	if c.CategoryType == 2 {
		k = k[:strings.LastIndex(k, "/")+1]
	}
	return k
}
func applyHistoryTasks(tx *native.DbTransaction, c *wire.HistoryTasksCommand) (*wire.HistoryTasksResult, error) {
	r := new(wire.HistoryTasksResult)
	// Temporal explicitly permits ignoring BestEffort completion; pinned SQL does so.
	if c.Kind == wire.HistoryTasksCommand_COMPLETE && c.BestEffort {
		return r, nil
	}
	if c.CategoryType != 1 && c.CategoryType != 2 {
		return &wire.HistoryTasksResult{Error: wire.HistoryTasksResult_INTERNAL, Message: "unknown history task category type"}, nil
	}
	prefix := fmt.Sprintf("v1/history-task/%d/%010d/%010d/", c.CategoryType, c.ShardId, c.CategoryId)
	low, high := taskBound(c, c.Minimum), taskBound(c, c.Maximum)
	if c.Kind == wire.HistoryTasksCommand_COMPLETE {
		t := proto.Clone(c.Minimum).(*wire.ExecutionTask)
		t.CategoryId = c.CategoryId
		t.CategoryType = c.CategoryType
		if e := tx.Delete([]byte(executionTaskKey(c.ShardId, t))); e != nil {
			return nil, backend(e)
		}
		return r, nil
	}
	normalized := proto.Clone(c).(*wire.HistoryTasksCommand)
	normalized.NextPageToken = nil
	normalized.PageSize = 0
	boundBytes, e := proto.MarshalOptions{Deterministic: true}.Marshal(normalized)
	if e != nil {
		return nil, backend(e)
	}
	binding := sha256.Sum256(boundBytes)
	after := ""
	if len(c.NextPageToken) > 0 {
		var token historyTasksCursor
		if json.Unmarshal(c.NextPageToken, &token) != nil || token.Version != 1 || !bytes.Equal(token.Binding, binding[:]) || !strings.HasPrefix(token.Last, prefix) || token.Last < low || token.Last >= high {
			return &wire.HistoryTasksResult{Error: wire.HistoryTasksResult_INTERNAL, Message: "invalid history-task page token"}, nil
		}
		after = token.Last
	}
	last := ""
	e = historyScan(tx, prefix, false, func(k, v []byte) (bool, error) {
		key := string(k)
		if key < low || key <= after {
			return false, nil
		}
		if key >= high {
			return true, nil
		}
		if c.Kind == wire.HistoryTasksCommand_RANGE_COMPLETE {
			if e := tx.Delete(k); e != nil {
				return false, backend(e)
			}
			return false, nil
		}
		task := new(wire.ExecutionTask)
		if e := proto.Unmarshal(v, task); e != nil {
			return false, backend(e)
		}
		if task.CategoryId != c.CategoryId || task.CategoryType != c.CategoryType || executionTaskKey(c.ShardId, task) != key {
			return false, status.Error(codes.Unavailable, "corrupt history task")
		}
		r.Tasks = append(r.Tasks, task)
		token, _ := json.Marshal(historyTasksCursor{1, binding[:], key})
		r.NextPageToken = token
		if proto.Size(r) > 3*1024*1024 {
			r.Tasks = r.Tasks[:len(r.Tasks)-1]
			if last == "" {
				return false, status.Error(codes.ResourceExhausted, "history task exceeds response budget")
			}
			r.NextPageToken, _ = json.Marshal(historyTasksCursor{1, binding[:], last})
			return true, nil
		}
		last = key
		return len(r.Tasks) >= int(c.PageSize), nil
	})
	if e != nil {
		return nil, e
	}
	return r, nil
}
