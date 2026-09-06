package persistence

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/partitions"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"math"
	"strings"
)

func ValidateMatchingCommand(c *wire.MatchingCommand) error {
	if c == nil {
		return status.Error(codes.InvalidArgument, "invalid matching command")
	}
	if c.Kind < 1 || c.Kind > 13 || (c.Kind != wire.MatchingCommand_LIST_QUEUES && len(c.NamespaceId) != 16) || ((c.Kind <= wire.MatchingCommand_COMPLETE_TASKS || c.Kind == wire.MatchingCommand_GET_USER_DATA) && c.Kind != wire.MatchingCommand_LIST_QUEUES && (c.Queue == "" || len(c.Queue) > 1024)) || len(c.Data) > 1024*1024 || proto.Size(c) > 1800*1024 || c.Subqueue < 0 {
		return status.Error(codes.InvalidArgument, "invalid matching command")
	}
	if (c.Kind == wire.MatchingCommand_GET_TASKS || c.Kind == wire.MatchingCommand_LIST_QUEUES || c.Kind == wire.MatchingCommand_LIST_USER_DATA) && (c.PageSize < 1 || c.PageSize > 1000) {
		return status.Error(codes.InvalidArgument, "invalid matching limit")
	}
	if c.Kind == wire.MatchingCommand_COMPLETE_TASKS && c.PageSize < 1 {
		return status.Error(codes.InvalidArgument, "invalid completion limit")
	}
	for _, task := range c.Tasks {
		if task == nil || task.Subqueue < 0 || len(task.Data) > 1024*1024 {
			return status.Error(codes.InvalidArgument, "invalid matching task")
		}
	}

	if len(c.BuildId) > 1024 || len(c.Updates) > 1000 {
		return status.Error(codes.InvalidArgument, "invalid user-data batch")
	}
	seen := map[string]bool{}
	for _, u := range c.Updates {
		if u == nil || u.Queue == "" || len(u.Queue) > 1024 || seen[u.Queue] || u.Version < 0 || u.Version == math.MaxInt64 || len(u.Data) > 1024*1024 {
			return status.Error(codes.InvalidArgument, "invalid user-data update")
		}
		seen[u.Queue] = true
		for _, id := range append(append([]string(nil), u.BuildIdsAdded...), u.BuildIdsRemoved...) {
			if len(id) > 1024 {
				return status.Error(codes.InvalidArgument, "invalid build ID")
			}
		}
	}
	return nil
}

// ApplyMatching stages the existing task queue, fair scheduling and worker-version
// data semantics. Its caller retains admission and atomically journals the result.
func ApplyMatching(ctx context.Context, tx ClusterTransaction, c *wire.MatchingCommand) (*wire.StoredOutcome, error) {
	if tx == nil {
		return nil, status.Error(codes.InvalidArgument, "nil matching transaction")
	}
	if err := ValidateMatchingCommand(c); err != nil {
		return nil, err
	}
	result, err := applyMatching(ctx, tx, c)
	if err != nil {
		return nil, err
	}
	return &wire.StoredOutcome{Result: &wire.StoredOutcome_MatchingResult{MatchingResult: result}}, nil
}

const matchingQueuePrefix = "v1/matching/queue/"

func matchingQueueDomainPrefix(c *wire.MatchingCommand) string {
	if c.Fair {
		return "v1/matching/fair/queue/"
	}
	return matchingQueuePrefix
}

func matchingKey(c *wire.MatchingCommand) string {
	return hex.EncodeToString(c.NamespaceId) + fmt.Sprintf("/%08x/", uint32(c.TaskType)) + hex.EncodeToString([]byte(c.Queue))
}
func matchingTaskPrefix(c *wire.MatchingCommand, sub int32) string {
	prefix := "v1/matching/task/"
	if c.Fair {
		prefix = "v1/matching/fair/task/"
	}
	return prefix + matchingKey(c) + fmt.Sprintf("/%08x/", uint32(sub))
}
func matchingTaskKey(c *wire.MatchingCommand, t *wire.MatchingTask) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(t.Id)^(1<<63))
	if c.Fair {
		return matchingTaskPrefix(c, t.Subqueue) + string(fairLevel(t.Pass, t.Id))
	}
	return matchingTaskPrefix(c, t.Subqueue) + string(b[:])
}
func matchingLogical(code wire.MatchingResult_Error, msg string) *wire.MatchingResult {
	return &wire.MatchingResult{Error: code, Message: msg}
}
func applyMatching(ctx context.Context, tx ClusterTransaction, c *wire.MatchingCommand) (*wire.MatchingResult, error) {
	if c.Kind >= wire.MatchingCommand_GET_USER_DATA {
		return applyMatchingUserData(ctx, tx, c)
	}
	r := new(wire.MatchingResult)
	if c.Fair && (c.Kind == wire.MatchingCommand_GET_TASKS || c.Kind == wire.MatchingCommand_COMPLETE_TASKS) {
		return applyFairTasks(ctx, tx, c)
	}
	key := matchingQueueDomainPrefix(c) + matchingKey(c)
	switch c.Kind {
	case wire.MatchingCommand_CREATE_QUEUE, wire.MatchingCommand_GET_QUEUE, wire.MatchingCommand_UPDATE_QUEUE, wire.MatchingCommand_DELETE_QUEUE, wire.MatchingCommand_CREATE_TASKS:
		record := new(wire.MatchingRecord)
		exists, err := loadCluster(ctx, tx, key, record)
		if err != nil {
			return nil, err
		}
		if c.Kind == wire.MatchingCommand_CREATE_QUEUE {
			if exists {
				return matchingLogical(wire.MatchingResult_CONDITION_FAILED, "task queue already exists"), nil
			}
		} else if c.Kind == wire.MatchingCommand_GET_QUEUE {
			if !exists {
				return matchingLogical(wire.MatchingResult_NOT_FOUND, "task queue not found"), nil
			}
			r.Queues = []*wire.MatchingRecord{record}
			return r, nil
		} else {
			expected := c.RangeId
			if c.Kind == wire.MatchingCommand_UPDATE_QUEUE {
				expected = c.PreviousRangeId
			}
			if c.Kind != wire.MatchingCommand_CREATE_TASKS && (!exists || record.RangeId != expected) {
				return matchingLogical(wire.MatchingResult_CONDITION_FAILED, "task queue range mismatch or missing"), nil
			}
		}
		switch c.Kind {
		case wire.MatchingCommand_CREATE_QUEUE, wire.MatchingCommand_UPDATE_QUEUE:
			return r, saveCluster(tx, key, &wire.MatchingRecord{RangeId: c.RangeId, Data: c.Data, Encoding: c.Encoding})
		case wire.MatchingCommand_DELETE_QUEUE:
			return r, tx.Delete([]byte(key))
		case wire.MatchingCommand_CREATE_TASKS:
			// Check the whole batch before writes: a logical error is itself journaled.
			seen := map[string]bool{}
			for _, task := range c.Tasks {
				key := matchingTaskKey(c, task)
				old, e := tx.Get(ctx, []byte(key))
				if e != nil {
					return nil, e
				}
				if seen[key] || old != nil {
					return matchingLogical(wire.MatchingResult_UNAVAILABLE, "duplicate task ID"), nil
				}
				seen[key] = true
			}
			if !exists || record.RangeId != c.RangeId {
				return matchingLogical(wire.MatchingResult_CONDITION_FAILED, "task queue range mismatch or missing"), nil
			}
			for _, task := range c.Tasks {
				if e := saveCluster(tx, matchingTaskKey(c, task), task); e != nil {
					return nil, e
				}
			}
			return r, nil
		}
	case wire.MatchingCommand_LIST_QUEUES:
		matchingQueuePrefix := matchingQueueDomainPrefix(c)
		if len(c.Token) > 2200 || (len(c.Token) > 0 && !strings.HasPrefix(string(c.Token), matchingQueuePrefix)) {
			return nil, status.Error(codes.InvalidArgument, "invalid queue token")
		}
		truncated := false
		err := scanCluster(ctx, tx, matchingQueuePrefix, func(suffix, value []byte) (bool, error) {
			key := matchingQueuePrefix + string(suffix)
			if bytes.Compare([]byte(key), c.Token) <= 0 {
				return false, nil
			}
			item := new(wire.MatchingRecord)
			if e := proto.Unmarshal(value, item); e != nil {
				return false, clusterEncodingError(e)
			}
			if len(r.Queues) >= int(c.PageSize) {
				truncated = true
				return true, nil
			}
			previous := r.Token
			r.Queues = append(r.Queues, item)
			r.Token = []byte(key)
			if proto.Size(r) > 3*1024*1024 {
				r.Queues = r.Queues[:len(r.Queues)-1]
				truncated = true
				r.Token = previous
				return true, nil
			}
			return false, nil
		})
		if !truncated {
			r.Token = nil
		}
		return r, err
	case wire.MatchingCommand_GET_TASKS, wire.MatchingCommand_COMPLETE_TASKS:
		prefix := matchingTaskPrefix(c, c.Subqueue)
		var after []byte
		if len(c.Token) > 0 {
			if len(c.Token) != 8 {
				return nil, status.Error(codes.InvalidArgument, "invalid task token")
			}
			after = c.Token
		}
		// Seek to the requested suffix in the native iterator. Scanning from the
		// prefix for every one-row page makes the pinned 1024-row suite quadratic.
		end := make([]byte, 8)
		binary.BigEndian.PutUint64(end, uint64(c.MaxId)^(1<<63))
		bounds := partitions.ScanRequest{End: end}
		if c.Kind == wire.MatchingCommand_GET_TASKS {
			start := make([]byte, 8)
			binary.BigEndian.PutUint64(start, uint64(c.MinId)^(1<<63))
			bounds.Start = start
			bounds.StartExclusive = false
			if after != nil && bytes.Compare(after, start) >= 0 {
				bounds.Start = after
				bounds.StartExclusive = true
			}
			if bytes.Compare(bounds.Start, end) >= 0 {
				return r, nil
			}
		}
		truncated := false
		var remove [][]byte
		err := scanMatchingRange(ctx, tx, prefix, bounds, func(suffix, value []byte) (bool, error) {
			if len(suffix) != 8 {
				return false, status.Error(codes.Internal, "invalid stored task key")
			}
			id := int64(binary.BigEndian.Uint64(suffix) ^ (1 << 63))
			if id >= c.MaxId {
				return true, nil
			}
			if c.Kind == wire.MatchingCommand_GET_TASKS && (id < c.MinId || (after != nil && bytes.Compare(suffix, after) <= 0)) {
				return false, nil
			}
			if c.Kind == wire.MatchingCommand_COMPLETE_TASKS {
				if len(remove) >= int(c.PageSize) {
					return true, nil
				}
				remove = append(remove, append([]byte(prefix), suffix...))
				return false, nil
			}
			if len(r.Tasks) >= int(c.PageSize) {
				truncated = true
				return true, nil
			}
			task := new(wire.MatchingTask)
			if e := proto.Unmarshal(value, task); e != nil {
				return false, clusterEncodingError(e)
			}
			previous := r.Token
			r.Tasks = append(r.Tasks, task)
			r.Token = append([]byte(nil), suffix...)
			if proto.Size(r) > 3*1024*1024 {
				r.Tasks = r.Tasks[:len(r.Tasks)-1]
				truncated = true
				r.Token = previous
				return true, nil
			}
			return false, nil
		})
		if err != nil {
			return nil, err
		}
		for _, key := range remove {
			if err = tx.Delete(key); err != nil {
				return nil, err
			}
		}
		if !truncated {
			r.Token = nil
		}
		r.Completed = int32(len(remove))
		return r, nil
	}
	return nil, status.Error(codes.InvalidArgument, "unknown matching operation")
}

// Fair commands require version 2 so older nodes cannot silently use legacy keys.
func MatchingProtocol(c *wire.MatchingCommand) uint32 {
	if c != nil && c.Fair {
		return 2
	}
	return 1
}
