package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
)

func validateHistoryPartitions(partitions []string) ([]string, error) {
	if len(partitions) == 0 || len(partitions) > 1024 {
		return nil, serviceerror.NewInvalidArgument("historyPartitions must contain 1..1024 names")
	}
	seen := map[string]bool{}
	for _, name := range partitions {
		if name == "" || len(name) > 128 || seen[name] {
			return nil, serviceerror.NewInvalidArgument("historyPartitions names must be nonempty, unique and at most 128 bytes")
		}
		seen[name] = true
	}
	return append([]string(nil), partitions...), nil
}
func historyPartition(partitions []string, fallback string, shard int32) (string, error) {
	if shard < 0 {
		return "", serviceerror.NewInvalidArgument("negative history shard")
	}
	if partitions == nil {
		return fallback, nil
	}
	return partitions[int(shard)%len(partitions)], nil
}
func NewPartitionedShardStore(address string, partitions []string, cluster string) (*ShardStore, error) {
	names, e := validateHistoryPartitions(partitions)
	if e != nil {
		return nil, e
	}
	s, e := NewShardStore(address, names[0], cluster)
	if e != nil {
		return nil, e
	}
	s.historyPartitions = names
	return s, nil
}
func NewPartitionedExecutionStore(address string, partitions []string) (*ExecutionStore, error) {
	names, e := validateHistoryPartitions(partitions)
	if e != nil {
		return nil, e
	}
	s, e := NewExecutionStore(address, names[0])
	if e != nil {
		return nil, e
	}
	s.WorkflowStore.historyPartitions = names
	s.HistoryStore.historyPartitions = names
	s.HistoryTasksStore.historyPartitions = names
	s.ExecutionTasksStore.historyPartitions = names
	return s, nil
}

type historyPartitionCursor struct {
	Version   int
	ListHash  []byte
	Partition int
	Local     []byte
}

func (s *HistoryStore) listHistoryPartitions(ctx context.Context, q *p.GetAllHistoryTreeBranchesRequest) (*p.InternalGetAllHistoryTreeBranchesResponse, error) {
	if q == nil || q.PageSize < 1 || q.PageSize > 1000 || len(q.NextPageToken) > 16384 {
		return nil, serviceerror.NewInvalidArgument("invalid global history page")
	}
	ctx, cancel := context.WithTimeout(ctx, s.invocationTimeout)
	defer cancel()
	names, _ := json.Marshal(s.historyPartitions)
	hash := sha256.Sum256(names)
	cursor := historyPartitionCursor{Version: 1, ListHash: append([]byte(nil), hash[:]...)}
	if len(q.NextPageToken) > 0 {
		var supplied historyPartitionCursor
		var fields map[string]json.RawMessage
		if json.Unmarshal(q.NextPageToken, &fields) != nil || len(fields) != 4 {
			return nil, serviceerror.NewInvalidArgument("invalid history partition cursor")
		}
		for _, name := range []string{"Version", "ListHash", "Partition", "Local"} {
			if raw, ok := fields[name]; !ok || (name != "Local" && bytes.Equal(bytes.TrimSpace(raw), []byte("null"))) {
				return nil, serviceerror.NewInvalidArgument("incomplete history partition cursor")
			}
		}
		if json.Unmarshal(q.NextPageToken, &supplied) != nil || supplied.Version != 1 || !bytes.Equal(supplied.ListHash, hash[:]) || supplied.Partition < 0 || supplied.Partition >= len(s.historyPartitions) {
			return nil, serviceerror.NewInvalidArgument("invalid history partition cursor")
		}
		cursor = supplied
	}
	for cursor.Partition < len(s.historyPartitions) {
		local := *s
		local.historyPartitions = nil
		local.partition = s.historyPartitions[cursor.Partition]
		r, e := local.GetAllHistoryTreeBranches(ctx, &p.GetAllHistoryTreeBranchesRequest{PageSize: q.PageSize, NextPageToken: cursor.Local})
		if e != nil {
			return nil, e
		}
		cursor.Local = r.NextPageToken
		if len(cursor.Local) == 0 {
			cursor.Partition++
		}
		r.NextPageToken = nil
		if cursor.Partition < len(s.historyPartitions) {
			r.NextPageToken, _ = json.Marshal(cursor)
		}
		if len(r.Branches) > 0 {
			return r, nil
		}
		if e = ctx.Err(); e != nil {
			return nil, e
		}
	}
	return &p.InternalGetAllHistoryTreeBranchesResponse{}, nil
}
