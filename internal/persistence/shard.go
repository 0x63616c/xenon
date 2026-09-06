// Package persistence owns atomic Temporal operations and durable replay.
package persistence

import (
	"context"
	"fmt"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ShardTransaction is the transaction seam consumed by shard semantics. Both
// the new partition engine and the temporary legacy bridge implement it.
type ShardTransaction interface {
	Get(context.Context, []byte) ([]byte, error)
	Put([]byte, []byte) error
}

// ApplyShard stages one operation in the caller's transaction. The caller owns
// replay, outcome accounting, atomic commit and AwaitDurable before any reply.
// Stored keys, protobuf encoding and Temporal RangeID behavior are unchanged.
func ApplyShard(ctx context.Context, tx ShardTransaction, command *wire.ShardCommand) (*wire.StoredOutcome, error) {
	if tx == nil || command == nil || command.ShardId < 0 || command.Kind < wire.ShardCommand_GET || command.Kind > wire.ShardCommand_ASSERT {
		return nil, status.Error(codes.InvalidArgument, "invalid shard command")
	}
	shardKey := fmt.Sprintf("v1/shard/%010d", command.ShardId)
	raw, err := tx.Get(ctx, []byte(shardKey))
	if err != nil {
		return nil, err
	}
	var shard *wire.StoredShard
	if raw != nil {
		shard = &wire.StoredShard{}
		if err = proto.Unmarshal(raw, shard); err != nil {
			return nil, status.Errorf(codes.Unavailable, "corrupt stored shard: %v", err)
		}
	}
	result := &wire.ShardResult{ShardId: command.ShardId}
	save := false
	switch command.Kind {
	case wire.ShardCommand_GET, wire.ShardCommand_CREATE_OR_GET:
		if shard == nil && command.Kind == wire.ShardCommand_CREATE_OR_GET {
			shard = &wire.StoredShard{RangeId: command.RangeId, Data: command.Data, Encoding: command.Encoding}
			save = true
		}
		if shard == nil {
			result.Error = wire.ShardResult_NOT_FOUND
			result.Message = fmt.Sprintf("shard %d not found", command.ShardId)
		} else {
			copyShard(result, shard)
		}
	case wire.ShardCommand_UPDATE, wire.ShardCommand_ASSERT:
		expected := command.RangeId
		if command.Kind == wire.ShardCommand_UPDATE {
			expected = command.PreviousRangeId
		}
		if shard == nil && command.Kind == wire.ShardCommand_UPDATE {
			result.Error = wire.ShardResult_UNAVAILABLE
			result.Message = fmt.Sprintf("Failed to lock shard %d: shard does not exist", command.ShardId)
		} else if shard == nil || shard.RangeId != expected {
			result.Error = wire.ShardResult_OWNERSHIP_LOST
			result.Message = fmt.Sprintf("shard %d range mismatch: expected %d", command.ShardId, expected)
		} else if command.Kind == wire.ShardCommand_UPDATE {
			shard = &wire.StoredShard{RangeId: command.RangeId, Data: command.Data, Encoding: command.Encoding}
			save = true
			copyShard(result, shard)
		}
	}
	if save {
		data, _ := proto.Marshal(shard)
		if err = tx.Put([]byte(shardKey), data); err != nil {
			return nil, err
		}
	}
	return &wire.StoredOutcome{Result: &wire.StoredOutcome_ShardResult{ShardResult: result}}, nil
}

func copyShard(result *wire.ShardResult, shard *wire.StoredShard) {
	result.RangeId = shard.RangeId
	result.Data = shard.Data
	result.Encoding = shard.Encoding
}
