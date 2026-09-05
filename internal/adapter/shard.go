// Package adapter implements complete Temporal persistence operations over Xenon RPC.
package adapter

import (
	"context"
	"crypto/sha256"
	"time"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type ShardStore struct {
	connection         *grpc.ClientConn
	client             wire.ShardPersistenceClient
	partition, cluster string
	invocationTimeout  time.Duration
}

var _ persistence.ShardStore = (*ShardStore)(nil)

// NewShardStore creates a wrapper for one configured logical partition. It does
// not claim dynamic routing or expose a factory for unimplemented stores.
func NewShardStore(address, partition, cluster string) (*ShardStore, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &ShardStore{connection: conn, client: wire.NewShardPersistenceClient(conn), partition: partition, cluster: cluster, invocationTimeout: 30 * time.Second}, nil
}
func (s *ShardStore) Close()                 { _ = s.connection.Close() }
func (s *ShardStore) GetName() string        { return "xenon" }
func (s *ShardStore) GetClusterName() string { return s.cluster }

func (s *ShardStore) invoke(ctx context.Context, command *wire.ShardCommand) (*wire.ShardResult, error) {
	ctx, cancel := context.WithTimeout(ctx, s.invocationTimeout)
	defer cancel()
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(command)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(payload)
	request := &wire.ShardRequest{ProtocolVersion: 1, Partition: s.partition, OperationId: uuid.NewString(), CommandSha256: digest[:], Command: command}
	// The identity is allocated once per invocation and retained across transport retries.
	for attempt := 0; attempt < 3; attempt++ {
		response, callErr := s.client.Execute(ctx, request)
		if callErr == nil {
			return response, logicalError(response)
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if status.Code(callErr) != codes.Unavailable || attempt == 2 {
			return nil, serviceerror.FromStatus(status.Convert(callErr))
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 20 * time.Millisecond):
		}
	}
	panic("unreachable")
}
func logicalError(result *wire.ShardResult) error {
	switch result.Error {
	case wire.ShardResult_NONE:
		return nil
	case wire.ShardResult_NOT_FOUND:
		return serviceerror.NewNotFound(result.Message)
	case wire.ShardResult_UNAVAILABLE:
		return serviceerror.NewUnavailable(result.Message)
	case wire.ShardResult_OWNERSHIP_LOST:
		return &persistence.ShardOwnershipLostError{ShardID: result.ShardId, Msg: result.Message}
	default:
		return serviceerror.NewInternal("unknown Xenon logical error")
	}
}
func (s *ShardStore) GetOrCreateShard(ctx context.Context, request *persistence.InternalGetOrCreateShardRequest) (*persistence.InternalGetOrCreateShardResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("nil shard request")
	}
	result, err := s.invoke(ctx, &wire.ShardCommand{Kind: wire.ShardCommand_GET, ShardId: request.ShardID})
	if err == nil {
		return shardResponse(result), nil
	}
	if _, missing := err.(*serviceerror.NotFound); !missing || request.CreateShardInfo == nil {
		return nil, err
	}
	rangeID, blob, err := request.CreateShardInfo()
	if err != nil {
		return nil, serviceerror.NewUnavailablef("GetOrCreateShard: failed to encode shard info for ShardID %v. Error: %v", request.ShardID, err)
	}
	if blob == nil {
		return nil, serviceerror.NewInvalidArgument("nil shard blob")
	}
	result, err = s.invoke(ctx, &wire.ShardCommand{Kind: wire.ShardCommand_CREATE_OR_GET, ShardId: request.ShardID, RangeId: rangeID, Data: blob.Data, Encoding: int32(blob.EncodingType)})
	if err != nil {
		return nil, err
	}
	return shardResponse(result), nil
}
func shardResponse(result *wire.ShardResult) *persistence.InternalGetOrCreateShardResponse {
	return &persistence.InternalGetOrCreateShardResponse{ShardInfo: &commonpb.DataBlob{Data: result.Data, EncodingType: enumspb.EncodingType(result.Encoding)}}
}
func (s *ShardStore) UpdateShard(ctx context.Context, request *persistence.InternalUpdateShardRequest) error {
	if request == nil || request.ShardInfo == nil {
		return serviceerror.NewInvalidArgument("nil shard request/blob")
	}
	_, err := s.invoke(ctx, &wire.ShardCommand{Kind: wire.ShardCommand_UPDATE, ShardId: request.ShardID, PreviousRangeId: request.PreviousRangeID, RangeId: request.RangeID, Owner: request.Owner, Data: request.ShardInfo.Data, Encoding: int32(request.ShardInfo.EncodingType)})
	return err
}
func (s *ShardStore) AssertShardOwnership(ctx context.Context, request *persistence.AssertShardOwnershipRequest) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("nil ownership request")
	}
	_, err := s.invoke(ctx, &wire.ShardCommand{Kind: wire.ShardCommand_ASSERT, ShardId: request.ShardID, RangeId: request.RangeID})
	return err
}
