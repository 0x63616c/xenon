package persistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/partitions"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Service binds a borrowed writer to its immutable reservation authority check
// and failure callback. The app captures the partition service's handle token in
// these callbacks; it must never obtain a different token after an error occurs.
// This first migrated family retains the existing shared journal key namespace.
type Service struct {
	wire.UnimplementedShardPersistenceServer
	writer    partitions.Writer
	partition identity.PartitionID
	limit     uint64
	authority func(context.Context) error
	failure   func(error)
}

func NewService(writer partitions.Writer, partition identity.PartitionID, limit uint64, authority func(context.Context) error, failure func(error)) (*Service, error) {
	if writer == nil || partition.Validate() != nil || limit == 0 || authority == nil || failure == nil {
		return nil, errors.New("invalid persistence service configuration")
	}
	return &Service{writer: writer, partition: partition, limit: limit, authority: authority, failure: failure}, nil
}

func (s *Service) Execute(ctx context.Context, request *wire.ShardRequest) (result *wire.ShardResult, err error) {
	if request == nil || request.ProtocolVersion != 1 || request.Partition != string(s.partition) || identity.ValidateOperationReference(request.OperationId) != nil || request.Command == nil || len(request.Command.Data) > 1024*1024 {
		return nil, status.Error(codes.InvalidArgument, "invalid shard request")
	}
	request = proto.Clone(request).(*wire.ShardRequest)
	encoded, e := proto.MarshalOptions{Deterministic: true}.Marshal(request.Command)
	if e != nil {
		return nil, e
	}
	digest := sha256.Sum256(encoded)
	if !bytes.Equal(digest[:], request.CommandSha256) {
		return nil, status.Error(codes.InvalidArgument, "command digest mismatch")
	}
	// Typed native failure reaches lifecycle notification before an RPC edge can
	// translate it. The callback filters terminal errors using the borrowed token.
	defer func() {
		if err != nil {
			s.failure(err)
		}
	}()
	tx, err := s.writer.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Abort() // after dispatch this cannot roll back or free an active call
	// Begin retains the per-writer gate through commit/durability. Check current
	// reservation under that gate, before reads or staging a persistence mutation.
	if err = s.authority(ctx); err != nil {
		return nil, err
	}
	stored, err := RunReplay(ReplayEffects{
		Get:     func(key string) ([]byte, error) { return tx.Get(ctx, []byte(key)) },
		Put:     func(key string, value []byte) error { return tx.Put([]byte(key), value) },
		Apply:   func() (*wire.StoredOutcome, error) { return ApplyShard(ctx, tx, request.Command) },
		Account: func(outcome *wire.StoredOutcome, size int) error { return AccountOutcome(ctx, tx, outcome, size) },
		Belongs: func(outcome *wire.StoredOutcome) bool { return outcome.GetShardResult() != nil },
		Commit: func(*wire.StoredOutcome) error {
			receipt, e := tx.Commit(ctx)
			if e != nil {
				return e
			}
			return s.writer.AwaitDurable(ctx, receipt)
		},
	}, request.OperationId, request.CommandSha256, s.limit)
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return stored.GetShardResult(), nil
}
