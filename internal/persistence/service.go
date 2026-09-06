package persistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/replay"
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
	if request == nil || request.ProtocolVersion != 1 || request.Partition != string(s.partition) || identity.OperationID(request.OperationId).Validate() != nil || request.Command == nil || len(request.Command.Data) > 1024*1024 {
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
	stored, err := replay.Run(replay.Effects{
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

type familyUsage struct {
	Entries             uint64 `json:"entries"`
	EncodedOutcomeBytes uint64 `json:"encoded_outcome_bytes"`
}

// AccountOutcome preserves legacy bounded aggregate accounting in the SAME
// transaction as operation effects, outcome and count. Never expire replay here.
func AccountOutcome(ctx context.Context, tx ShardTransaction, outcome *wire.StoredOutcome, size int) error {
	raw, err := tx.Get(ctx, []byte("v1/outcome_usage"))
	if err != nil {
		return err
	}
	usage := map[string]familyUsage{}
	if raw != nil {
		if len(raw) > 65536 {
			return status.Error(codes.Unavailable, "outcome accounting too large")
		}
		if err = json.Unmarshal(raw, &usage); err != nil || usage == nil {
			return status.Error(codes.Unavailable, "corrupt outcome accounting")
		}
	}
	if outcome == nil || size < 0 {
		return status.Error(codes.InvalidArgument, "invalid outcome accounting input")
	}
	ref := outcome.ProtoReflect()
	field := ref.WhichOneof(ref.Descriptor().Oneofs().ByName("result"))
	if field == nil {
		return status.Error(codes.Unavailable, "outcome has no result family")
	}
	name := string(field.Name())
	value := usage[name]
	if value.Entries == math.MaxUint64 || math.MaxUint64-value.EncodedOutcomeBytes < uint64(size) {
		return status.Error(codes.ResourceExhausted, "outcome accounting overflow")
	}
	value.Entries++
	value.EncodedOutcomeBytes += uint64(size)
	usage[name] = value
	raw, err = json.Marshal(usage)
	if err != nil {
		return err
	}
	return tx.Put([]byte("v1/outcome_usage"), raw)
}
