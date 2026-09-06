package persistence

import (
	"bytes"
	"context"
	"crypto/sha256"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/identity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// MatchingService exposes another family on the SAME borrowed writer, authority
// callbacks and replay namespace as Service. Begin retains the writer gate over
// the complete operation and durability wait; this wrapper never opens a writer.
type MatchingService struct {
	wire.UnimplementedMatchingPersistenceServer
	service *Service
}

var _ wire.MatchingPersistenceServer = (*MatchingService)(nil)

func NewMatchingService(service *Service) (*MatchingService, error) {
	if service == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid matching service configuration")
	}
	return &MatchingService{service: service}, nil
}

func (s *MatchingService) Execute(ctx context.Context, request *wire.MatchingRequest) (result *wire.MatchingResult, err error) {
	if request == nil || request.ProtocolVersion != MatchingProtocol(request.Command) || request.Partition != string(s.service.partition) || identity.ValidateOperationReference(request.OperationId) != nil || request.Command == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid matching envelope")
	}
	request = proto.Clone(request).(*wire.MatchingRequest)
	if err := ValidateMatchingCommand(request.Command); err != nil {
		return nil, err
	}
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
			s.service.failure(err)
		}
	}()
	tx, err := s.service.writer.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Abort() // after dispatch this cannot roll back or free an active call
	// Begin retains the per-writer gate through commit/durability. Check current
	// reservation under that gate, before reads or staging a persistence mutation.
	if err = s.service.authority(ctx); err != nil {
		return nil, err
	}
	stored, err := RunReplay(ReplayEffects{
		Get:     func(key string) ([]byte, error) { return tx.Get(ctx, []byte(key)) },
		Put:     func(key string, value []byte) error { return tx.Put([]byte(key), value) },
		Apply:   func() (*wire.StoredOutcome, error) { return ApplyMatching(ctx, tx, request.Command) },
		Account: func(outcome *wire.StoredOutcome, size int) error { return AccountOutcome(ctx, tx, outcome, size) },
		Belongs: func(outcome *wire.StoredOutcome) bool { return outcome.GetMatchingResult() != nil },
		Commit: func(*wire.StoredOutcome) error {
			receipt, e := tx.Commit(ctx)
			if e != nil {
				return e
			}
			return s.service.writer.AwaitDurable(ctx, receipt)
		},
	}, request.OperationId, request.CommandSha256, s.service.limit)
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return stored.GetMatchingResult(), nil
}
