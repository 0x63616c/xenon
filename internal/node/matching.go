package node

import (
	"bytes"
	"context"
	"crypto/sha256"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/persistence"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	native "slatedb.io/slatedb-go/uniffi"
)

// Matching shares the owner partition, including all subqueues. No per-subqueue routing.
type MatchingServer struct {
	wire.UnimplementedMatchingPersistenceServer
	Owner *Owner
}

func (s *MatchingServer) Execute(ctx context.Context, req *wire.MatchingRequest) (*wire.MatchingResult, error) {
	if req == nil || req.ProtocolVersion != matchingProtocol(req.Command) || req.Partition != s.Owner.config.Partition || !operationID.MatchString(req.OperationId) || req.Command == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid matching request")
	}
	req = proto.Clone(req).(*wire.MatchingRequest)
	c := req.Command
	if err := persistence.ValidateMatchingCommand(c); err != nil {
		return nil, err
	}
	encoded, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	digest := sha256.Sum256(encoded)
	if !bytes.Equal(req.CommandSha256, digest[:]) {
		return nil, status.Error(codes.InvalidArgument, "matching digest mismatch")
	}
	outcome, err := s.Owner.runJournalResult(ctx, req.OperationId, req.CommandSha256, matchingFamily, func(tx *native.DbTransaction) (*wire.StoredOutcome, error) {
		r, e := applyMatching(tx, c)
		if e != nil {
			return nil, e
		}
		return &wire.StoredOutcome{Result: &wire.StoredOutcome_MatchingResult{MatchingResult: r}}, nil
	})
	if err != nil {
		return nil, err
	}
	r := outcome.GetMatchingResult()

	return r, nil
}

func applyMatching(tx *native.DbTransaction, c *wire.MatchingCommand) (*wire.MatchingResult, error) {
	outcome, err := persistence.ApplyMatching(context.Background(), legacyClusterTransaction{legacyShardTransaction{tx}}, c)
	if err != nil {
		return nil, err
	}
	return outcome.GetMatchingResult(), nil
}
func matchingProtocol(c *wire.MatchingCommand) uint32 { return persistence.MatchingProtocol(c) }
