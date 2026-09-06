package node

import (
	"bytes"
	"context"
	"crypto/sha256"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/persistence"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	native "slatedb.io/slatedb-go/uniffi"
)

type NexusServer struct {
	wire.UnimplementedNexusPersistenceServer
	Owner *Owner
}

func (s *NexusServer) Execute(ctx context.Context, q *wire.NexusRequest) (*wire.NexusResult, error) {
	if q == nil || q.ProtocolVersion != 1 || q.Partition != s.Owner.config.Partition || !operationID.MatchString(q.OperationId) || q.Command == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid nexus envelope")
	}
	q = proto.Clone(q).(*wire.NexusRequest)
	c := q.Command
	if err := persistence.ValidateNexusCommand(c); err != nil {
		return nil, err
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(raw)
	if !bytes.Equal(digest[:], q.CommandSha256) {
		return nil, status.Error(codes.InvalidArgument, "nexus digest mismatch")
	}
	data, err := s.Owner.Run(ctx, func(*native.Db) ([]byte, error) {
		out, err := s.Owner.journal(q.OperationId, q.CommandSha256, nexusFamily, func(tx *native.DbTransaction) (*wire.StoredOutcome, error) {
			r, err := applyNexus(tx, c)
			if err != nil {
				return nil, err
			}
			return &wire.StoredOutcome{Result: &wire.StoredOutcome_NexusResult{NexusResult: r}}, nil
		})
		if err != nil {
			return nil, err
		}
		return proto.Marshal(out.GetNexusResult())
	})
	if err != nil {
		return nil, err
	}
	r := new(wire.NexusResult)
	if err = proto.Unmarshal(data, r); err != nil {
		return nil, backend(err)
	}
	return r, nil
}

// applyNexus delegates only semantics; Owner.Run and its journal retain the
// existing native transaction, serialization, replay and durability boundary.
func applyNexus(tx *native.DbTransaction, c *wire.NexusCommand) (*wire.NexusResult, error) {
	outcome, err := persistence.ApplyNexus(context.Background(), legacyClusterTransaction{legacyShardTransaction{tx}}, c)
	if err != nil {
		return nil, err
	}
	return outcome.GetNexusResult(), nil
}

const nexusPrefix = "v1/nexus/endpoint/"
const nexusVersion = "v1/nexus/version"
