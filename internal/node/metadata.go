package node

import (
	"bytes"
	"context"
	"crypto/sha256"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/persistence"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type MetadataServer struct {
	wire.UnimplementedMetadataPersistenceServer
	Owner *Owner
}

func (s *MetadataServer) Execute(ctx context.Context, request *wire.MetadataRequest) (*wire.MetadataResult, error) {
	if request == nil || request.ProtocolVersion != 1 || request.Partition != s.Owner.config.Partition || !operationID.MatchString(request.OperationId) || request.Command == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid protocol, partition or operation identity")
	}
	request = proto.Clone(request).(*wire.MetadataRequest)
	c := request.Command
	if err := persistence.ValidateMetadataCommand(c); err != nil {
		return nil, err
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid command encoding")
	}
	digest := sha256.Sum256(encoded)
	if !bytes.Equal(digest[:], request.CommandSha256) {
		return nil, status.Error(codes.InvalidArgument, "command digest mismatch")
	}
	resultBytes, err := s.Owner.Run(ctx, func(_ partitions.Writer) ([]byte, error) {
		outcome, err := s.Owner.journal(request.OperationId, request.CommandSha256, metadataFamily, func(tx partitions.Transaction) (*wire.StoredOutcome, error) {
			result, err := applyNamespace(tx, c)
			if err != nil {
				return nil, err
			}
			return &wire.StoredOutcome{Result: &wire.StoredOutcome_MetadataResult{MetadataResult: result}}, nil
		})
		if err != nil {
			return nil, err
		}
		return proto.Marshal(outcome.GetMetadataResult())
	})
	if err != nil {
		return nil, err
	}
	result := &wire.MetadataResult{}
	if err = proto.Unmarshal(resultBytes, result); err != nil {
		return nil, backend(err)
	}
	return result, nil
}

// applyNamespace retains the legacy Owner.Run/journal execution boundary while
// sharing conditional/index semantics with the new partition persistence service.
func applyNamespace(tx partitions.Transaction, c *wire.MetadataCommand) (*wire.MetadataResult, error) {
	outcome, err := persistence.ApplyNamespace(context.Background(), tx, c)
	if err != nil {
		return nil, err
	}
	return outcome.GetMetadataResult(), nil
}
