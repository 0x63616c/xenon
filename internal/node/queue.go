package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/persistence"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	native "slatedb.io/slatedb-go/uniffi"
)

type QueueServer struct {
	wire.UnimplementedQueuePersistenceServer
	Owner *Owner
}

func (s *QueueServer) Execute(ctx context.Context, request *wire.QueueRequest) (*wire.QueueResult, error) {
	if request == nil || request.ProtocolVersion != 1 || request.Partition != s.Owner.config.Partition || !operationID.MatchString(request.OperationId) || request.Command == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid queue envelope")
	}
	request = proto.Clone(request).(*wire.QueueRequest)
	c := request.Command
	if err := persistence.ValidateQueueCommand(c); err != nil {
		return nil, err
	}
	raw, e := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if e != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid command")
	}
	digest := sha256.Sum256(raw)
	if !bytes.Equal(digest[:], request.CommandSha256) {
		return nil, status.Error(codes.InvalidArgument, "command digest mismatch")
	}
	encoded, e := s.Owner.Run(ctx, func(*native.Db) ([]byte, error) {
		outcome, e := s.Owner.journal(request.OperationId, digest[:], queueFamily, func(tx *native.DbTransaction) (*wire.StoredOutcome, error) {
			r, e := applyQueue(tx, c)
			if e != nil {
				return nil, e
			}
			return &wire.StoredOutcome{Result: &wire.StoredOutcome_QueueResult{QueueResult: r}}, nil
		})
		if e != nil {
			return nil, e
		}
		return proto.Marshal(outcome.GetQueueResult())
	})
	if e != nil {
		return nil, e
	}
	r := new(wire.QueueResult)
	if e = proto.Unmarshal(encoded, r); e != nil {
		return nil, backend(e)
	}
	return r, nil
}
func queuePrefix(t int32) string { return fmt.Sprintf("v1/queue/%d/", t) }
func queueEntryKey(t int32, id int64) string {
	return fmt.Sprintf("%sm/%016x", queuePrefix(t), uint64(id))
}
func queueMetadata(tx *native.DbTransaction, t int32) (*wire.QueueMetadataRecord, error) {
	raw, e := get(tx, queuePrefix(t)+"meta")
	if e != nil || raw == nil {
		return nil, e
	}
	r := new(wire.QueueMetadataRecord)
	if e = proto.Unmarshal(raw, r); e != nil {
		return nil, backend(e)
	}
	return r, nil
}
func applyQueue(tx *native.DbTransaction, c *wire.QueueCommand) (*wire.QueueResult, error) {
	out, err := persistence.ApplyQueue(context.Background(), legacyClusterTransaction{legacyShardTransaction{tx}}, c)
	if err != nil {
		return nil, err
	}
	return out.GetQueueResult(), nil
}
