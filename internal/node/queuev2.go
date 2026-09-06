package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/persistence"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	native "slatedb.io/slatedb-go/uniffi"
)

type QueueV2Server struct {
	wire.UnimplementedQueueV2PersistenceServer
	Owner *Owner
}

func (s *QueueV2Server) Execute(ctx context.Context, q *wire.QueueV2Request) (*wire.QueueV2Result, error) {
	if q == nil || q.ProtocolVersion != 1 || q.Partition != s.Owner.config.Partition || !operationID.MatchString(q.OperationId) || q.Command == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid QueueV2 envelope")
	}
	q = proto.Clone(q).(*wire.QueueV2Request)
	c := q.Command
	if err := persistence.ValidateQueueV2Command(c); err != nil {
		return nil, err
	}
	b, e := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if e != nil {
		return nil, e
	}
	h := sha256.Sum256(b)
	if !bytes.Equal(h[:], q.CommandSha256) {
		return nil, status.Error(codes.InvalidArgument, "QueueV2 digest mismatch")
	}
	data, e := s.Owner.Run(ctx, func(*native.Db) ([]byte, error) {
		out, e := s.Owner.journal(q.OperationId, q.CommandSha256, queuev2Family, func(tx *native.DbTransaction) (*wire.StoredOutcome, error) {
			r, e := applyQueueV2(tx, c)
			if e != nil {
				return nil, e
			}
			return &wire.StoredOutcome{Result: &wire.StoredOutcome_Queuev2Result{Queuev2Result: r}}, nil
		})
		if e != nil {
			return nil, e
		}
		return proto.Marshal(out.GetQueuev2Result())
	})
	if e != nil {
		return nil, e
	}
	r := new(wire.QueueV2Result)
	if e = proto.Unmarshal(data, r); e != nil {
		return nil, backend(e)
	}
	return r, nil
}
func qv2Type(t int64) string { return fmt.Sprintf("v1/queuev2/%d/", t) }
func qv2StateKey(t int64, n string) string {
	return qv2Type(t) + "meta/" + hex.EncodeToString([]byte(n))
}
func qv2Messages(t int64, n string) string {
	return qv2Type(t) + "messages/" + hex.EncodeToString([]byte(n)) + "/"
}
func qv2MessageKey(t int64, n string, id int64) string {
	return fmt.Sprintf("%s%016x", qv2Messages(t, n), id)
}
func applyQueueV2(tx *native.DbTransaction, c *wire.QueueV2Command) (*wire.QueueV2Result, error) {
	out, err := persistence.ApplyQueueV2(context.Background(), legacyClusterTransaction{legacyShardTransaction{tx}}, c)
	if err != nil {
		return nil, err
	}
	return out.GetQueuev2Result(), nil
}
