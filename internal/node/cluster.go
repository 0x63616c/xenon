package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"time"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/persistence"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type ClusterServer struct {
	wire.UnimplementedClusterPersistenceServer
	Owner *Owner
	Now   func() time.Time
}

func clusterStamp(t time.Time) *wire.ClusterTime {
	return &wire.ClusterTime{Seconds: t.Unix(), Nanos: int32(t.Nanosecond())}
}
func instant(t *wire.ClusterTime) time.Time { return time.Unix(t.Seconds, int64(t.Nanos)).UTC() }
func (s *ClusterServer) Execute(ctx context.Context, q *wire.ClusterRequest) (*wire.ClusterResult, error) {
	if q == nil || q.ProtocolVersion != 1 || q.Partition != s.Owner.config.Partition || !operationID.MatchString(q.OperationId) || q.Command == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid cluster envelope")
	}
	q = proto.Clone(q).(*wire.ClusterRequest)
	c := q.Command
	if err := persistence.ValidateClusterCommand(c); err != nil {
		return nil, err
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid cluster encoding")
	}
	digest := sha256.Sum256(raw)
	if !bytes.Equal(digest[:], q.CommandSha256) {
		return nil, status.Error(codes.InvalidArgument, "cluster digest mismatch")
	}
	data, err := s.Owner.Run(ctx, func(_ partitions.Writer) ([]byte, error) {
		out, err := s.Owner.journal(q.OperationId, q.CommandSha256, clusterFamily, func(tx partitions.Transaction) (*wire.StoredOutcome, error) {
			now := time.Now().UTC()
			if s.Now != nil {
				now = s.Now().UTC()
			}
			r, err := applyCluster(tx, c, now)
			if err != nil {
				return nil, err
			}
			if proto.Size(r) > 3*1024*1024 {
				return nil, status.Error(codes.ResourceExhausted, "cluster response exceeds byte budget")
			}
			return &wire.StoredOutcome{Result: &wire.StoredOutcome_ClusterResult{ClusterResult: r}}, nil
		})
		if err != nil {
			return nil, err
		}
		return proto.Marshal(out.GetClusterResult())
	})
	if err != nil {
		return nil, err
	}
	r := new(wire.ClusterResult)
	if err = proto.Unmarshal(data, r); err != nil {
		return nil, backend(err)
	}
	return r, nil
}

func applyCluster(tx partitions.Transaction, c *wire.ClusterCommand, now time.Time) (*wire.ClusterResult, error) {
	outcome, err := persistence.ApplyCluster(context.Background(), tx, c, now)
	if err != nil {
		return nil, err
	}
	return outcome.GetClusterResult(), nil
}
