package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"time"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/persistence"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	native "slatedb.io/slatedb-go/uniffi"
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
	data, err := s.Owner.Run(ctx, func(_ *native.Db) ([]byte, error) {
		out, err := s.Owner.journal(q.OperationId, q.CommandSha256, clusterFamily, func(tx *native.DbTransaction) (*wire.StoredOutcome, error) {
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

const memberPrefix = "v1/cluster/member/"

func loadCluster(tx *native.DbTransaction, key string, message proto.Message) (bool, error) {
	b, err := get(tx, key)
	if err != nil || b == nil {
		return false, err
	}
	if err = proto.Unmarshal(b, message); err != nil {
		return false, backend(err)
	}
	return true, nil
}
func saveCluster(tx *native.DbTransaction, key string, message proto.Message) error {
	b, err := proto.Marshal(message)
	if err != nil {
		return backend(err)
	}
	return put(tx, key, b)
}
func scanCluster(tx *native.DbTransaction, prefix string, visit func([]byte, []byte) (bool, error)) error {
	return scanClusterRange(tx, prefix, native.KeyRange{}, visit)
}

func scanClusterRange(tx *native.DbTransaction, prefix string, bounds native.KeyRange, visit func([]byte, []byte) (bool, error)) error {
	iter, err := tx.ScanPrefix([]byte(prefix), bounds)
	if err != nil {
		return backend(err)
	}
	defer iter.Destroy()
	for {
		entry, err := iter.Next()
		if err != nil {
			return backend(err)
		}
		if entry == nil {
			return nil
		}
		stop, err := visit(entry.Key[len(prefix):], entry.Value)
		if err != nil {
			return err
		}
		if stop {
			return nil
		}
	}
}
func applyCluster(tx *native.DbTransaction, c *wire.ClusterCommand, now time.Time) (*wire.ClusterResult, error) {
	outcome, err := persistence.ApplyCluster(context.Background(), legacyClusterTransaction{legacyShardTransaction{tx}}, c, now)
	if err != nil {
		return nil, err
	}
	return outcome.GetClusterResult(), nil
}

// The legacy Owner.Run/journal still owns serialization and durability. This
// bridge only exposes its existing native transaction to shared semantics.
type legacyClusterTransaction struct{ legacyShardTransaction }

func (t legacyClusterTransaction) Delete(key []byte) error { return backend(t.tx.Delete(key)) }
func (t legacyClusterTransaction) Scan(_ context.Context, r partitions.ScanRequest) (partitions.ReadResult, error) {
	bounds := native.KeyRange{StartInclusive: !r.StartExclusive, EndInclusive: r.EndInclusive}
	if r.Start != nil {
		bounds.Start = &r.Start
	}
	if r.End != nil {
		bounds.End = &r.End
	}
	order := native.IterationOrderAscending
	if r.Reverse {
		order = native.IterationOrderDescending
	}
	durability := native.DurabilityLevelMemory
	if r.RemoteDurable {
		durability = native.DurabilityLevelRemote
	}
	iter, err := t.tx.ScanWithOptions(bounds, native.ScanOptions{DurabilityFilter: durability, ReadAheadBytes: 1, MaxFetchTasks: 1, Order: &order})
	if err != nil {
		return partitions.ReadResult{}, backend(err)
	}
	defer iter.Destroy()
	result := partitions.ReadResult{}
	for {
		row, err := iter.Next()
		if err != nil {
			return partitions.ReadResult{}, backend(err)
		}
		if row == nil {
			return result, nil
		}
		if len(result.Entries) == r.Limit {
			result.More = true
			return result, nil
		}
		result.Entries = append(result.Entries, partitions.Entry{Key: bytes.Clone(row.Key), Value: bytes.Clone(row.Value)})
	}
}
