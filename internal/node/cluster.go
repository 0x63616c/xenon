package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"math"
	"net"
	"time"
	"unicode/utf8"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
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
func validClusterTime(t *wire.ClusterTime) bool {
	return t != nil && t.Nanos >= 0 && t.Nanos < 1e9 && t.Seconds >= -62135596800 && t.Seconds <= 253402300799
}
func instant(t *wire.ClusterTime) time.Time { return time.Unix(t.Seconds, int64(t.Nanos)).UTC() }
func (s *ClusterServer) Execute(ctx context.Context, q *wire.ClusterRequest) (*wire.ClusterResult, error) {
	if q == nil || q.ProtocolVersion != 1 || q.Partition != s.Owner.config.Partition || !operationID.MatchString(q.OperationId) || q.Command == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid cluster envelope")
	}
	q = proto.Clone(q).(*wire.ClusterRequest)
	c := q.Command
	if c.Kind < wire.ClusterCommand_LIST || c.Kind > wire.ClusterCommand_PRUNE_MEMBERS || len(c.ClusterName) > 1024 || !utf8.ValidString(c.ClusterName) {
		return nil, status.Error(codes.InvalidArgument, "invalid cluster command")
	}
	if c.Kind == wire.ClusterCommand_SAVE && (c.Blob == nil || len(c.Blob.Data) > 1024*1024) {
		return nil, status.Error(codes.InvalidArgument, "invalid cluster blob")
	}
	if c.Kind == wire.ClusterCommand_LIST && c.PageSize < 1 {
		return nil, status.Error(codes.InvalidArgument, "cluster page size must be positive")
	}
	if c.Kind == wire.ClusterCommand_GET_MEMBERS && !validClusterTime(c.SessionStartedAfter) {
		return nil, status.Error(codes.InvalidArgument, "invalid session filter")
	}
	if c.Kind == wire.ClusterCommand_UPSERT_MEMBER {
		m := c.Member
		if m == nil || len(m.HostId) != 16 || (len(m.RpcAddress) != 4 && len(m.RpcAddress) != 16) || m.RpcPort > 65535 || !validClusterTime(m.SessionStart) {
			return nil, status.Error(codes.InvalidArgument, "invalid cluster member")
		}
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
func clusterLogical(code wire.ClusterResult_Error, message string) *wire.ClusterResult {
	return &wire.ClusterResult{Error: code, Message: message}
}

const clusterPrefix = "v1/cluster/metadata/"
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
	r := new(wire.ClusterResult)
	switch c.Kind {
	case wire.ClusterCommand_GET, wire.ClusterCommand_SAVE:
		record := new(wire.ClusterRecord)
		exists, err := loadCluster(tx, clusterPrefix+c.ClusterName, record)
		if err != nil {
			return nil, err
		}
		if c.Kind == wire.ClusterCommand_GET {
			if !exists {
				return clusterLogical(wire.ClusterResult_NOT_FOUND, "cluster metadata not found"), nil
			}
			r.Record = record
			break
		}
		if record.Version != c.Version {
			return clusterLogical(wire.ClusterResult_UNAVAILABLE, "cluster metadata version mismatch"), nil
		}
		if c.Version == math.MaxInt64 {
			return clusterLogical(wire.ClusterResult_RESOURCE_EXHAUSTED, "cluster version exhausted"), nil
		}
		record = &wire.ClusterRecord{Blob: c.Blob, Version: c.Version + 1}
		if err = saveCluster(tx, clusterPrefix+c.ClusterName, record); err != nil {
			return nil, err
		}
		r.Applied = true
	case wire.ClusterCommand_DELETE:
		if err := tx.Delete([]byte(clusterPrefix + c.ClusterName)); err != nil {
			return nil, backend(err)
		}
	case wire.ClusterCommand_LIST:
		var after []byte
		if c.NextPageToken != nil {
			if len(c.NextPageToken) < 1 || c.NextPageToken[0] != 1 || !utf8.Valid(c.NextPageToken[1:]) {
				return clusterLogical(wire.ClusterResult_INTERNAL, "invalid cluster page token"), nil
			}
			after = c.NextPageToken[1:]
		}
		var last []byte
		truncated := false
		err := scanCluster(tx, clusterPrefix, func(key, value []byte) (bool, error) {
			if after != nil && bytes.Compare(key, after) <= 0 {
				return false, nil
			}
			item := new(wire.ClusterRecord)
			if err := proto.Unmarshal(value, item); err != nil {
				return false, backend(err)
			}
			r.Records = append(r.Records, item)
			r.NextPageToken = append([]byte{1}, key...)
			if proto.Size(r) > 3*1024*1024 {
				r.Records = r.Records[:len(r.Records)-1]
				if len(r.Records) == 0 {
					return false, status.Error(codes.ResourceExhausted, "cluster record exceeds page budget")
				}
				truncated = true
				return true, nil
			}
			last = append([]byte{}, key...)
			if int64(len(r.Records)) >= c.PageSize {
				truncated = true
				return true, nil
			}
			return false, nil
		})
		if err != nil {
			return nil, err
		}
		if truncated {
			r.NextPageToken = append([]byte{1}, last...)
		} else {
			r.NextPageToken = nil
		}
	case wire.ClusterCommand_UPSERT_MEMBER:
		m := proto.Clone(c.Member).(*wire.ClusterMemberRecord)
		m.LastHeartbeat = clusterStamp(now)
		m.RecordExpiry = clusterStamp(now.Add(time.Duration(c.RecordExpiryNanos)))
		if !validClusterTime(m.RecordExpiry) {
			return clusterLogical(wire.ClusterResult_INVALID_ARGUMENT, "membership expiry outside timestamp range"), nil
		}
		if err := saveCluster(tx, memberPrefix+string(m.HostId), m); err != nil {
			return nil, err
		}
	case wire.ClusterCommand_GET_MEMBERS:
		if len(c.NextPageToken) != 0 && len(c.NextPageToken) != 16 {
			return clusterLogical(wire.ClusterResult_INTERNAL, "invalid membership page token"), nil
		}
		var last []byte
		truncated := false
		err := scanCluster(tx, memberPrefix, func(key, value []byte) (bool, error) {
			if c.HostIdEquals != nil {
				if !bytes.Equal(key, c.HostIdEquals) {
					return false, nil
				}
			} else if len(c.NextPageToken) > 0 && bytes.Compare(key, c.NextPageToken) <= 0 {
				return false, nil
			}
			m := new(wire.ClusterMemberRecord)
			if err := proto.Unmarshal(value, m); err != nil {
				return false, backend(err)
			}
			if !validClusterTime(m.RecordExpiry) || !validClusterTime(m.LastHeartbeat) || !validClusterTime(m.SessionStart) {
				return false, status.Error(codes.Unavailable, "corrupt membership timestamp")
			}
			if !instant(m.RecordExpiry).After(now) || (c.LastHeartbeatWithinNanos > 0 && !instant(m.LastHeartbeat).After(now.Add(-time.Duration(c.LastHeartbeatWithinNanos)))) || (c.RoleEquals != 0 && m.Role != c.RoleEquals) || (c.RpcAddressEquals != nil && net.IP(c.RpcAddressEquals).String() != net.IP(m.RpcAddress).String()) || (!instant(c.SessionStartedAfter).IsZero() && instant(m.SessionStart).Before(instant(c.SessionStartedAfter))) {
				return false, nil
			}
			r.Members = append(r.Members, m)
			r.NextPageToken = m.HostId
			if proto.Size(r) > 3*1024*1024 {
				r.Members = r.Members[:len(r.Members)-1]
				if len(r.Members) == 0 {
					return false, status.Error(codes.ResourceExhausted, "membership record exceeds budget")
				}
				truncated = true
				return true, nil
			}
			last = append([]byte{}, m.HostId...)
			if c.PageSize > 0 && int64(len(r.Members)) >= c.PageSize {
				truncated = true
				return true, nil
			}
			return false, nil
		})
		if err != nil {
			return nil, err
		}
		if truncated {
			r.NextPageToken = last
		} else {
			r.NextPageToken = nil
		}
	case wire.ClusterCommand_PRUNE_MEMBERS:
		err := scanCluster(tx, memberPrefix, func(key, value []byte) (bool, error) {
			m := new(wire.ClusterMemberRecord)
			if err := proto.Unmarshal(value, m); err != nil {
				return false, backend(err)
			}
			if !validClusterTime(m.RecordExpiry) {
				return false, status.Error(codes.Unavailable, "corrupt expiry")
			}
			if instant(m.RecordExpiry).Before(now) {
				return false, backend(tx.Delete(append([]byte(memberPrefix), key...)))
			}
			return false, nil
		})
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}
