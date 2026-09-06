// Package persistence owns atomic Temporal operations and durable replay.
package persistence

import (
	"bytes"
	"context"
	"math"
	"net"
	"time"
	"unicode/utf8"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/partitions"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ClusterTransaction is implemented by the opaque partition transaction and the
// retiring node bridge. No writer is opened by operation semantics.
type ClusterTransaction interface {
	ShardTransaction
	Delete([]byte) error
	Scan(context.Context, partitions.ScanRequest) (partitions.ReadResult, error)
}

// ValidateClusterCommand preserves the existing transport validation separately
// from logical failures that must be durably replayed with operation results.
func ValidateClusterCommand(c *wire.ClusterCommand) error {
	if c == nil {
		return status.Error(codes.InvalidArgument, "invalid cluster command")
	}
	if c.Kind < wire.ClusterCommand_LIST || c.Kind > wire.ClusterCommand_PRUNE_MEMBERS || len(c.ClusterName) > 1024 || !utf8.ValidString(c.ClusterName) {
		return status.Error(codes.InvalidArgument, "invalid cluster command")
	}
	if c.Kind == wire.ClusterCommand_SAVE && (c.Blob == nil || len(c.Blob.Data) > 1024*1024) {
		return status.Error(codes.InvalidArgument, "invalid cluster blob")
	}
	if c.Kind == wire.ClusterCommand_LIST && c.PageSize < 1 {
		return status.Error(codes.InvalidArgument, "cluster page size must be positive")
	}
	if c.Kind == wire.ClusterCommand_GET_MEMBERS && !validClusterTime(c.SessionStartedAfter) {
		return status.Error(codes.InvalidArgument, "invalid session filter")
	}
	if c.Kind == wire.ClusterCommand_UPSERT_MEMBER {
		m := c.Member
		if m == nil || len(m.HostId) != 16 || (len(m.RpcAddress) != 4 && len(m.RpcAddress) != 16) || m.RpcPort > 65535 || !validClusterTime(m.SessionStart) {
			return status.Error(codes.InvalidArgument, "invalid cluster member")
		}
	}
	return nil
}

// ApplyCluster stages the existing metadata CAS/list and Temporal membership
// semantics. now is an explicit observation; replay never reevaluates expiry.
// The caller owns serialization, outcome accounting, commit and AwaitDurable.
func ApplyCluster(ctx context.Context, tx ClusterTransaction, c *wire.ClusterCommand, now time.Time) (*wire.StoredOutcome, error) {
	if tx == nil {
		return nil, status.Error(codes.InvalidArgument, "nil cluster transaction")
	}
	if err := ValidateClusterCommand(c); err != nil {
		return nil, err
	}
	result, err := applyCluster(ctx, tx, c, now.UTC())
	if err != nil {
		return nil, err
	}
	if proto.Size(result) > 3*1024*1024 {
		return nil, status.Error(codes.ResourceExhausted, "cluster response exceeds byte budget")
	}
	return &wire.StoredOutcome{Result: &wire.StoredOutcome_ClusterResult{ClusterResult: result}}, nil
}

func clusterLogical(code wire.ClusterResult_Error, message string) *wire.ClusterResult {
	return &wire.ClusterResult{Error: code, Message: message}
}

const clusterPrefix = "v1/cluster/metadata/"
const memberPrefix = "v1/cluster/member/"

func clusterEncodingError(err error) error {
	return status.Errorf(codes.Unavailable, "storage outcome unknown: %v", err)
}
func loadCluster(ctx context.Context, tx ClusterTransaction, key string, message proto.Message) (bool, error) {
	b, err := tx.Get(ctx, []byte(key))
	if err != nil || b == nil {
		return false, err
	}
	if err = proto.Unmarshal(b, message); err != nil {
		return false, clusterEncodingError(err)
	}
	return true, nil
}
func saveCluster(tx ClusterTransaction, key string, message proto.Message) error {
	b, err := proto.Marshal(message)
	if err != nil {
		return clusterEncodingError(err)
	}
	return tx.Put([]byte(key), b)
}
func scanCluster(ctx context.Context, tx ClusterTransaction, prefix string, visit func([]byte, []byte) (bool, error)) error {
	// Prefixes end in '/', so incrementing that byte is the exact exclusive end.
	end := []byte(prefix)
	end[len(end)-1]++
	scan := partitions.ScanRequest{Start: []byte(prefix), End: end, Limit: 1}
	for {
		rows, err := tx.Scan(ctx, scan)
		if err != nil {
			return err
		}
		for _, row := range rows.Entries {
			if !bytes.HasPrefix(row.Key, []byte(prefix)) {
				return status.Error(codes.Unavailable, "cluster scan returned out-of-range key")
			}
			stop, err := visit(row.Key[len(prefix):], row.Value)
			if err != nil {
				return err
			}
			if stop {
				return nil
			}
			scan.Start = bytes.Clone(row.Key)
			scan.StartExclusive = true
		}
		if !rows.More {
			return nil
		}
		if len(rows.Entries) == 0 {
			return status.Error(codes.Unavailable, "cluster scan did not advance")
		}
	}
}
func clusterStamp(t time.Time) *wire.ClusterTime {
	return &wire.ClusterTime{Seconds: t.Unix(), Nanos: int32(t.Nanosecond())}
}
func validClusterTime(t *wire.ClusterTime) bool {
	return t != nil && t.Nanos >= 0 && t.Nanos < 1e9 && t.Seconds >= -62135596800 && t.Seconds <= 253402300799
}
func instant(t *wire.ClusterTime) time.Time { return time.Unix(t.Seconds, int64(t.Nanos)).UTC() }
func applyCluster(ctx context.Context, tx ClusterTransaction, c *wire.ClusterCommand, now time.Time) (*wire.ClusterResult, error) {
	r := new(wire.ClusterResult)
	switch c.Kind {
	case wire.ClusterCommand_GET, wire.ClusterCommand_SAVE:
		record := new(wire.ClusterRecord)
		exists, err := loadCluster(ctx, tx, clusterPrefix+c.ClusterName, record)
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
			return nil, err
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
		err := scanCluster(ctx, tx, clusterPrefix, func(key, value []byte) (bool, error) {
			if after != nil && bytes.Compare(key, after) <= 0 {
				return false, nil
			}
			item := new(wire.ClusterRecord)
			if err := proto.Unmarshal(value, item); err != nil {
				return false, clusterEncodingError(err)
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
		err := scanCluster(ctx, tx, memberPrefix, func(key, value []byte) (bool, error) {
			if c.HostIdEquals != nil {
				if !bytes.Equal(key, c.HostIdEquals) {
					return false, nil
				}
			} else if len(c.NextPageToken) > 0 && bytes.Compare(key, c.NextPageToken) <= 0 {
				return false, nil
			}
			m := new(wire.ClusterMemberRecord)
			if err := proto.Unmarshal(value, m); err != nil {
				return false, clusterEncodingError(err)
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
		err := scanCluster(ctx, tx, memberPrefix, func(key, value []byte) (bool, error) {
			m := new(wire.ClusterMemberRecord)
			if err := proto.Unmarshal(value, m); err != nil {
				return false, clusterEncodingError(err)
			}
			if !validClusterTime(m.RecordExpiry) {
				return false, status.Error(codes.Unavailable, "corrupt expiry")
			}
			if instant(m.RecordExpiry).Before(now) {
				return false, tx.Delete(append([]byte(memberPrefix), key...))
			}
			return false, nil
		})
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}
