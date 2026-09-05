package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"math"
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
	if c.Kind < wire.NexusCommand_UPSERT || c.Kind > wire.NexusCommand_LIST {
		return nil, status.Error(codes.InvalidArgument, "invalid nexus operation")
	}
	if c.Kind == wire.NexusCommand_UPSERT && (c.Endpoint == nil || len(c.Endpoint.Id) != 16 || len(c.Endpoint.Data) > 1024*1024) {
		return nil, status.Error(codes.InvalidArgument, "invalid nexus endpoint")
	}
	if (c.Kind == wire.NexusCommand_GET || c.Kind == wire.NexusCommand_DELETE) && len(c.Id) != 16 {
		return nil, status.Error(codes.InvalidArgument, "invalid nexus ID")
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

const nexusPrefix = "v1/nexus/endpoint/"
const nexusVersion = "v1/nexus/version"

func applyNexus(tx *native.DbTransaction, c *wire.NexusCommand) (*wire.NexusResult, error) {
	raw, err := get(tx, nexusVersion)
	if err != nil {
		return nil, err
	}
	var version int64
	if raw != nil {
		if len(raw) != 8 {
			return nil, status.Error(codes.Unavailable, "corrupt nexus version")
		}
		version = int64(binary.BigEndian.Uint64(raw))
		if version < 1 {
			return nil, status.Error(codes.Unavailable, "invalid nexus version")
		}
	}
	r := &wire.NexusResult{TableVersion: version}
	fail := func(code wire.NexusResult_Error, msg string) (*wire.NexusResult, error) {
		r.Error = code
		r.Message = msg
		return r, nil
	}
	switch c.Kind {
	case wire.NexusCommand_LIST:
		var after []byte
		if len(c.NextPageToken) > 0 {
			if len(c.NextPageToken) != 17 || c.NextPageToken[0] != 1 {
				return fail(wire.NexusResult_INTERNAL, "invalid nexus page token")
			}
			after = c.NextPageToken[1:]
		}
		if c.TableVersion != 0 && c.TableVersion != version {
			return fail(wire.NexusResult_UNAVAILABLE, "nexus endpoints table version mismatch")
		}
		if c.PageSize <= 0 {
			return r, nil
		}
		var last []byte
		truncated := false
		err = scanCluster(tx, nexusPrefix, func(key, value []byte) (bool, error) {
			if after != nil && bytes.Compare(key, after) <= 0 {
				return false, nil
			}
			item := new(wire.NexusEndpoint)
			if e := proto.Unmarshal(value, item); e != nil {
				return false, backend(e)
			}
			if len(key) != 16 || !bytes.Equal(key, item.Id) {
				return false, status.Error(codes.Unavailable, "corrupt nexus identity")
			}
			r.Endpoints = append(r.Endpoints, item)
			r.NextPageToken = append([]byte{1}, key...)
			if proto.Size(r) > 3*1024*1024 {
				r.Endpoints = r.Endpoints[:len(r.Endpoints)-1]
				if len(r.Endpoints) == 0 {
					return false, status.Error(codes.ResourceExhausted, "nexus endpoint exceeds page budget")
				}
				truncated = true
				return true, nil
			}
			last = append([]byte{}, key...)
			if int64(len(r.Endpoints)) >= c.PageSize {
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
		return r, nil
	case wire.NexusCommand_GET:
		item := new(wire.NexusEndpoint)
		exists, e := loadCluster(tx, nexusPrefix+string(c.Id), item)
		if e != nil {
			return nil, e
		}
		if !exists {
			return fail(wire.NexusResult_NOT_FOUND, "nexus endpoint not found")
		}
		r.Endpoint = item
		return r, nil
	}
	if c.TableVersion != version || (c.Kind == wire.NexusCommand_DELETE && version == 0) {
		code := wire.NexusResult_UNAVAILABLE
		if c.Kind == wire.NexusCommand_UPSERT && c.TableVersion == 0 && version != 0 {
			code = wire.NexusResult_CONDITION_FAILED
		}
		return fail(code, "nexus endpoints table version mismatch")
	}
	if version == math.MaxInt64 {
		return fail(wire.NexusResult_RESOURCE_EXHAUSTED, "nexus table version exhausted")
	}
	id := c.Id
	if c.Kind == wire.NexusCommand_UPSERT {
		id = c.Endpoint.Id
	}
	key := nexusPrefix + string(id)
	item := new(wire.NexusEndpoint)
	exists, err := loadCluster(tx, key, item)
	if err != nil {
		return nil, err
	}
	if c.Kind == wire.NexusCommand_DELETE {
		if !exists {
			return fail(wire.NexusResult_NOT_FOUND, "nexus endpoint not found")
		}
		if err = tx.Delete([]byte(key)); err != nil {
			return nil, backend(err)
		}
	} else {
		if (c.Endpoint.Version == 0 && exists) || (c.Endpoint.Version != 0 && (!exists || item.Version != c.Endpoint.Version)) {
			return fail(wire.NexusResult_UNAVAILABLE, "nexus endpoint version mismatch")
		}
		if c.Endpoint.Version == math.MaxInt64 {
			return fail(wire.NexusResult_RESOURCE_EXHAUSTED, "nexus endpoint version exhausted")
		}
		item = proto.Clone(c.Endpoint).(*wire.NexusEndpoint)
		item.Version++
		if err = saveCluster(tx, key, item); err != nil {
			return nil, err
		}
	}
	raw = make([]byte, 8)
	binary.BigEndian.PutUint64(raw, uint64(version+1))
	if err = put(tx, nexusVersion, raw); err != nil {
		return nil, err
	}
	r.TableVersion = version + 1
	return r, nil
}
