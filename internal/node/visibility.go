package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/persistence"
	vmodel "github.com/0x63616c/xenon/internal/visibility"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	native "slatedb.io/slatedb-go/uniffi"
)

type VisibilityServer struct {
	wire.UnimplementedVisibilityPersistenceServer
	Owner *Owner
}

func (s *VisibilityServer) Execute(ctx context.Context, q *wire.VisibilityRequest) (*wire.VisibilityResult, error) {
	if q == nil || q.ProtocolVersion != 1 || q.Partition != s.Owner.config.Partition || !operationID.MatchString(q.OperationId) || q.Command == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid history-task envelope")
	}
	q = proto.Clone(q).(*wire.VisibilityRequest)
	c := q.Command
	if e := persistence.ValidateVisibilityCommand(c); e != nil {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	if c.Kind <= wire.VisibilityCommand_GET {
		n, r := c.NamespaceId, c.RunId
		if c.Document != nil {
			n, r = c.Document.NamespaceId, c.Document.RunId
		}
		partition, e := vmodel.Partition(n, r)
		if e != nil || partition != s.Owner.config.Partition {
			return nil, status.Error(codes.InvalidArgument, "visibility document routed to wrong partition")
		}
	}

	b, e := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if e != nil {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	d := sha256.Sum256(b)
	if !bytes.Equal(d[:], q.CommandSha256) {
		return nil, status.Error(codes.InvalidArgument, "digest mismatch")
	}
	raw, e := s.Owner.Run(ctx, func(*native.Db) ([]byte, error) {
		out, e := s.Owner.journal(q.OperationId, d[:], visibilityFamily, func(tx *native.DbTransaction) (*wire.StoredOutcome, error) {
			r, e := applyVisibility(tx, c)
			if e != nil {
				return nil, e
			}
			return &wire.StoredOutcome{Result: &wire.StoredOutcome_VisibilityResult{VisibilityResult: r}}, nil
		})
		if e != nil {
			return nil, e
		}
		return proto.Marshal(out.GetVisibilityResult())
	})
	if e != nil {
		return nil, e
	}
	r := new(wire.VisibilityResult)
	if e = proto.Unmarshal(raw, r); e != nil {
		return nil, backend(e)
	}
	return r, nil
}

func visibilityKey(n, r string) string      { return "v1/visibility/doc/" + n + "/" + r }
func visibilityOrderPrefix(n string) string { return "v1/visibility/order/" + n + "/" }
func visibilityOrder(d *wire.VisibilityDocument) string {
	return visibilityOrderPrefix(d.NamespaceId) + vmodel.SortKey(d)
}
func visibilityIndices(d *wire.VisibilityDocument) ([]string, error) {
	keys := []string{visibilityOrder(d)}
	for name, a := range d.Attributes {
		if a == nil || a.Missing {
			continue
		}
		for _, v := range a.Values {
			value := v
			if a.ValueType == int32(enumspb.INDEXED_VALUE_TYPE_DOUBLE) {
				normalized, e := vmodel.NormalizedScalar(&wire.VisibilityAttribute{ValueType: a.ValueType, Values: []*wire.QueryValue{v}})
				if e != nil {
					return nil, backend(e)
				}
				number, ok := normalized.(float64)
				if !ok {
					return nil, status.Error(codes.Unavailable, "invalid stored numeric index")
				}
				value = &wire.QueryValue{Scalar: &wire.QueryValue_DoubleValue{DoubleValue: number}}
			}
			raw, e := proto.MarshalOptions{Deterministic: true}.Marshal(value)
			if e != nil {
				return nil, backend(e)
			}
			keys = append(keys, fmt.Sprintf("v1/visibility/type/%s/%s/%d/%s/%s", d.NamespaceId, hex.EncodeToString([]byte(name)), a.ValueType, fmt.Sprintf("%x", sha256.Sum256(raw)), d.RunId))
		}
	}
	return keys, nil
}
func applyVisibility(tx *native.DbTransaction, c *wire.VisibilityCommand) (*wire.VisibilityResult, error) {
	outcome, err := persistence.ApplyVisibility(context.Background(), legacyClusterTransaction{legacyShardTransaction{tx}}, c)
	if err != nil {
		return nil, err
	}
	return outcome.GetVisibilityResult(), nil
}
