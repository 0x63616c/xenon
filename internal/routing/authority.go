package routing

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"

	"github.com/0x63616c/xenon/internal/identity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const expectedOwnerHeader = "x-xenon-expected-owner"

// ExpectedOwner is a routing hint, never a lease. Admission rereads control under
// the borrowed writer gate and compares this complete tuple. Path is pinned by
// LayoutDigest, so it need not be repeated in transport metadata.
type ExpectedOwner struct {
	Cluster            identity.ClusterID     `json:"cluster"`
	LayoutDigest       [32]byte               `json:"layout"`
	Partition          identity.PartitionID   `json:"partition"`
	Node               identity.NodeID        `json:"node"`
	Incarnation        identity.IncarnationID `json:"incarnation"`
	AssignmentRevision uint64                 `json:"assignment"`
	Reservation        identity.TransitionID  `json:"reservation"`
	Generation         uint64                 `json:"generation"`
}

func (h ExpectedOwner) valid() bool {
	return h.Cluster.Validate() == nil && h.LayoutDigest != ([32]byte{}) && h.Partition.Validate() == nil && h.Node.Validate() == nil && h.Incarnation.Validate() == nil && h.AssignmentRevision != 0 && h.Reservation.Validate() == nil && h.Generation != 0
}
func (h ExpectedOwner) encode() string {
	raw, _ := json.Marshal(h)
	return base64.RawURLEncoding.EncodeToString(raw)
}
func decodeExpected(md metadata.MD) (ExpectedOwner, error) {
	values := md.Get(expectedOwnerHeader)
	bad := func() (ExpectedOwner, error) {
		return ExpectedOwner{}, status.Error(codes.InvalidArgument, "invalid expected owner metadata")
	}
	if len(values) != 1 || len(values[0]) > 2048 {
		return bad()
	}
	raw, err := base64.RawURLEncoding.DecodeString(values[0])
	if err != nil {
		return bad()
	}
	var hint ExpectedOwner
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&hint) != nil || decoder.Decode(new(any)) != io.EOF || !hint.valid() || hint.encode() != values[0] {
		return bad()
	}
	return hint, nil
}

type expectedOwnerContextKey struct{}

func withExpected(ctx context.Context, h ExpectedOwner) context.Context {
	return context.WithValue(ctx, expectedOwnerContextKey{}, h)
}
func expectedFrom(ctx context.Context) (ExpectedOwner, bool) {
	h, ok := ctx.Value(expectedOwnerContextKey{}).(ExpectedOwner)
	return h, ok && h.valid()
}
