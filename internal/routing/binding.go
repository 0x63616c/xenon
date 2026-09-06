package routing

import (
	"context"
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/persistence"
	"github.com/0x63616c/xenon/internal/registry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ServerConfig is fixed before serving. The host owns driver polling, stop/drain
// and network lifecycle. The binding owns authoritative reads and never opens a
// writer. Layout and its expected digest must come from explicit host config.
type ServerConfig struct {
	Cluster              identity.ClusterID
	Owner                cluster.Owner
	Layout               cluster.Layout
	ExpectedLayoutDigest [32]byte
	Store                registry.Store
	ControlKey           registry.Key
	MaxControlBytes      int
	MaxOutcomes          uint64
	Now                  func() time.Time
	Partitions           map[identity.PartitionID]*partitions.Service
	Events               chan<- Event
}
type binding struct{ config ServerConfig }

func newBinding(c ServerConfig) (*binding, error) {
	digest, err := c.Layout.Digest()
	if err != nil || digest != c.ExpectedLayoutDigest || c.ExpectedLayoutDigest == ([32]byte{}) || c.Cluster.Validate() != nil || c.Owner.Node.Validate() != nil || c.Owner.Incarnation.Validate() != nil || c.Owner.Address == "" || c.Store == nil || registry.ValidateKey(c.ControlKey) != nil || c.MaxControlBytes <= 0 || c.MaxOutcomes == 0 || c.Now == nil || len(c.Partitions) != len(c.Layout.Partitions) {
		return nil, errors.New("invalid RPC binding configuration")
	}
	for _, p := range c.Layout.Partitions {
		if c.Partitions[p.ID] == nil {
			return nil, errors.New("missing partition driver")
		}
	}
	c.Layout.Partitions = slices.Clone(c.Layout.Partitions)
	c.Partitions = maps.Clone(c.Partitions)
	return &binding{config: c}, nil
}
func (b *binding) read(ctx context.Context) (cluster.Control, error) {
	record, err := b.config.Store.Read(ctx, b.config.ControlKey)
	if err != nil {
		if ctx.Err() != nil {
			return cluster.Control{}, status.FromContextError(ctx.Err()).Err()
		}
		return cluster.Control{}, status.Error(codes.Unavailable, "cannot read partition authority")
	}
	snapshot, err := cluster.DecodeControl(b.config.ControlKey, record, b.config.MaxControlBytes)
	if err != nil {
		return cluster.Control{}, status.Error(codes.FailedPrecondition, "invalid partition control")
	}
	if err = snapshot.ValidateLayout(b.config.ExpectedLayoutDigest); err != nil {
		return cluster.Control{}, status.Error(codes.FailedPrecondition, "partition layout mismatch")
	}
	control := snapshot.Control()
	if control.Cluster != b.config.Cluster {
		return cluster.Control{}, status.Error(codes.FailedPrecondition, "cluster identity mismatch")
	}
	return control, nil
}
func (b *binding) hint(id identity.PartitionID, p cluster.PartitionControl) ExpectedOwner {
	return ExpectedOwner{Cluster: b.config.Cluster, LayoutDigest: b.config.ExpectedLayoutDigest, Partition: id, Node: p.Desired.Node, Incarnation: p.Desired.Incarnation, AssignmentRevision: p.AssignmentRevision, Reservation: p.Reservation, Generation: p.Generation}
}
func (b *binding) Resolve(ctx context.Context, logical string, _ bool) (Route, error) {
	physical, ok := b.config.Layout.Resolve(logical)
	if !ok {
		return Route{}, status.Error(codes.NotFound, "unknown logical partition")
	}
	control, err := b.read(ctx)
	if err != nil {
		return Route{}, err
	}
	p, ok := control.Partitions[physical.ID]
	if !ok || p.Path != physical.Path || !p.Ready {
		return Route{}, status.Error(codes.Unavailable, "no ready partition owner")
	}
	hint := b.hint(physical.ID, p)
	return Route{Node: string(p.Desired.Node) + "/" + string(p.Desired.Incarnation), Address: p.Desired.Address, Expected: &hint}, nil
}
func (b *binding) dispatch(ctx context.Context, method string, request proto.Message) (proto.Message, error) {
	entry, ok := serviceMethods[method]
	if !ok {
		return nil, status.Error(codes.Unimplemented, "unknown persistence method")
	}
	envelope, ok := request.(interface{ GetPartition() string })
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "missing logical partition")
	}
	physical, ok := b.config.Layout.Resolve(envelope.GetPartition())
	if !ok {
		return nil, status.Error(codes.NotFound, "unknown logical partition")
	}
	expected, ok := expectedFrom(ctx)
	if !ok || expected.Cluster != b.config.Cluster || expected.LayoutDigest != b.config.ExpectedLayoutDigest || expected.Partition != physical.ID || expected.Node != b.config.Owner.Node || expected.Incarnation != b.config.Owner.Incarnation {
		return nil, StaleOwner()
	}
	driver := b.config.Partitions[physical.ID]
	writer, attempt, token, ready := driver.Writer()
	if !ready || attempt.Partition != physical.ID || attempt.Path != physical.Path || attempt.Incarnation != expected.Incarnation || attempt.AssignmentRevision != expected.AssignmentRevision || attempt.Reservation != expected.Reservation || attempt.Generation != expected.Generation {
		return nil, StaleOwner()
	}
	authority := func(ctx context.Context) error {
		control, err := b.read(ctx)
		if err != nil {
			return err
		}
		p, ok := control.Partitions[physical.ID]
		if !ok || !p.Ready || p.Path != attempt.Path || p.Desired != b.config.Owner || b.hint(physical.ID, p) != expected {
			return StaleOwner()
		}
		// Compare-only reborrow rejects stopping/replaced drivers. Never substitute
		// its newer token into the failure callback or the borrowed writer below.
		_, currentAttempt, currentToken, currentReady := driver.Writer()
		if !currentReady || currentToken != token || currentAttempt != attempt {
			return StaleOwner()
		}
		return nil
	}
	base, err := persistence.NewService(writer, physical.ID, b.config.MaxOutcomes, authority, func(err error) { driver.ObserveFailure(token, err) })
	if err != nil {
		return nil, status.Error(codes.Internal, "cannot bind persistence service")
	}
	local := proto.Clone(request)
	field := local.ProtoReflect().Descriptor().Fields().ByName("partition")
	if field == nil || field.Kind() != protoreflect.StringKind {
		return nil, status.Error(codes.InvalidArgument, "missing partition envelope")
	}
	local.ProtoReflect().Set(field, protoreflect.ValueOfString(string(physical.ID)))
	response, err := entry.execute(ctx, b, base, local)
	// Family callbacks already observed the original typed error with the exact
	// token. Any native failure here may follow durable execution child journals.
	var unknown *partitions.UnknownOutcome
	if errors.As(err, &unknown) || errors.Is(err, partitions.ErrFenced) || errors.Is(err, partitions.ErrRetired) {
		return nil, UnknownOutcome()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, status.FromContextError(err).Err()
	}
	return response, err
}
