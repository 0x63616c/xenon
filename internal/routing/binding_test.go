package routing

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/identity"
	p "github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/registry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type rpcStore struct {
	mu     sync.Mutex
	record registry.Record
	next   int
}

func (s *rpcStore) Read(context.Context, registry.Key) (registry.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.record.Clone(), nil
}
func (s *rpcStore) Create(context.Context, registry.Key, registry.Write) (registry.Record, error) {
	panic("unexpected bootstrap")
}
func (s *rpcStore) Replace(_ context.Context, key registry.Key, v registry.Version, w registry.Write) (registry.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.record.Version != v {
		return registry.Record{}, &registry.Conflict{Key: key}
	}
	body, err := registry.Encode(key, v, w)
	if err != nil {
		return registry.Record{}, err
	}
	s.record = registry.Record{Body: body, Version: registry.Version(w.Transition)}
	return s.record.Clone(), nil
}
func (s *rpcStore) change(t *testing.T, fn func(*cluster.Control)) {
	t.Helper()
	record, _ := s.Read(context.Background(), "cluster/control")
	snapshot, err := cluster.DecodeControl("cluster/control", record, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	control := snapshot.Control()
	fn(&control)
	raw, _ := json.Marshal(control)
	s.next++
	write, err := registry.NewWrite("cluster/control", record.Version, identity.TransitionID(fmt.Sprintf("trn_%022d", s.next+1000)), raw)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Replace(context.Background(), "cluster/control", record.Version, write)
	if err != nil {
		t.Fatal(err)
	}
}

type rpcIDs struct{ n int }

func (s *rpcIDs) NewID(prefix string) (string, error) {
	s.n++
	return fmt.Sprintf("%s_%022d", prefix, s.n+100), nil
}

type rpcEngine struct {
	writer *rpcWriter
	opened bool
}

func (e *rpcEngine) Open(context.Context, p.OpenRequest) (p.Writer, error) {
	if e.opened {
		e.writer = &rpcWriter{data: maps.Clone(e.writer.data)}
	}
	e.opened = true
	return e.writer, nil
}

// Controlled application storage, separate from actual partition lifecycle.
// Commit stages; AwaitDurable alone publishes the journal and mutation map.
type rpcWriter struct {
	p.Writer
	data                   map[string][]byte
	reads, commits, closes int
	retireAfter            int
	retired                bool
	onBegin                func()
}
type rpcOperation struct {
	w        *rpcWriter
	released bool
}
type rpcTx struct {
	w    *rpcWriter
	data map[string][]byte
}
type rpcReceipt struct{ tx *rpcTx }

func (r *rpcReceipt) MutationID() uint64 { return uint64(r.tx.w.commits) }
func (w *rpcWriter) BeginOperation(context.Context) (p.Operation, error) {
	if w.retired {
		return nil, p.ErrRetired
	}
	return &rpcOperation{w: w}, nil
}
func (o *rpcOperation) Release() { o.released = true }
func (o *rpcOperation) Begin(ctx context.Context) (p.Transaction, error) {
	if o.released {
		return nil, p.ErrOperationDone
	}
	return o.w.Begin(ctx)
}
func (w *rpcWriter) Begin(context.Context) (p.Transaction, error) {
	if w.onBegin != nil {
		fn := w.onBegin
		w.onBegin = nil
		fn()
	}
	if w.retired {
		return nil, p.ErrRetired
	}
	return &rpcTx{w: w, data: maps.Clone(w.data)}, nil
}
func (w *rpcWriter) AwaitDurable(_ context.Context, r p.CommitReceipt) error {
	w.data = maps.Clone(r.(*rpcReceipt).tx.data)
	w.commits++
	if w.retireAfter > 0 && w.commits == w.retireAfter {
		w.retired = true
	}
	return nil
}
func (w *rpcWriter) Close(context.Context) error { w.closes++; w.retired = true; return nil }
func (tx *rpcTx) Get(_ context.Context, key []byte) ([]byte, error) {
	tx.w.reads++
	return bytes.Clone(tx.data[string(key)]), nil
}
func (tx *rpcTx) Put(key, value []byte) error                     { tx.data[string(key)] = bytes.Clone(value); return nil }
func (tx *rpcTx) Delete(key []byte) error                         { delete(tx.data, string(key)); return nil }
func (tx *rpcTx) Abort() error                                    { return nil }
func (tx *rpcTx) Commit(context.Context) (p.CommitReceipt, error) { return &rpcReceipt{tx: tx}, nil }
func (tx *rpcTx) Scan(_ context.Context, r p.ScanRequest) (p.ReadResult, error) {
	keys := slices.Sorted(maps.Keys(tx.data))
	if r.Reverse {
		slices.Reverse(keys)
	}
	out := p.ReadResult{}
	for _, key := range keys {
		b := []byte(key)
		if r.Start != nil && (bytes.Compare(b, r.Start) < 0 || r.StartExclusive && bytes.Equal(b, r.Start)) {
			continue
		}
		if r.End != nil && (bytes.Compare(b, r.End) > 0 || !r.EndInclusive && bytes.Equal(b, r.End)) {
			continue
		}
		if len(out.Entries) == r.Limit {
			out.More = true
			break
		}
		out.Entries = append(out.Entries, p.Entry{Key: b, Value: bytes.Clone(tx.data[key])})
	}
	return out, nil
}

func rpcFixture(t *testing.T) (ServerConfig, *rpcStore, *rpcWriter) {
	t.Helper()
	owner := cluster.Owner{Node: "nod_0000000000000000000001", Incarnation: "inc_0000000000000000000001", Address: "local:8080"}
	layout := cluster.Layout{Version: 1, Placement: cluster.DefaultPlacementConfig(), Partitions: []cluster.PhysicalPartition{{LogicalName: "logical-domain", ID: "prt_0000000000000000000001", Path: "data/explicit-path"}}}
	digest, _ := layout.Digest()
	partition := layout.Partitions[0]
	write, err := cluster.BootstrapWrite("cluster/control", "trn_0000000000000000000001", cluster.Control{Format: cluster.ControlFormat, Layout: &layout, Cluster: "clu_0000000000000000000001", Coordinator: cluster.Coordinator{Incarnation: owner.Incarnation, Generation: 1}, AssignmentRevision: 1, Partitions: map[identity.PartitionID]cluster.PartitionControl{partition.ID: {Path: partition.Path, Desired: owner, AssignmentRevision: 1}}}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := registry.Encode("cluster/control", "", write)
	if err != nil {
		t.Fatal(err)
	}
	store := &rpcStore{record: registry.Record{Body: encoded, Version: registry.Version(write.Transition)}}
	writer := &rpcWriter{data: map[string][]byte{}}
	driver, err := p.NewService(context.Background(), p.ControllerConfig{ExpectedLayoutDigest: digest, Key: "cluster/control", Partition: partition.ID, Incarnation: owner.Incarnation, MaxControlBytes: 1 << 20}, store, &rpcEngine{writer: writer}, &rpcIDs{})
	if err != nil {
		t.Fatal(err)
	}
	driver.Poll()
	synctest.Wait()
	if _, _, _, ready := driver.Writer(); !ready {
		t.Fatal("actual partition driver did not activate")
	}
	return ServerConfig{Cluster: "clu_0000000000000000000001", Owner: owner, Layout: layout, ExpectedLayoutDigest: digest, Store: store, ControlKey: "cluster/control", MaxControlBytes: 1 << 20, MaxOutcomes: 1000, Now: func() time.Time { return time.Unix(100, 0) }, Partitions: map[identity.PartitionID]*p.Service{partition.ID: driver}}, store, writer
}
func envelope(q proto.Message, id int, command proto.Message) proto.Message {
	ref := q.ProtoReflect()
	fields := ref.Descriptor().Fields()
	ref.Set(fields.ByName("protocol_version"), protoreflect.ValueOfUint32(1))
	ref.Set(fields.ByName("partition"), protoreflect.ValueOfString("logical-domain"))
	ref.Set(fields.ByName("operation_id"), protoreflect.ValueOfString(fmt.Sprintf("op_%022d", id)))
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(command)
	digest := sha256.Sum256(raw)
	ref.Set(fields.ByName("command_sha256"), protoreflect.ValueOfBytes(digest[:]))
	ref.Set(fields.ByName("command"), protoreflect.ValueOfMessage(command.ProtoReflect()))
	return q
}
func callInterceptor(ctx context.Context, router *Router, method string, q proto.Message) (any, error) {
	return router.Interceptor(serviceReply)(ctx, q, &grpc.UnaryServerInfo{FullMethod: method}, nil)
}
func TestAllServiceFamiliesDispatchThroughRealPartitionDriver(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config, _, writer := rpcFixture(t)
		server, router, err := NewServer(config)
		if err != nil {
			t.Fatal(err)
		}
		defer server.Stop()
		defer router.Close()
		if len(server.GetServiceInfo()) != 12 {
			t.Fatal("missing service registration")
		}
		cases := []struct {
			method           string
			request, command proto.Message
		}{
			{wire.ShardPersistence_Execute_FullMethodName, &wire.ShardRequest{}, &wire.ShardCommand{Kind: wire.ShardCommand_GET, ShardId: 7}},
			{wire.QueuePersistence_Execute_FullMethodName, &wire.QueueRequest{}, &wire.QueueCommand{Kind: wire.QueueCommand_INIT, QueueType: 1}},
			{wire.QueueV2Persistence_Execute_FullMethodName, &wire.QueueV2Request{}, &wire.QueueV2Command{Kind: wire.QueueV2Command_LIST, PageSize: 1}},
			{wire.MetadataPersistence_Execute_FullMethodName, &wire.MetadataRequest{}, &wire.MetadataCommand{Kind: wire.MetadataCommand_GET_METADATA}},
			{wire.ClusterPersistence_Execute_FullMethodName, &wire.ClusterRequest{}, &wire.ClusterCommand{Kind: wire.ClusterCommand_LIST, PageSize: 1}},
			{wire.NexusPersistence_Execute_FullMethodName, &wire.NexusRequest{}, &wire.NexusCommand{Kind: wire.NexusCommand_LIST, PageSize: 1}},
			{wire.MatchingPersistence_Execute_FullMethodName, &wire.MatchingRequest{}, &wire.MatchingCommand{Kind: wire.MatchingCommand_LIST_QUEUES, PageSize: 1}},
			{wire.HistoryPersistence_Execute_FullMethodName, &wire.HistoryRequest{}, &wire.HistoryCommand{Kind: wire.HistoryCommand_LIST_TREES, PageSize: 1}},
			{wire.ExecutionPersistence_Execute_FullMethodName, &wire.ExecutionRequest{}, &wire.ExecutionCommand{Kind: wire.ExecutionCommand_LIST, PageSize: 1}},
			{wire.HistoryTasksPersistence_Execute_FullMethodName, &wire.HistoryTasksRequest{}, &wire.HistoryTasksCommand{Kind: wire.HistoryTasksCommand_READ, CategoryType: 1, Minimum: &wire.ExecutionTask{}, Maximum: &wire.ExecutionTask{TaskId: 10}, PageSize: 1}},
			{wire.ExecutionTasksPersistence_Execute_FullMethodName, &wire.ExecutionTasksRequest{}, &wire.ExecutionTasksCommand{Kind: wire.ExecutionTasksCommand_IS_EMPTY_DLQ}},
			{wire.VisibilityPersistence_Execute_FullMethodName, &wire.VisibilityRequest{}, &wire.VisibilityCommand{Kind: wire.VisibilityCommand_GET_SCHEMA}},
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		for index, tc := range cases {
			request := envelope(tc.request, index+1, tc.command)
			before := proto.Clone(request)
			response, err := callInterceptor(ctx, router, tc.method, request)
			if err != nil || response == nil {
				t.Fatalf("%s: %v", tc.method, err)
			}
			if !proto.Equal(before, request) {
				t.Fatal("external request mutated")
			}
		}
		if writer.commits != 12 {
			t.Fatal("families bypassed durable replay", writer.commits)
		}
		for i := range cases {
			if writer.data[fmt.Sprintf("v1/outcome/op_%022d", i+1)] == nil {
				t.Fatal("missing journal", i)
			}
		}
	})
}

func TestStaleResolvedAndForwardedHintsDoNotExecute(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config, store, writer := rpcFixture(t)
		binding, err := newBinding(config)
		if err != nil {
			t.Fatal(err)
		}
		route, err := binding.Resolve(context.Background(), "logical-domain", false)
		if err != nil {
			t.Fatal(err)
		}
		request := envelope(&wire.ShardRequest{}, 1, &wire.ShardCommand{Kind: wire.ShardCommand_CREATE_OR_GET, ShardId: 7})
		change := func() {
			store.change(t, func(control *cluster.Control) {
				control.AssignmentRevision++
				p := control.Partitions[route.Expected.Partition]
				p.AssignmentRevision++
				p.Generation++
				p.Reservation = "trn_0000000000000000000999"
				control.Partitions[route.Expected.Partition] = p
			})
		}
		writer.onBegin = change // changes after atomic borrow but before under-gate check
		_, err = binding.dispatch(withExpected(context.Background(), *route.Expected), wire.ShardPersistence_Execute_FullMethodName, request)
		if !retryable(err, request) || writer.reads != 0 || writer.commits != 0 {
			t.Fatal("stale admission executed", err, writer.reads)
		}
		driver := config.Partitions[route.Expected.Partition]
		driver.Poll()
		synctest.Wait()
		replacement, _, _, ready := driver.Writer()
		if !ready {
			t.Fatal("replacement driver not ready")
		}
		writer = replacement.(*rpcWriter)
		server, router, err := NewServer(config)
		if err != nil {
			t.Fatal(err)
		}
		defer server.Stop()
		defer router.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(hopHeader, "1", expectedOwnerHeader, route.Expected.encode()))
		_, err = callInterceptor(ctx, router, wire.ShardPersistence_Execute_FullMethodName, request)
		if !retryable(err, request) || writer.reads != 0 {
			t.Fatal("fresh resolution replaced stale forwarded hint", err)
		}
		for _, values := range [][]string{nil, {"not-base64"}, {route.Expected.encode(), route.Expected.encode()}} {
			md := metadata.Pairs(hopHeader, "1")
			if values != nil {
				md[expectedOwnerHeader] = values
			}
			_, err = callInterceptor(metadata.NewIncomingContext(ctx, md), router, wire.ShardPersistence_Execute_FullMethodName, request)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatal("bad forwarding hint admitted", err)
			}
		}
	})
}

func TestRetirementAfterDurableExecutionChildRemainsUnknown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config, _, writer := rpcFixture(t)
		server, router, err := NewServer(config)
		if err != nil {
			t.Fatal(err)
		}
		defer server.Stop()
		defer router.Close()
		history := &wire.HistoryCommand{Kind: wire.HistoryCommand_APPEND, TreeId: bytes.Repeat([]byte{1}, 16), BranchId: bytes.Repeat([]byte{2}, 16), Node: &wire.HistoryNodeRecord{NodeId: 1, Events: &wire.HistoryBlob{Data: []byte("child")}}}
		command := &wire.ExecutionCommand{Kind: wire.ExecutionCommand_GET, NamespaceId: "11111111-1111-4111-8111-111111111111", WorkflowId: "workflow", RunId: "22222222-2222-4222-8222-222222222222", HistoryPrewrites: []*wire.HistoryCommand{history}}
		request := envelope(&wire.ExecutionRequest{}, 10, command)
		original := proto.Clone(request)
		writer.retireAfter = 1 // child is durable, subsequent root Begin returns Retired
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err = callInterceptor(ctx, router, wire.ExecutionPersistence_Execute_FullMethodName, request)
		if !isUnknownRouting(err) {
			t.Fatal("post-child retirement mislabeled", err)
		}
		synctest.Wait()
		if writer.closes != 1 {
			t.Fatal("original borrowed token not retired", writer.closes)
		}
		if writer.data["v1/outcome/op_0000000000000000000010-h-0"] == nil || writer.data["v1/outcome/op_0000000000000000000010"] != nil {
			t.Fatal("child/root journal boundary lost")
		}
		if !proto.Equal(request, original) {
			t.Fatal("unknown retry changed operation")
		}
	})
}

func TestForwardingCarriesFullHintAndExactEnvelope(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		config, _, writer := rpcFixture(t)
		server, destination, err := NewServer(config)
		if err != nil {
			t.Fatal(err)
		}
		defer server.Stop()
		defer destination.Close()
		originConfig := config
		originConfig.Owner = cluster.Owner{Node: "nod_0000000000000000000002", Incarnation: "inc_0000000000000000000002", Address: "other:8080"}
		originServer, origin, err := NewServer(originConfig)
		if err != nil {
			t.Fatal(err)
		}
		defer originServer.Stop()
		defer origin.Close()
		request := envelope(&wire.ShardRequest{}, 1, &wire.ShardCommand{Kind: wire.ShardCommand_GET, ShardId: 7})
		saved := proto.Clone(request)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		deadline, _ := ctx.Deadline()
		forwards := 0
		origin.Invoke = func(forward context.Context, address, method string, q, response proto.Message) error {
			forwards++
			gotDeadline, ok := forward.Deadline()
			if address != config.Owner.Address || !ok || gotDeadline != deadline || !proto.Equal(q, saved) {
				t.Fatal("forward changed envelope/deadline")
			}
			md, _ := metadata.FromOutgoingContext(forward)
			hint, err := decodeExpected(md)
			if err != nil || hint.Cluster != config.Cluster || hint.LayoutDigest != config.ExpectedLayoutDigest || hint.Partition != config.Layout.Partitions[0].ID || hint.Node != config.Owner.Node {
				t.Fatal("missing full route tuple", hint, err)
			}
			result, err := callInterceptor(metadata.NewIncomingContext(forward, md), destination, method, q)
			if err == nil {
				proto.Merge(response, result.(proto.Message))
			}
			return err
		}
		if _, err = callInterceptor(ctx, origin, wire.ShardPersistence_Execute_FullMethodName, request); err != nil || forwards != 1 || writer.commits != 1 {
			t.Fatal(err, forwards, writer.commits)
		}
		if !proto.Equal(saved, request) {
			t.Fatal("caller envelope changed")
		}
	})
}

func TestRPCLayoutAndClusterPinsRejectChangedControl(t *testing.T) {
	for _, kind := range []string{"layout", "cluster"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				config, store, writer := rpcFixture(t)
				binding, err := newBinding(config)
				if err != nil {
					t.Fatal(err)
				}
				store.change(t, func(control *cluster.Control) {
					if kind == "layout" {
						control.Layout.Partitions[0].LogicalName = "different-logical-domain"
					} else {
						control.Cluster = "clu_0000000000000000000002"
					}
				})
				if _, err = binding.Resolve(context.Background(), "logical-domain", true); status.Code(err) != codes.FailedPrecondition || writer.reads != 0 {
					t.Fatal("control pin bypassed", err)
				}
			})
		})
	}
}

func TestOriginKeepsEarlierUnknownAcrossLaterFailures(t *testing.T) {
	for _, next := range []string{"stale", "resolver", "success", "canceled"} {
		t.Run(next, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			unknown := UnknownOutcome()
			calls := 0
			router := &Router{Node: "local", Directory: directoryFunc(func(context.Context, string, bool) (Route, error) {
				if calls > 0 && next == "resolver" {
					return Route{}, status.Error(codes.Unavailable, "read failed")
				}
				return Route{Node: "local", Address: "local"}, nil
			}), Local: func(context.Context, string, proto.Message) (proto.Message, error) {
				calls++
				if calls == 1 {
					if next == "canceled" {
						cancel()
					}
					return nil, unknown
				}
				if next == "success" {
					return &wire.ShardResult{}, nil
				}
				return nil, StaleOwner()
			}}
			request := envelope(&wire.ShardRequest{}, 1, &wire.ShardCommand{Kind: wire.ShardCommand_GET})
			response, err := callInterceptor(ctx, router, wire.ShardPersistence_Execute_FullMethodName, request)
			if next == "success" {
				if err != nil || response == nil || calls != 2 {
					t.Fatal(err, calls)
				}
			} else if err != unknown {
				t.Fatal("earlier ambiguity replaced", err)
			}
			if calls > 3 {
				t.Fatal("unbounded retry")
			}
		})
	}
}
