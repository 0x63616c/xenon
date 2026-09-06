package partitionservice_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/identity"
	p "github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/partitions/slatedb"
	"github.com/0x63616c/xenon/internal/persistence"
	"github.com/0x63616c/xenon/internal/registry"
	registrys3 "github.com/0x63616c/xenon/internal/registry/s3"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const ownerA identity.IncarnationID = "inc_0000000000000000000001"
const ownerB identity.IncarnationID = "inc_0000000000000000000002"
const partition identity.PartitionID = "prt_0000000000000000000001"
const key registry.Key = "cluster/control"

func TestPartitionNativeShardReplay(t *testing.T) {
	f := newFixture(t)
	a := f.service(ownerA, f.engine)
	bind := func(service *p.Service) *persistence.Service {
		f.ready(service)
		writer, attempt, handle, ready := service.Writer()
		if !ready {
			t.Fatal("missing borrowed writer")
		}
		check := func(ctx context.Context) error {
			r, err := f.store.Read(ctx, key)
			if err != nil {
				return err
			}
			s, err := cluster.DecodeControl(key, r, 1<<20)
			if err != nil {
				return err
			}
			current := s.Control().Partitions[partition]
			if !current.Ready || current.Desired.Incarnation != attempt.Incarnation || current.AssignmentRevision != attempt.AssignmentRevision || current.Generation != attempt.Generation || current.Reservation != attempt.Reservation {
				return cluster.ErrStaleControl
			}
			return nil
		}
		result, err := persistence.NewService(writer, partition, 1, check, func(err error) { service.ObserveFailure(handle, err) })
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	request := func(id string, data string) *wire.ShardRequest {
		command := &wire.ShardCommand{Kind: wire.ShardCommand_CREATE_OR_GET, ShardId: 7, RangeId: 11, Data: []byte(data), Encoding: 1}
		raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(command)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(raw)
		return &wire.ShardRequest{ProtocolVersion: 1, Partition: string(partition), OperationId: id, CommandSha256: digest[:], Command: command}
	}
	call := func(service *persistence.Service, req *wire.ShardRequest) (*wire.ShardResult, error) {
		ctx, cancel := context.WithTimeout(f.ctx, time.Duration(f.config.PhaseTimeout)*time.Second)
		defer cancel()
		return service.Execute(ctx, req)
	}
	q := request("op_0000000000000000000001", "acknowledged shard")
	before, err := call(bind(a), q)
	if err != nil || before.GetRangeId() != 11 {
		t.Fatalf("initial shard: %+v %v", before, err)
	}
	s := f.snapshot()
	w, err := s.Assign(ownerA, f.ids.transition(), map[identity.PartitionID]cluster.Owner{partition: owner(ownerB)})
	f.replace(s, w, err)
	b := f.service(ownerB, f.engine)
	a.Poll()
	current := bind(b)
	after, err := call(current, q)
	if err != nil || !proto.Equal(before, after) {
		t.Fatalf("new-owner replay: %+v %v", after, err)
	}
	if _, err = call(current, request(q.OperationId, "changed digest")); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("changed ID digest accepted: %v", err)
	}
	if _, err = call(current, request("op_0000000000000000000002", "second mutation")); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("replay capacity bypassed: %v", err)
	}
	written := f.ready(b)
	ctx, cancel := context.WithTimeout(f.ctx, time.Duration(f.config.PhaseTimeout)*time.Second)
	defer cancel()
	data, err := written.ReadDurable(ctx, p.ReadRequest{Keys: [][]byte{[]byte("v1/outcome_count"), []byte("v1/outcome/" + q.OperationId), []byte("v1/outcome_usage"), []byte("v1/shard/0000000007")}})
	if err != nil || len(data.Entries) != 4 {
		t.Fatalf("journal recovery: %+v %v", data, err)
	}
	if len(data.Entries[0].Value) != 8 || binary.BigEndian.Uint64(data.Entries[0].Value) != 1 || len(data.Entries[1].Value) == 0 || !strings.Contains(string(data.Entries[2].Value), `"entries":1`) {
		t.Fatalf("replay/accounting changed: %+v", data)
	}
	var recovered wire.StoredShard
	if err := proto.Unmarshal(data.Entries[3].Value, &recovered); err != nil || recovered.RangeId != 11 || string(recovered.Data) != "acknowledged shard" || recovered.Encoding != 1 {
		t.Fatalf("application shard recovery: %+v %v", &recovered, err)
	}
	t.Log("real shard outcome replayed after ownership movement without duplicate application; changed digest and capacity rejected")
}

type config struct {
	PhaseTimeout   int    `json:"phase_timeout_seconds"`
	CleanupTimeout int    `json:"cleanup_timeout_seconds"`
	PollMillis     int    `json:"poll_milliseconds"`
	StateKey       string `json:"state_key"`
	StateValue     string `json:"state_value"`
	OutcomeKey     string `json:"outcome_key"`
	OutcomeValue   string `json:"outcome_value"`
}
type sequence struct {
	mu sync.Mutex
	n  int
}

func (s *sequence) NewID(prefix string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.n++
	return fmt.Sprintf("%s_%022d", prefix, s.n), nil
}
func (s *sequence) transition() identity.TransitionID {
	v, _ := s.NewID("trn")
	return identity.TransitionID(v)
}

type fixture struct {
	t      *testing.T
	config config
	store  registry.Store
	engine p.Engine
	ids    *sequence
	ctx    context.Context
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	endpoint, backend := os.Getenv("AWS_ENDPOINT"), os.Getenv("XENON_ENGINE_STORE")
	if endpoint == "" || backend == "" {
		t.Skip("requires pinned partition-service runner")
	}
	body, err := os.ReadFile("case.json")
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, ids: &sequence{}}
	if err = json.Unmarshal(body, &f.config); err != nil {
		t.Fatal(err)
	}
	if f.config.PhaseTimeout <= 0 || f.config.CleanupTimeout <= 0 || f.config.PollMillis <= 0 {
		t.Fatal("invalid timing fixture")
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	f.ctx = ctx
	client := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(endpoint), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider(os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"), "")})
	bucket := strings.TrimPrefix(backend, "s3://")
	requestCtx, done := context.WithTimeout(ctx, time.Duration(f.config.PhaseTimeout)*time.Second)
	defer done()
	_, err = client.CreateBucket(requestCtx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	// Each test owns a separate object namespace in the one fresh run-owned bucket.
	if err != nil {
		var exists smithy.APIError
		if !errors.As(err, &exists) || exists.ErrorCode() != "BucketAlreadyOwnedByYou" {
			t.Fatal(err)
		}
	}
	f.store, err = registrys3.New(client, bucket, t.Name()+"/registry")
	if err != nil {
		t.Fatal(err)
	}
	f.engine, err = slatedb.New(backend)
	if err != nil {
		t.Fatal(err)
	}
	c := cluster.Control{Format: 1, Cluster: "clu_0000000000000000000001", Coordinator: cluster.Coordinator{Incarnation: ownerA, Generation: 1}, AssignmentRevision: 1, Partitions: map[identity.PartitionID]cluster.PartitionControl{partition: {Path: t.Name() + "/data", Desired: owner(ownerA), AssignmentRevision: 1}}}
	w, err := cluster.BootstrapWrite(key, f.ids.transition(), c, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.Create(requestCtx, key, w); err != nil {
		t.Fatal(err)
	}
	return f
}
func owner(i identity.IncarnationID) cluster.Owner {
	node := identity.NodeID("nod_0000000000000000000001")
	if i == ownerB {
		node = "nod_0000000000000000000002"
	}
	return cluster.Owner{Node: node, Incarnation: i, Address: "local-scenario"}
}
func (f *fixture) snapshot() cluster.Snapshot {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, time.Duration(f.config.PhaseTimeout)*time.Second)
	defer cancel()
	r, err := f.store.Read(ctx, key)
	if err != nil {
		f.t.Fatal(err)
	}
	s, err := cluster.DecodeControl(key, r, 1<<20)
	if err != nil {
		f.t.Fatal(err)
	}
	return s
}
func (f *fixture) replace(s cluster.Snapshot, w registry.Write, err error) {
	f.t.Helper()
	if err != nil {
		f.t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, time.Duration(f.config.PhaseTimeout)*time.Second)
	defer cancel()
	if _, err = f.store.Replace(ctx, key, s.Authority().Version, w); err != nil {
		f.t.Fatal(err)
	}
}
func (f *fixture) service(i identity.IncarnationID, engine p.Engine) *p.Service {
	f.t.Helper()
	s, err := p.NewService(f.ctx, p.ControllerConfig{Key: key, Partition: partition, Incarnation: i, MaxControlBytes: 1 << 20}, f.store, engine, f.ids)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() {
		s.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(f.config.CleanupTimeout)*time.Second)
		defer cancel()
		for {
			s.Poll()
			attempt, cancelAttempt := context.WithTimeout(ctx, time.Duration(f.config.PollMillis)*time.Millisecond)
			err := s.Drain(attempt)
			cancelAttempt()
			if err == nil {
				return
			}
			if ctx.Err() != nil {
				f.t.Errorf("service cleanup failed: %v; phase=%v pending=%+v", err, s.Snapshot().Phase, s.Snapshot().Pending())
				return
			}
		}
	})
	return s
}

// Real timers only bound this backend/native integration harness. They are not
// injected deterministic controller clocks or cross-process ownership authority.
func (f *fixture) wait(s *p.Service, label string, predicate func(p.State) bool) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, time.Duration(f.config.PhaseTimeout)*time.Second)
	defer cancel()
	for {
		s.Poll()
		state := s.Snapshot()
		if predicate(state) {
			f.t.Log(label)
			return
		}
		timer := time.NewTimer(time.Duration(f.config.PollMillis) * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			f.t.Fatalf("%s: %v phase=%v pending=%+v last=%v", label, ctx.Err(), state.Phase, state.Pending(), state.LastError)
		}
	}
}

type borrowedWriter struct {
	p.Writer
	report func(error)
}

func (f *fixture) ready(s *p.Service) borrowedWriter {
	f.t.Helper()
	f.wait(s, "writer ready", func(st p.State) bool { return st.Phase == p.Ready })
	w, a, handle, ok := s.Writer()
	if !ok || a.Partition != partition {
		f.t.Fatal("ready handle missing")
	}
	c := f.snapshot().Control().Partitions[partition]
	if !c.Ready || c.Reservation != a.Reservation || c.Generation != a.Generation || c.AssignmentRevision != a.AssignmentRevision {
		f.t.Fatal("ready tuple differs from durable registry")
	}
	return borrowedWriter{Writer: w, report: func(err error) { s.ObserveFailure(handle, err) }}
}
func (f *fixture) write(w borrowedWriter) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, time.Duration(f.config.PhaseTimeout)*time.Second)
	defer cancel()
	tx, err := w.Begin(ctx)
	if err != nil {
		w.report(err)
		f.t.Fatal(err)
	}
	defer tx.Abort()
	for _, pair := range [][2]string{{f.config.StateKey, f.config.StateValue}, {f.config.OutcomeKey, f.config.OutcomeValue}} {
		k, v := pair[0], pair[1]
		if err = tx.Put([]byte(k), []byte(v)); err != nil {
			w.report(err)
			f.t.Fatal(err)
		}
	}
	receipt, err := tx.Commit(ctx)
	if err != nil {
		w.report(err)
		f.t.Fatal(err)
	}
	if err = w.AwaitDurable(ctx, receipt); err != nil {
		w.report(err)
		f.t.Fatal(err)
	}
	f.t.Log("acknowledged atomic state and outcome after AwaitDurable")
}
func (f *fixture) read(w borrowedWriter) {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, time.Duration(f.config.PhaseTimeout)*time.Second)
	defer cancel()
	for _, pair := range [][2]string{{f.config.StateKey, f.config.StateValue}, {f.config.OutcomeKey, f.config.OutcomeValue}} {
		k, v := pair[0], pair[1]
		r, err := w.ReadDurable(ctx, p.ReadRequest{Keys: [][]byte{[]byte(k)}})
		if err != nil || len(r.Entries) != 1 || string(r.Entries[0].Value) != v {
			w.report(err)
			f.t.Fatalf("acknowledged %q missing: %+v %v", k, r, err)
		}
	}
}
func (f *fixture) move() {
	f.t.Helper()
	s := f.snapshot()
	w, err := s.ChangeCoordinator(f.ids.transition(), cluster.CoordinatorChange{Expected: s.Authority().Version, Incarnation: ownerB, Generation: 2})
	f.replace(s, w, err)
	s = f.snapshot()
	w, err = s.Assign(ownerB, f.ids.transition(), map[identity.PartitionID]cluster.Owner{partition: owner(ownerB)})
	f.replace(s, w, err)
}
func TestPartitionNativeAcknowledgedMovement(t *testing.T) {
	f := newFixture(t)
	a := f.service(ownerA, f.engine)
	f.write(f.ready(a))
	// An already prepared old leader proposal must fail its actual S3 CAS after replacement.
	before := f.snapshot()
	stale, err := before.Assign(ownerA, f.ids.transition(), map[identity.PartitionID]cluster.Owner{partition: owner(ownerB)})
	if err != nil {
		t.Fatal(err)
	}
	f.move()
	_, err = f.store.Replace(f.ctx, key, before.Authority().Version, stale)
	var conflict *registry.Conflict
	if !errors.As(err, &conflict) {
		t.Fatalf("old plan was not rejected: %v", err)
	}
	f.wait(a, "old writer drained after move", func(s p.State) bool { return s.Phase == p.Idle && s.Handle() == 0 })
	b := f.service(ownerB, f.engine)
	f.read(f.ready(b))
	t.Log("new owner recovered both acknowledged values")
}

// delayCompletion holds delivery only, after the actual native Open completes.
// It does not substitute registry, transaction, fencing or durability semantics.
type delayCompletion struct {
	p.Engine
	completed chan struct{}
	release   chan struct{}
	once      sync.Once
}

func (e *delayCompletion) unblock() { e.once.Do(func() { close(e.release) }) }
func (e *delayCompletion) Open(ctx context.Context, r p.OpenRequest) (p.Writer, error) {
	w, err := e.Engine.Open(ctx, r)
	close(e.completed)
	<-e.release
	return w, err
}
func TestPartitionNativeObsoleteOpenCompletion(t *testing.T) {
	f := newFixture(t)
	delayed := &delayCompletion{Engine: f.engine, completed: make(chan struct{}), release: make(chan struct{})}
	a := f.service(ownerA, delayed)
	t.Cleanup(delayed.unblock)
	a.Poll()
	select {
	case <-delayed.completed:
	case <-time.After(time.Duration(f.config.PhaseTimeout) * time.Second):
		t.Fatal("native open never completed")
	}
	f.move()
	f.wait(a, "old open retained while reassigned", func(s p.State) bool { return s.Phase == p.Opening && s.Handle() == 0 })
	b := f.service(ownerB, f.engine)
	w := f.ready(b)
	f.write(w)
	delayed.unblock()
	f.wait(a, "obsolete native handle closed", func(s p.State) bool { return s.Phase == p.Idle && s.Handle() == 0 })
	if _, _, _, ok := a.Writer(); ok {
		t.Fatal("obsolete completion activated")
	}
	f.read(w)
	current := f.snapshot().Control().Partitions[partition]
	if !current.Ready || current.Desired.Incarnation != ownerB {
		t.Fatal("obsolete completion replaced current readiness")
	}
	t.Log("late old completion closed without losing current owner acknowledgments")
}

// delayStart schedules the real Open after a replacement writer has committed.
// This is distinct from holding delivery of an already-opened native handle.
type delayStart struct {
	p.Engine
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (e *delayStart) unblock() { e.once.Do(func() { close(e.release) }) }
func (e *delayStart) Open(ctx context.Context, request p.OpenRequest) (p.Writer, error) {
	close(e.entered)
	<-e.release
	return e.Engine.Open(ctx, request)
}
func TestPartitionNativeLateOpenFencesCurrent(t *testing.T) {
	f := newFixture(t)
	delayed := &delayStart{Engine: f.engine, entered: make(chan struct{}), release: make(chan struct{})}
	a := f.service(ownerA, delayed)
	t.Cleanup(delayed.unblock)
	a.Poll()
	select {
	case <-delayed.entered:
	case <-time.After(time.Duration(f.config.PhaseTimeout) * time.Second):
		t.Fatal("old open was not dispatched")
	}
	f.move()
	b := f.service(ownerB, f.engine)
	oldB := f.ready(b)
	f.write(oldB)
	delayed.unblock()
	f.wait(a, "obsolete actual open closed after fencing replacement", func(state p.State) bool { return state.Phase == p.Idle && state.Handle() == 0 })
	ctx, cancel := context.WithTimeout(f.ctx, time.Duration(f.config.PhaseTimeout)*time.Second)
	defer cancel()
	_, err := oldB.ReadDurable(ctx, p.ReadRequest{Keys: [][]byte{[]byte(f.config.StateKey)}})
	if !errors.Is(err, p.ErrFenced) && !errors.Is(err, p.ErrRetired) {
		t.Fatalf("late open did not retire former B writer: %v", err)
	}
	oldB.report(err)
	f.read(f.ready(b))
	current := f.snapshot().Control().Partitions[partition]
	if !current.Ready || current.Desired.Incarnation != ownerB || current.Generation < 3 {
		t.Fatalf("current owner did not recover after finite obsolete open: %+v", current)
	}
	t.Log("late actual native open fenced replacement; obsolete opener retired and current owner recovered acknowledged state")
}
