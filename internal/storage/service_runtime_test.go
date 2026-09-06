package storage

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/0x63616c/xenon/internal/agent"
	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/registry"
	"github.com/0x63616c/xenon/internal/registry/filesystem"
	"go.temporal.io/api/serviceerror"
)

func TestServiceRuntimeRefusesLegacyAndStopIsIdempotent(t *testing.T) {
	r := NewServiceRuntime(agent.Config{})
	if err := r.Start(context.Background()); !errors.Is(err, ErrLegacyPrefix) {
		t.Fatal(err)
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Stop(canceled); err != nil {
		t.Fatal("completed stop lost idempotence", err)
	}
	if r.Diagnostics().Started {
		t.Fatal("legacy runtime started")
	}
}

type boundedTestStore struct {
	deadline time.Time
	write    registry.Write
}

func (s *boundedTestStore) Read(ctx context.Context, k registry.Key) (registry.Record, error) {
	s.deadline, _ = ctx.Deadline()
	return registry.Record{}, &registry.Unavailable{Key: k}
}
func (s *boundedTestStore) Create(ctx context.Context, k registry.Key, w registry.Write) (registry.Record, error) {
	s.deadline, _ = ctx.Deadline()
	s.write = w
	return registry.Record{}, &registry.UnknownOutcome{Key: k, Transition: w.Transition}
}
func (s *boundedTestStore) Replace(ctx context.Context, k registry.Key, v registry.Version, w registry.Write) (registry.Record, error) {
	return s.Create(ctx, k, w)
}
func TestHostRegistryBudgetsPreserveUnknownAndEarlierDeadline(t *testing.T) {
	inner := &boundedTestStore{}
	store := boundedRegistry{Store: inner, timeout: time.Minute}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	w := registry.Write{Transition: "trn_0000000000000000000001", Body: []byte("exact")}
	_, err := store.Replace(ctx, "control", "v1", w)
	var unknown *registry.UnknownOutcome
	if !errors.As(err, &unknown) || unknown.Transition != w.Transition || !inner.deadline.Equal(deadline) {
		t.Fatal("budget or ambiguity altered", err, inner.deadline)
	}
	_, _ = store.Read(context.Background(), "control")
	if inner.deadline.IsZero() {
		t.Fatal("registry call lacks host budget")
	}
}

type retainedOpen struct {
	entered, release chan struct{}
	calls            atomic.Int32
}

func (e *retainedOpen) Open(context.Context, partitions.OpenRequest) (partitions.Writer, error) {
	e.calls.Add(1)
	close(e.entered)
	<-e.release
	return nil, partitions.ErrFenced
}
func TestHostStopRetainsPendingOpenAndReportsProcessExit(t *testing.T) {
	store, err := filesystem.New(filesystem.Config{Directory: t.TempDir(), MaxRecordBytes: 1 << 20, Wait: func(ctx context.Context) error { return ctx.Err() }})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	const inc identity.IncarnationID = "inc_0000000000000000000001"
	const part identity.PartitionID = "prt_0000000000000000000001"
	layout := cluster.Layout{Version: 1, Placement: cluster.DefaultPlacementConfig(), Partitions: []cluster.PhysicalPartition{{LogicalName: "global", ID: part, Path: "test/data/global"}}}
	digest, _ := layout.Digest()
	control := cluster.Control{Format: cluster.ControlFormat, Cluster: "clu_0000000000000000000001", Layout: &layout, Coordinator: cluster.Coordinator{Incarnation: inc, Generation: 1}, AssignmentRevision: 1, Partitions: map[identity.PartitionID]cluster.PartitionControl{part: {Path: layout.Partitions[0].Path, Desired: cluster.Owner{Node: "nod_0000000000000000000001", Incarnation: inc, Address: "a:80"}, AssignmentRevision: 1}}}
	w, err := cluster.BootstrapWrite("control", "trn_0000000000000000000001", control, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.Create(context.Background(), "control", w); err != nil {
		t.Fatal(err)
	}
	engine := &retainedOpen{entered: make(chan struct{}), release: make(chan struct{})}
	driver, err := partitions.NewService(context.Background(), partitions.ControllerConfig{Key: "control", Partition: part, Incarnation: inc, MaxControlBytes: 1 << 20, ExpectedLayoutDigest: digest}, store, engine, identity.Generator{})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticks := time.NewTicker(time.Millisecond)
	defer ticks.Stop()
	waiting := true
	for waiting {
		driver.Poll()
		select {
		case <-engine.entered:
			waiting = false
		case <-ticks.C:
		case <-deadline.C:
			t.Fatal("reservation did not reach controlled open")
		}
	}
	coordinator, err := cluster.NewService(context.Background(), cluster.ControllerConfig{Key: "control", Incarnation: inc, ExpectedLayoutDigest: digest, MaxControlBytes: 1 << 20, RenewalInterval: time.Second, SuspectAfter: 5 * time.Second}, store, identity.Generator{})
	if err != nil {
		t.Fatal(err)
	}
	r := NewServiceRuntime(agent.Config{ServiceStorage: &agent.ServiceStorageConfig{Layout: layout}})
	r.started = true
	r.startAttempt = true
	r.coordinator = coordinator
	r.partitions = map[identity.PartitionID]*partitions.Service{part: driver}
	close(r.startDone)
	close(r.loopsDone)
	stop, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	err = r.Stop(stop)
	cancel()
	if !errors.Is(err, agent.ErrProcessExitRequired) || len(driver.Snapshot().Pending()) == 0 || engine.calls.Load() != 1 {
		t.Fatal("pending open was freed or forgotten", err)
	}
	close(engine.release)
	drain, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = r.Stop(drain); err != nil {
		t.Fatal(err)
	}
	if len(driver.Snapshot().Pending()) != 0 {
		t.Fatal("late completion not drained")
	}
}

func TestReadinessRetriesChildBudgetWithinOriginalParentDeadline(t *testing.T) {
	for _, transient := range []error{context.DeadlineExceeded, serviceerror.NewUnavailable("owner recovering"), serviceerror.NewDeadlineExceeded("child invocation expired")} {
		t.Run(transient.Error(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			deadline, _ := ctx.Deadline()
			calls := 0
			err := waitForReadiness(ctx, time.Millisecond, func(observed context.Context) error {
				actual, _ := observed.Deadline()
				if !actual.Equal(deadline) {
					t.Fatal("extended parent deadline")
				}
				calls++
				if calls == 1 {
					if transient == context.DeadlineExceeded {
						child, stop := context.WithTimeout(observed, time.Millisecond)
						defer stop()
						<-child.Done()
						if observed.Err() != nil {
							t.Fatal("parent expired with child")
						}
						return child.Err()
					}
					return transient
				}
				return nil
			})
			if err != nil || calls != 2 {
				t.Fatal("completed child failure escaped readiness early", err, calls)
			}
		})
	}
}

func TestReadinessPreservesParentBudgetAndPermanentFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	calls := 0
	err := waitForReadiness(ctx, time.Millisecond, func(context.Context) error { calls++; return serviceerror.NewUnavailable("owner recovering") })
	if !errors.Is(err, context.DeadlineExceeded) || calls < 2 {
		t.Fatal(err, calls)
	}
	for _, permanent := range []error{serviceerror.NewInvalidArgument("layout mismatch"), agent.Permanent(serviceerror.NewUnavailable("listener stopped")), context.Canceled} {
		calls = 0
		err = waitForReadiness(context.Background(), time.Millisecond, func(context.Context) error { calls++; return permanent })
		if !errors.Is(err, permanent) || calls != 1 {
			t.Fatal("permanent failure retried", err, calls)
		}
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if err = waitForReadiness(canceled, time.Millisecond, func(context.Context) error { t.Fatal("observed after cancellation"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestReadinessRejectsSuccessAfterParentEnds(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		if deadline {
			cancel()
			ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
		}
		calls := 0
		err := waitForReadiness(ctx, time.Millisecond, func(context.Context) error {
			calls++
			if !deadline {
				cancel()
			}
			<-ctx.Done()
			return nil
		})
		cancel()
		if !errors.Is(err, ctx.Err()) || calls != 1 {
			t.Fatal("expired observation reported success", err, calls)
		}
	}
}
