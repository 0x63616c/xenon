package simulation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/cluster"
	ids "github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/persistence"
	"github.com/0x63616c/xenon/internal/registry"
	"github.com/0x63616c/xenon/internal/routing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type seamResolver struct {
	calls int
	mode  string
	last  string
}

func (r *seamResolver) Resolve(_ context.Context, _ string, refresh bool) (routing.Route, error) {
	r.calls++
	if r.mode == "route_refresh" && !refresh {
		r.last = "stale"
		return routing.Route{Node: "stale", Address: "same:8080"}, nil
	}
	r.last = "current"
	return routing.Route{Node: "current", Address: "same:8080"}, nil
}

type seamDisk struct {
	durable               map[string][]byte
	commits, applications int
}
type seamTx struct {
	disk       *seamDisk
	staged     map[string][]byte
	failCommit bool
}

func (d *seamDisk) begin(fail bool) *seamTx {
	return &seamTx{disk: d, staged: seamCopyState(d.durable), failCommit: fail}
}

func seamCopyState(in map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(in))
	for key, value := range in {
		out[key] = bytes.Clone(value)
	}
	return out
}
func (t *seamTx) effects() persistence.ReplayEffects {
	return persistence.ReplayEffects{
		Get: func(k string) ([]byte, error) { return bytes.Clone(t.staged[k]), nil },
		Put: func(k string, v []byte) error { t.staged[k] = bytes.Clone(v); return nil },
		Apply: func() (*wire.StoredOutcome, error) {
			t.disk.applications++
			return &wire.StoredOutcome{Result: &wire.StoredOutcome_ShardResult{ShardResult: &wire.ShardResult{Data: []byte("ok")}}}, nil
		},
		Account: func(*wire.StoredOutcome, int) error { return nil },
		Belongs: func(o *wire.StoredOutcome) bool { return o.GetShardResult() != nil },
		Commit: func(*wire.StoredOutcome) error {
			if t.failCommit {
				return status.Error(codes.Unavailable, "crash before commit")
			}
			t.disk.durable = seamCopyState(t.staged)
			t.disk.commits++
			return nil
		},
	}
}

// runProductionSeam schedules failures around production Router and RunReplay.
// The harness controls callbacks only; routing, retry, digest and exactly-once
// decisions remain in their production packages.
func runProductionSeam(mode string) error {
	if mode == "overlapping_join" {
		return runOverlappingJoinSeam()
	}
	if len(mode) > len("partition_") && mode[:len("partition_")] == "partition_" {
		return runPartitionTransitionSeam(mode)
	}
	negative := ""
	if len(mode) > len("negative_") && mode[:len("negative_")] == "negative_" {
		negative, mode = mode[len("negative_"):], ""
	}
	disk := &seamDisk{durable: map[string][]byte{}}
	digest := sha256.Sum256([]byte("dst-operation"))
	request := &wire.ShardRequest{ProtocolVersion: 1, Partition: "p", OperationId: "op_0000000000000000000001", CommandSha256: digest[:]}
	deliver := func() (*wire.ShardResult, error) {
		out, err := persistence.RunReplay(disk.begin(false).effects(), request.OperationId, request.CommandSha256, 10)
		if err != nil {
			return nil, err
		}
		return proto.Clone(out.GetShardResult()).(*wire.ShardResult), nil
	}
	if mode == "crash_before_commit" {
		if _, err := persistence.RunReplay(disk.begin(true).effects(), request.OperationId, request.CommandSha256, 10); status.Code(err) != codes.Unavailable {
			return errors.New("crash cut did not interrupt commit")
		}
		result, err := deliver()
		if err != nil || !bytes.Equal(result.Data, []byte("ok")) || disk.applications != 2 || disk.commits != 1 {
			return errors.New("crash-before-commit recovery failed")
		}
		return nil
	}
	resolver := &seamResolver{mode: mode}
	router := &routing.Router{Node: "origin", Directory: resolver, Local: func(context.Context, string, proto.Message) (proto.Message, error) { return deliver() }}
	invocations := 0
	router.Invoke = func(_ context.Context, address, _ string, _, response proto.Message) error {
		invocations++
		if mode == "route_refresh" && invocations == 1 {
			return routing.StaleOwner()
		}
		if mode == "drop_then_retry" && invocations == 1 {
			return status.Error(codes.Unavailable, "dropped before delivery")
		}
		result, err := deliver()
		if err != nil {
			return err
		}
		proto.Merge(response, result)
		if mode == "duplicate_delivery" {
			if _, err = deliver(); err != nil {
				return err
			}
		}
		if mode == "response_lost_after_commit" && invocations == 1 {
			return status.Error(codes.Unavailable, "response lost after commit")
		}
		_ = address
		return nil
	}
	if mode == "closed_admission" {
		_ = router.Close()
	}
	invoke := router.Interceptor(func(string) proto.Message { return new(wire.ShardResult) })
	ctx := seamDeadlineContext{Context: context.Background(), deadline: time.Unix(2_000_000_000, 0)}
	call := func() error {
		_, err := invoke(ctx, request, &grpc.UnaryServerInfo{FullMethod: wire.ShardPersistence_Execute_FullMethodName}, nil)
		return err
	}
	first := call()
	switch mode {
	case "drop_then_retry", "response_lost_after_commit":
		if status.Code(first) != codes.Unavailable {
			return errors.New("dropped delivery was not reported")
		}
		if err := call(); err != nil {
			return err
		}
	case "closed_admission":
		if status.Code(first) != codes.Unavailable || disk.applications != 0 {
			return errors.New("closed router admitted work")
		}
		return nil
	default:
		if first != nil {
			return first
		}
	}
	observation := productionSeamObservation{disk: disk, request: request, expectedDigest: digest, acknowledged: true, acknowledgedNode: resolver.last}
	switch negative {
	case "acknowledged_write_loss":
		delete(disk.durable, "v1/outcome_count")
	case "duplicate_application":
		disk.applications++
	case "changed_operation_digest":
		request.CommandSha256[0] ^= 0xff
	case "stale_owner_acknowledgment":
		observation.acknowledgedNode = "stale"
	case "failed_healthy_settle":
		disk.commits = 0
	case "":
	default:
		return fmt.Errorf("unknown production seam negative control %q", negative)
	}
	if err := checkProductionSeam(observation); err != nil {
		return err
	}
	if mode == "route_refresh" && resolver.calls != 2 {
		return errors.New("stale route did not refresh")
	}
	return nil
}

type productionSeamObservation struct {
	disk             *seamDisk
	request          *wire.ShardRequest
	expectedDigest   [32]byte
	acknowledged     bool
	acknowledgedNode string
}

// checkProductionSeam is the independent assertion path used by every routing
// production-seam DST case. Negative controls corrupt its observed inputs; they
// do not call a separate test-only oracle.
func checkProductionSeam(o productionSeamObservation) error {
	fail := func(invariant, mechanism, diagnostic string) error {
		return checkerFailure(invariant, mechanism, diagnostic)
	}
	count := o.disk.durable["v1/outcome_count"]
	if o.acknowledged && (len(count) != 8 || binary.BigEndian.Uint64(count) != 1) {
		return fail("acknowledged_write", "missing_after_recovery", "acknowledged outcome is absent after recovery")
	}
	if o.disk.applications != 1 {
		return fail("application", "duplicate", "logical operation applied more than once")
	}
	if !bytes.Equal(o.request.CommandSha256, o.expectedDigest[:]) {
		return fail("operation_digest", "changed", "operation digest changed across routing")
	}
	if o.acknowledged && o.acknowledgedNode != "current" {
		return fail("authority", "stale_acknowledgment", "stale owner acknowledged routed work")
	}
	if o.disk.commits < 1 {
		return fail("progress", "stalled", "healthy owner did not make durable progress")
	}
	return nil
}

const seamInc ids.IncarnationID = "inc_0000000000000000000001"
const seamPart ids.PartitionID = "prt_0000000000000000000001"
const seamKey registry.Key = "cluster/control"

func seamTransition(n int) ids.TransitionID {
	return ids.TransitionID(fmt.Sprintf("trn_%022d", n))
}

func seamPartitionFixture() (partitions.State, registry.Record, error) {
	layout := cluster.Layout{Version: 1, Placement: cluster.DefaultPlacementConfig(), Partitions: []cluster.PhysicalPartition{{LogicalName: "partition-a", ID: seamPart, Path: "data/a"}}}
	digest, err := layout.Digest()
	if err != nil {
		return partitions.State{}, registry.Record{}, err
	}
	state, err := partitions.NewState(partitions.ControllerConfig{ExpectedLayoutDigest: digest, Key: seamKey, Partition: seamPart, Incarnation: seamInc, MaxControlBytes: 1 << 20})
	if err != nil {
		return partitions.State{}, registry.Record{}, err
	}
	owner := cluster.Owner{Node: "nod_0000000000000000000001", Incarnation: seamInc, Address: "node:8080"}
	write, err := cluster.BootstrapWrite(seamKey, seamTransition(1), cluster.Control{Format: cluster.ControlFormat, Layout: &layout, Cluster: "clu_0000000000000000000001", Coordinator: cluster.Coordinator{Incarnation: seamInc, Generation: 1}, AssignmentRevision: 1, Partitions: map[ids.PartitionID]cluster.PartitionControl{seamPart: {Path: "data/a", Desired: owner, AssignmentRevision: 1}}}, 1<<20)
	if err != nil {
		return partitions.State{}, registry.Record{}, err
	}
	body, err := registry.Encode(seamKey, "", write)
	return state, registry.Record{Body: body, Version: registry.Version(write.Transition)}, err
}

func seamStep(state partitions.State, event partitions.Event) (partitions.State, []partitions.Effect) {
	event.Incarnation = seamInc
	return partitions.Step(state, event)
}

func seamOnly(effects []partitions.Effect, kind partitions.EffectKind) (partitions.Effect, error) {
	if len(effects) != 1 || effects[0].Kind != kind {
		return partitions.Effect{}, fmt.Errorf("want effect %v, got %+v", kind, effects)
	}
	return effects[0], nil
}

func seamPublished(effect partitions.Effect) (registry.Record, error) {
	body, err := registry.Encode(seamKey, effect.Expected, effect.Write)
	return registry.Record{Body: body, Version: registry.Version(effect.Write.Transition)}, err
}

// runPartitionTransitionSeam places failures at production controller effect
// boundaries. The harness supplies completions; partitions.Step retains every
// reservation, Open, Ready and cleanup ordering decision.
func runPartitionTransitionSeam(mode string) error {
	state, record, err := seamPartitionFixture()
	if err != nil {
		return err
	}
	state, effects := seamStep(state, partitions.Event{Kind: partitions.Poll})
	read, err := seamOnly(effects, partitions.ReadControl)
	if err != nil {
		return err
	}
	if mode == "partition_before_reservation" {
		state, effects = seamStep(state, partitions.Event{Kind: partitions.ReadCompleted, Effect: read.ID, Err: errors.New("read failed")})
		if len(effects) != 0 || state.LastError == nil || state.Phase != partitions.Idle {
			return errors.New("failure before reservation changed authority")
		}
		return nil
	}
	state, effects = seamStep(state, partitions.Event{Kind: partitions.ReadCompleted, Effect: read.ID, Record: record, Transition: seamTransition(2)})
	reserve, err := seamOnly(effects, partitions.PublishControl)
	if err != nil {
		return err
	}
	if mode == "partition_after_reservation" {
		state, effects = seamStep(state, partitions.Event{Kind: partitions.PublishCompleted, Effect: reserve.ID, Err: &registry.UnknownOutcome{Key: seamKey, Transition: reserve.Write.Transition, Cause: context.Canceled}})
		_, err = seamOnly(effects, partitions.ReadControl)
		if err != nil || state.LastUnknown == nil {
			return errors.New("ambiguous reservation was not retained")
		}
		return nil
	}
	record, err = seamPublished(reserve)
	if err != nil {
		return err
	}
	state, effects = seamStep(state, partitions.Event{Kind: partitions.PublishCompleted, Effect: reserve.ID, Record: record})
	open, err := seamOnly(effects, partitions.OpenEngine)
	if err != nil {
		return err
	}
	if mode == "partition_before_open" {
		state, effects = seamStep(state, partitions.Event{Kind: partitions.Stop})
		if len(effects) != 0 {
			return errors.New("stop raced ahead of pending open")
		}
		_, effects = seamStep(state, partitions.Event{Kind: partitions.OpenCompleted, Effect: open.ID, HasWriter: true})
		_, err = seamOnly(effects, partitions.CloseEngine)
		return err
	}
	if mode == "partition_after_open" {
		_, effects = seamStep(state, partitions.Event{Kind: partitions.OpenCompleted, Effect: open.ID, HasWriter: true, Err: errors.New("open failed after allocating writer")})
		_, err = seamOnly(effects, partitions.CloseEngine)
		return err
	}
	state, effects = seamStep(state, partitions.Event{Kind: partitions.OpenCompleted, Effect: open.ID, HasWriter: true})
	read, err = seamOnly(effects, partitions.ReadControl)
	if err != nil {
		return err
	}
	if mode == "partition_before_ready" {
		state, effects = seamStep(state, partitions.Event{Kind: partitions.ReadCompleted, Effect: read.ID, Err: errors.New("read failed")})
		if len(effects) != 0 || state.Handle() != open.ID || state.Phase != partitions.Activating {
			return errors.New("failure before Ready discarded live writer")
		}
		return nil
	}
	state, effects = seamStep(state, partitions.Event{Kind: partitions.ReadCompleted, Effect: read.ID, Record: record, Transition: seamTransition(3)})
	ready, err := seamOnly(effects, partitions.PublishControl)
	if err != nil {
		return err
	}
	if mode != "partition_after_ready" {
		return fmt.Errorf("unknown partition seam %q", mode)
	}
	state, effects = seamStep(state, partitions.Event{Kind: partitions.PublishCompleted, Effect: ready.ID, Err: &registry.UnknownOutcome{Key: seamKey, Transition: ready.Write.Transition, Cause: context.Canceled}})
	_, err = seamOnly(effects, partitions.ReadControl)
	if err != nil || state.LastUnknown == nil || state.Handle() != open.ID {
		return errors.New("ambiguous Ready discarded live writer")
	}
	return nil
}

type seamDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c seamDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }
