// Package node owns complete persistence operations and embedded SlateDB resources.
package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/0x63616c/xenon/internal/processcut"
	"github.com/google/uuid"
	"regexp"
	"sync/atomic"
	"time"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	native "slatedb.io/slatedb-go/uniffi"
)

var operationID = regexp.MustCompile(`^[a-zA-Z0-9-]{1,128}$`)

type Config struct {
	Partition        string
	MaxOutcomes      uint64
	OperationTimeout time.Duration
	AdmissionTimeout time.Duration
	MaxAdmitted      int
	// Authority runs under the partition gate before every operation. When set,
	// all returned results also require a nonempty durable native barrier.
	Authority func(context.Context) error
}

func DefaultConfig(partition string) Config {
	return Config{Partition: partition, MaxOutcomes: 10000, OperationTimeout: 20 * time.Second, AdmissionTimeout: 5 * time.Second, MaxAdmitted: 64}
}

// Owner never destroys a native resource while an FFI call can still reference it.
// A timeout quarantines the partition; completion drains resources but does not
// reopen admission. Process replacement is required to recover service.
type Owner struct {
	wire.UnimplementedShardPersistenceServer
	db          *native.Db
	config      Config
	gate        chan struct{}
	admitted    chan struct{}
	quarantined atomic.Bool
	active      atomic.Int32
	destroyed   atomic.Bool
	// Only the gate-owning native worker accesses cut/cutReady.
	cut      *processcut.Invocation
	cutReady bool
}

func NewOwner(db *native.Db, c Config) (*Owner, error) {
	if db == nil || c.Partition == "" || c.MaxOutcomes == 0 || c.OperationTimeout <= 0 || c.AdmissionTimeout <= 0 || c.MaxAdmitted <= 0 {
		return nil, fmt.Errorf("invalid owner configuration")
	}
	return &Owner{db: db, config: c, gate: make(chan struct{}, 1), admitted: make(chan struct{}, c.MaxAdmitted)}, nil
}
func (o *Owner) Retire()                       { o.quarantined.Store(true) }
func (o *Owner) Quarantined() bool             { return o.quarantined.Load() }
func (o *Owner) ActiveNativeOperations() int32 { return o.active.Load() }

// Close requires drained application work. It never frees the database while a
// native call remains active. Shutdown itself may block; callers supervising an
// unresponsive process must terminate it rather than call Destroy prematurely.
func (o *Owner) Close(ctx context.Context) error {
	o.quarantined.Store(true)
	timer := time.NewTimer(o.config.AdmissionTimeout)
	defer timer.Stop()
	select {
	case o.gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("native operation still active; retain owner until drained or replace process")
	}
	if o.destroyed.Load() {
		<-o.gate
		return nil
	}
	done := make(chan error, 1)
	o.active.Add(1)
	go func() {
		err := o.db.Shutdown()
		o.db.Destroy()
		o.destroyed.Store(true)
		o.active.Add(-1)
		<-o.gate
		done <- err
	}()
	deadline := time.NewTimer(o.config.OperationTimeout)
	defer deadline.Stop()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-deadline.C:
		return fmt.Errorf("native shutdown still active; resources retained")
	}
}

func (o *Owner) Execute(ctx context.Context, request *wire.ShardRequest) (*wire.ShardResult, error) {
	if request == nil || request.ProtocolVersion != 1 || request.Partition != o.config.Partition || !operationID.MatchString(request.OperationId) || request.Command == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid protocol, partition or operation identity")
	}
	// Own immutable request bytes before an admitted goroutine outlives the RPC.
	request = proto.Clone(request).(*wire.ShardRequest)
	command := request.Command
	if command.ShardId < 0 || len(command.Data) > 1024*1024 || command.Kind < wire.ShardCommand_GET || command.Kind > wire.ShardCommand_ASSERT {
		return nil, status.Error(codes.InvalidArgument, "invalid shard command")
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(command)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid command encoding")
	}
	digest := sha256.Sum256(encoded)
	if !bytes.Equal(digest[:], request.CommandSha256) {
		return nil, status.Error(codes.InvalidArgument, "command digest mismatch")
	}
	encodedResult, err := o.Run(ctx, func(_ *native.Db) ([]byte, error) {
		result, err := o.apply(request)
		if err != nil {
			return nil, err
		}
		return proto.Marshal(result)
	})
	if err != nil {
		return nil, err
	}
	result := &wire.ShardResult{}
	if err = proto.Unmarshal(encodedResult, result); err != nil {
		return nil, status.Error(codes.Internal, "invalid internal result encoding")
	}
	return result, nil
}

// Run is the whole-operation seam shared by local store handlers. All native calls
// and their Shutdown/Destroy actions must remain inside operation. The callback
// may outlive ctx; it must never export native handles. Logical persisted errors
// belong in the encoded result, while Unavailable means uncertain native state.
func (o *Owner) Run(ctx context.Context, operation func(*native.Db) ([]byte, error)) ([]byte, error) {
	if operation == nil {
		return nil, status.Error(codes.InvalidArgument, "nil operation")
	}
	if o.quarantined.Load() {
		return nil, status.Error(codes.Unavailable, "partition requires recovery")
	}
	select {
	case o.admitted <- struct{}{}:
	default:
		return nil, status.Error(codes.ResourceExhausted, "partition admission full")
	}
	defer func() { <-o.admitted }()
	timer := time.NewTimer(o.config.AdmissionTimeout)
	defer timer.Stop()
	select {
	case o.gate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, status.Error(codes.ResourceExhausted, "partition admission timeout")
	}
	if o.quarantined.Load() {
		<-o.gate
		return nil, status.Error(codes.Unavailable, "partition requires recovery")
	}
	// After acquiring the gate, admitted ownership moves to the native worker.
	// A separate bounded slot above caps waiting RPCs; the gate caps native work.
	type outcome struct {
		result []byte
		err    error
	}
	done := make(chan outcome, 1)
	decision := make(chan struct{})
	defer close(decision)
	o.active.Add(1)
	go func() {
		o.cut = processcut.FromContext(ctx)
		o.cutReady = false

		var result []byte
		var err error
		if o.config.Authority != nil {
			err = o.config.Authority(ctx)
		}
		if err != nil {
			err = status.Errorf(codes.Unavailable, "ownership authority unavailable: %v", err)
		} else {
			result, err = operation(o.db)
			if err == nil && o.config.Authority != nil {
				err = o.authorityBarrier()
			}
		}
		if err == nil && o.cutReady {
			err = o.cutStage(processcut.BeforeReply)
		}
		if status.Code(err) == codes.Unavailable {
			o.quarantined.Store(true)
		}
		o.cut = nil
		o.cutReady = false
		o.active.Add(-1)
		// Publish completion before releasing gate, preventing timeout/admission races.
		done <- outcome{result, err}
		<-decision
		<-o.gate
	}()
	deadline := time.NewTimer(o.config.OperationTimeout)
	defer deadline.Stop()
	select {
	case value := <-done:
		return value.result, value.err
	case <-ctx.Done():
		o.quarantined.Store(true)
		return nil, ctx.Err()
	case <-deadline.C:
		o.quarantined.Store(true)
		return nil, status.Error(codes.Unavailable, "storage deadline expired; native resources retained, partition quarantined")
	}
}
func backend(err error) error {
	if err == nil {
		return nil
	}
	return status.Errorf(codes.Unavailable, "storage outcome unknown: %v", err)
}
func get(tx *native.DbTransaction, key string) ([]byte, error) {
	value, err := tx.Get([]byte(key))
	if err != nil {
		return nil, backend(err)
	}
	if value == nil {
		return nil, nil
	}
	return *value, nil
}
func put(tx *native.DbTransaction, key string, value []byte) error {
	return backend(tx.Put([]byte(key), value))
}
func commit(tx *native.DbTransaction) error {
	optional, err := tx.Commit()
	if err != nil {
		return backend(err)
	}
	if optional == nil || *optional == nil {
		return status.Error(codes.Unavailable, "missing durability handle")
	}
	handle := *optional
	defer handle.Destroy()
	return backend(handle.AwaitDurable())
}
func (o *Owner) apply(request *wire.ShardRequest) (*wire.ShardResult, error) {
	outcome, err := o.journal(request.OperationId, request.CommandSha256, shardFamily, func(tx *native.DbTransaction) (*wire.StoredOutcome, error) {
		command := request.Command
		shardKey := fmt.Sprintf("v1/shard/%010d", command.ShardId)
		raw, err := get(tx, shardKey)
		if err != nil {
			return nil, err
		}
		var shard *wire.StoredShard
		if raw != nil {
			shard = &wire.StoredShard{}
			if err = proto.Unmarshal(raw, shard); err != nil {
				return nil, backend(err)
			}
		}
		result := &wire.ShardResult{ShardId: command.ShardId}
		save := false
		switch command.Kind {
		case wire.ShardCommand_GET, wire.ShardCommand_CREATE_OR_GET:
			if shard == nil && command.Kind == wire.ShardCommand_CREATE_OR_GET {
				shard = &wire.StoredShard{RangeId: command.RangeId, Data: command.Data, Encoding: command.Encoding}
				save = true
			}
			if shard == nil {
				result.Error = wire.ShardResult_NOT_FOUND
				result.Message = fmt.Sprintf("shard %d not found", command.ShardId)
			} else {
				copyShard(result, shard)
			}
		case wire.ShardCommand_UPDATE, wire.ShardCommand_ASSERT:
			expected := command.RangeId
			if command.Kind == wire.ShardCommand_UPDATE {
				expected = command.PreviousRangeId
			}
			if shard == nil && command.Kind == wire.ShardCommand_UPDATE {
				result.Error = wire.ShardResult_UNAVAILABLE
				result.Message = fmt.Sprintf("Failed to lock shard %d: shard does not exist", command.ShardId)
			} else if shard == nil || shard.RangeId != expected {
				result.Error = wire.ShardResult_OWNERSHIP_LOST
				result.Message = fmt.Sprintf("shard %d range mismatch: expected %d", command.ShardId, expected)
			} else if command.Kind == wire.ShardCommand_UPDATE {
				shard = &wire.StoredShard{RangeId: command.RangeId, Data: command.Data, Encoding: command.Encoding}
				save = true
				copyShard(result, shard)
			}
		}
		if save {
			data, _ := proto.Marshal(shard)
			if err = put(tx, shardKey, data); err != nil {
				return nil, err
			}
		}
		return &wire.StoredOutcome{Result: &wire.StoredOutcome_ShardResult{ShardResult: result}}, nil
	})
	if err != nil {
		return nil, err
	}
	return outcome.GetShardResult(), nil
}

func copyShard(result *wire.ShardResult, shard *wire.StoredShard) {
	result.RangeId = shard.RangeId
	result.Data = shard.Data
	result.Encoding = shard.Encoding
}

// authorityBarrier overwrites one reserved key. Even read/replay-only operations
// must touch the WAL and await durability to detect a delayed opener's fence.
func (o *Owner) authorityBarrier() error {
	tx, err := o.db.Begin(native.IsolationLevelSerializableSnapshot)
	if err != nil {
		return backend(err)
	}
	defer tx.Destroy()
	if err = put(tx, "v1/ownership/read-barrier", []byte(uuid.NewString())); err != nil {
		return err
	}
	return commit(tx)
}
