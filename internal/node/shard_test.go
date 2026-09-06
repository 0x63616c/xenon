package node

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"os"
	native "slatedb.io/slatedb-go/uniffi"
	"sync/atomic"
	"testing"
	"time"
)

type fixture struct {
	Schema          int    `json:"schema"`
	Prefix          string `json:"prefix"`
	NativeTimeoutMS int    `json:"native_timeout_ms"`
	DrainSeconds    int    `json:"drain_timeout_seconds"`
	RejectedCallers int    `json:"rejected_callers"`
}

func cfg(t *testing.T) fixture {
	t.Helper()
	raw, e := os.ReadFile("../../proof/go-shard/case.json")
	if e != nil {
		t.Fatal(e)
	}
	var c fixture
	if e = json.Unmarshal(raw, &c); e != nil || c.Schema != 1 || c.NativeTimeoutMS <= 0 || c.DrainSeconds <= 0 || c.RejectedCallers < 1 {
		t.Fatal("invalid fixture", e)
	}
	return c
}
func objects(t *testing.T) *native.ObjectStore {
	t.Helper()
	s, e := native.ObjectStoreResolve("memory:///")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.Destroy)
	return s
}
func engine(t *testing.T, s *native.ObjectStore, path string, manual bool) *native.Db {
	t.Helper()
	b := native.NewDbBuilder(path, s)
	defer b.Destroy()
	if manual {
		x := native.SettingsDefault()
		defer x.Destroy()
		if e := x.Set("flush_interval", "null"); e != nil {
			t.Fatal(e)
		}
		if e := b.WithSettings(x); e != nil {
			t.Fatal(e)
		}
	}
	d, e := b.Build()
	if e != nil {
		t.Fatal(e)
	}
	return d
}
func owner(t *testing.T, d *native.Db) *Owner {
	t.Helper()
	o, e := NewOwner(d, DefaultConfig("p"))
	if e != nil {
		t.Fatal(e)
	}
	return o
}
func closeOwner(t *testing.T, o *Owner) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg(t).DrainSeconds)*time.Second)
	defer cancel()
	if e := o.Close(ctx); e != nil && !errors.Is(e, native.ErrErrorClosed) {
		t.Error(e)
	}
}
func request(id string, c *wire.ShardCommand) *wire.ShardRequest {
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	digest := sha256.Sum256(raw)
	return &wire.ShardRequest{ProtocolVersion: 1, Partition: "p", OperationId: id, CommandSha256: digest[:], Command: c}
}
func TestGoOwnerRecoveryAndReplay(t *testing.T) {
	s := objects(t)
	path := cfg(t).Prefix + "-recover"
	o := owner(t, engine(t, s, path, false))
	ctx := context.Background()
	create := request("create", &wire.ShardCommand{Kind: wire.ShardCommand_CREATE_OR_GET, ShardId: 7, RangeId: 11, Data: []byte{0, 255, 1}, Encoding: 2})
	initial, e := o.Execute(ctx, create)
	if e != nil || initial.Error != wire.ShardResult_NONE {
		t.Fatal(initial, e)
	}
	update := request("update", &wire.ShardCommand{Kind: wire.ShardCommand_UPDATE, ShardId: 7, PreviousRangeId: 11, RangeId: 15, Data: []byte("later"), Encoding: 2})
	if r, e := o.Execute(ctx, update); e != nil || r.Error != wire.ShardResult_NONE {
		t.Fatal(r, e)
	}
	closeOwner(t, o)
	recovered := owner(t, engine(t, s, path, false))
	defer closeOwner(t, recovered)
	replay, e := recovered.Execute(ctx, create)
	if e != nil || !proto.Equal(replay, initial) {
		t.Fatal("durable result replay changed", replay, e)
	}
	current, e := recovered.Execute(ctx, request("get", &wire.ShardCommand{Kind: wire.ShardCommand_GET, ShardId: 7}))
	if e != nil || current.RangeId != 15 {
		t.Fatal("replay changed current state", current, e)
	}
}
func TestGoOwnerFencedReplay(t *testing.T) {
	s := objects(t)
	path := cfg(t).Prefix + "-fence"
	old := owner(t, engine(t, s, path, false))
	ctx := context.Background()
	create := request("create", &wire.ShardCommand{Kind: wire.ShardCommand_CREATE_OR_GET, ShardId: 1, RangeId: 3})
	if _, e := old.Execute(ctx, create); e != nil {
		t.Fatal(e)
	}
	replacement := owner(t, engine(t, s, path, false))
	defer closeOwner(t, replacement)
	if _, e := old.Execute(ctx, create); status.Code(e) != codes.Unavailable || !old.Quarantined() {
		t.Fatal("fenced replay served", e)
	}
	closeOwner(t, old)
	if r, e := replacement.Execute(ctx, create); e != nil || r.Error != wire.ShardResult_NONE {
		t.Fatal(r, e)
	}
}
func TestGoOwnerNativeTimeout(t *testing.T) {
	c := cfg(t)
	s := objects(t)
	path := c.Prefix + "-timeout"
	d := engine(t, s, path, true)
	o := owner(t, d)
	o.config.OperationTimeout = time.Duration(c.NativeTimeoutMS) * time.Millisecond
	create := request("create", &wire.ShardCommand{Kind: wire.ShardCommand_CREATE_OR_GET, ShardId: 1, RangeId: 3})
	if _, e := o.Execute(context.Background(), create); status.Code(e) != codes.Unavailable {
		t.Fatal("missing native timeout", e)
	}
	if !o.Quarantined() || o.ActiveNativeOperations() != 1 || o.destroyed.Load() {
		t.Fatal("native resources released prematurely")
	}
	for i := 0; i < c.RejectedCallers; i++ {
		if _, e := o.Execute(context.Background(), create); status.Code(e) != codes.Unavailable {
			t.Fatal("quarantined operation admitted", e)
		}
	}
	if o.ActiveNativeOperations() != 1 {
		t.Fatal("native work unbounded")
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if e := o.Close(closeCtx); e == nil || o.destroyed.Load() {
		t.Fatal("close freed active native state", e)
	}
	if e := d.FlushWithOptions(native.FlushOptions{FlushType: native.FlushTypeWal}); e != nil {
		t.Fatal(e)
	}
	deadline := time.Now().Add(time.Duration(c.DrainSeconds) * time.Second)
	for o.ActiveNativeOperations() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if o.ActiveNativeOperations() != 0 || !o.Quarantined() {
		t.Fatal("native wait did not drain without reopening")
	}
	closeOwner(t, o)
	recovered := owner(t, engine(t, s, path, false))
	defer closeOwner(t, recovered)
	if r, e := recovered.Execute(context.Background(), create); e != nil || r.Error != wire.ShardResult_NONE {
		t.Fatal("unknown outcome could not reconcile", r, e)
	}
}
func TestGoOwnerPausedAdmission(t *testing.T) {
	s := objects(t)
	o := owner(t, engine(t, s, cfg(t).Prefix+"-admission", false))
	defer closeOwner(t, o)
	o.gate <- struct{}{}
	var calls atomic.Int32
	done := make(chan error, 1)
	go func() {
		_, e := o.Run(context.Background(), func(*native.Db) ([]byte, error) { calls.Add(1); return nil, nil })
		done <- e
	}()
	deadline := time.Now().Add(time.Second)
	for len(o.admitted) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(o.admitted) != 1 {
		t.Fatal("admission barrier not reached")
	}
	o.quarantined.Store(true)
	<-o.gate
	select {
	case e := <-done:
		if status.Code(e) != codes.Unavailable {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("paused admission hung")
	}
	if calls.Load() != 0 {
		t.Fatal("callback admitted across quarantine")
	}
}
