package probe

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	db "slatedb.io/slatedb-go/uniffi"
	"sync/atomic"
	"testing"
	"time"
)

type fixture struct {
	FlushInterval       json.RawMessage `json:"flush_interval"`
	Schema              int             `json:"schema"`
	Backend             string          `json:"backend"`
	Prefix              string          `json:"prefix"`
	CallerTimeoutMS     int             `json:"caller_timeout_ms"`
	DrainTimeoutSeconds int             `json:"drain_timeout_seconds"`
	RejectedCallers     int             `json:"rejected_callers"`
}

func config(t *testing.T) fixture {
	t.Helper()
	b, e := os.ReadFile("case.json")
	if e != nil {
		t.Fatal(e)
	}
	var c fixture
	if e = json.Unmarshal(b, &c); e != nil {
		t.Fatal(e)
	}
	if c.Schema != 1 || c.CallerTimeoutMS <= 0 || c.DrainTimeoutSeconds <= 0 || c.RejectedCallers <= 0 || string(c.FlushInterval) != "null" {
		t.Fatal("invalid fixture")
	}
	return c
}
func store(t *testing.T) *db.ObjectStore {
	t.Helper()
	s, e := db.ObjectStoreResolve(config(t).Backend)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.Destroy)
	return s
}
func open(t *testing.T, s *db.ObjectStore, path string, manual bool) *db.Db {
	t.Helper()
	b := db.NewDbBuilder(path, s)
	defer b.Destroy()
	if manual {
		settings := db.SettingsDefault()
		defer settings.Destroy()
		if e := settings.Set("flush_interval", string(config(t).FlushInterval)); e != nil {
			t.Fatal(e)
		}
		if e := b.WithSettings(settings); e != nil {
			t.Fatal(e)
		}
	}
	d, e := b.Build()
	if e != nil {
		t.Fatal(e)
	}
	return d
}
func closeDB(t *testing.T, d *db.Db) {
	t.Helper()
	if e := d.Shutdown(); e != nil {
		t.Error(e)
	}
	status := d.Status()
	if status.CloseReason == nil || *status.CloseReason != db.CloseReasonClean {
		t.Error("shutdown did not report clean closure")
	}
	if handle, e := d.Put([]byte("after-shutdown"), []byte("rejected")); !errors.Is(e, db.ErrErrorClosed) {
		if handle != nil {
			handle.Destroy()
		}
		t.Errorf("closed database accepted write or lost typed error: %v", e)
	}
	d.Destroy()
}
func put(t *testing.T, d *db.Db, key, value string) *db.WriteHandle {
	t.Helper()
	h, e := d.Put([]byte(key), []byte(value))
	if e != nil {
		t.Fatal(e)
	}
	return h
}
func durable(t *testing.T, h *db.WriteHandle) {
	t.Helper()
	defer h.Destroy()
	if e := h.AwaitDurable(); e != nil {
		t.Fatal(e)
	}
}
func tx(t *testing.T, d *db.Db) *db.DbTransaction {
	t.Helper()
	x, e := d.Begin(db.IsolationLevelSerializableSnapshot)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(x.Destroy)
	return x
}
func commit(t *testing.T, x *db.DbTransaction) *db.WriteHandle {
	t.Helper()
	h, e := x.Commit()
	if e != nil {
		t.Fatal(e)
	}
	if h == nil || *h == nil {
		t.Fatal("missing commit handle")
	}
	return *h
}
func TestSerializableReadConflict(t *testing.T) {
	s := store(t)
	d := open(t, s, config(t).Prefix+"-conflict", false)
	defer closeDB(t, d)
	durable(t, put(t, d, "a", "0"))
	durable(t, put(t, d, "b", "0"))
	first, second := tx(t, d), tx(t, d)
	if _, e := first.Get([]byte("b")); e != nil {
		t.Fatal(e)
	}
	if _, e := second.Get([]byte("a")); e != nil {
		t.Fatal(e)
	}
	if e := first.Put([]byte("a"), []byte("1")); e != nil {
		t.Fatal(e)
	}
	if e := second.Put([]byte("b"), []byte("1")); e != nil {
		t.Fatal(e)
	}
	durable(t, commit(t, first))
	if h, e := second.Commit(); e == nil {
		if h != nil {
			(*h).Destroy()
		}
		t.Fatal("write skew accepted")
	} else if !errors.Is(e, db.ErrErrorTransaction) {
		t.Fatalf("lost typed transaction error: %T %v", e, e)
	}
	value, e := d.Get([]byte("b"))
	if e != nil || value == nil || !bytes.Equal(*value, []byte("0")) {
		t.Fatal("failed transaction leaked", e)
	}
}
func TestDurabilityVisibilityAndReopen(t *testing.T) {
	s := store(t)
	path := config(t).Prefix + "-durable"
	d := open(t, s, path, true)
	h := put(t, d, "state", "acknowledged")
	value, e := d.Get([]byte("state"))
	if e != nil || value == nil {
		t.Fatal("memory visibility missing", e)
	}
	remote, e := d.GetWithOptions([]byte("state"), db.ReadOptions{DurabilityFilter: db.DurabilityLevelRemote})
	if e != nil || remote != nil {
		t.Fatal("volatile value remotely visible", e)
	}
	finished := make(chan error, 1)
	go func() { finished <- h.AwaitDurable() }()
	select {
	case e := <-finished:
		t.Fatal("durability returned before flush", e)
	case <-time.After(time.Duration(config(t).CallerTimeoutMS) * time.Millisecond):
	}
	if e = d.FlushWithOptions(db.FlushOptions{FlushType: db.FlushTypeWal}); e != nil {
		t.Fatal(e)
	}
	select {
	case e = <-finished:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Duration(config(t).DrainTimeoutSeconds) * time.Second):
		t.Fatal("flush did not drain")
	}
	h.Destroy()
	closeDB(t, d)
	recovered := open(t, s, path, false)
	defer closeDB(t, recovered)
	value, e = recovered.Get([]byte("state"))
	if e != nil || value == nil || !bytes.Equal(*value, []byte("acknowledged")) {
		t.Fatal("acknowledged state not recovered", e)
	}
}
func TestTypedFence(t *testing.T) {
	s := store(t)
	path := config(t).Prefix + "-fence"
	old := open(t, s, path, false)
	durable(t, put(t, old, "before", "yes"))
	replacement := open(t, s, path, false)
	defer closeDB(t, replacement)
	h, e := old.Put([]byte("stale"), []byte("no"))
	if e == nil {
		e = h.AwaitDurable()
		h.Destroy()
	}
	var closed *db.ErrorClosed
	if !errors.Is(e, db.ErrErrorClosed) || !errors.As(e, &closed) || closed.Reason != db.CloseReasonFenced {
		t.Fatalf("typed fence lost: %T %v", e, e)
	}
	// Shutdown of an already fenced database may report its terminal fencing error.
	if e = old.Shutdown(); e != nil && !errors.Is(e, db.ErrErrorClosed) {
		t.Fatal(e)
	}
	old.Destroy()
	durable(t, put(t, replacement, "after", "yes"))
}

// oneNativeWait models an owning process admission slot. Timeout is NOT native
// cancellation: the slot and handle remain held until the original FFI call ends.
type oneNativeWait struct {
	slot        chan struct{}
	quarantined atomic.Bool
	active      atomic.Int32
	destroyed   atomic.Bool
}

func (w *oneNativeWait) await(h *db.WriteHandle, deadline time.Duration) (<-chan error, error) {
	if w.quarantined.Load() {
		return nil, errors.New("quarantined")
	}
	select {
	case w.slot <- struct{}{}:
	default:
		return nil, errors.New("busy")
	}
	done := make(chan error, 1)
	w.active.Add(1)
	go func() {
		e := h.AwaitDurable()
		h.Destroy()
		w.destroyed.Store(true)
		w.active.Add(-1)
		<-w.slot
		done <- e
	}()
	select {
	case e := <-done:
		return nil, e
	case <-time.After(deadline):
		w.quarantined.Store(true)
		return done, errors.New("caller timed out; native wait retained")
	}
}
func TestTimedOutNativeWaitRetainsHandleAndGate(t *testing.T) {
	c := config(t)
	s := store(t)
	d := open(t, s, c.Prefix+"-timeout", true)
	defer closeDB(t, d)
	w := &oneNativeWait{slot: make(chan struct{}, 1)}
	h := put(t, d, "pending", "retained")
	started := time.Now()
	done, e := w.await(h, time.Duration(c.CallerTimeoutMS)*time.Millisecond)
	if e == nil || done == nil || time.Since(started) > time.Second {
		t.Fatal("caller timeout not bounded")
	}
	if !w.quarantined.Load() || w.active.Load() != 1 || w.destroyed.Load() || len(w.slot) != 1 {
		t.Fatal("native handle or gate released before native completion")
	}
	for i := 0; i < c.RejectedCallers; i++ {
		if _, e = w.await(nil, time.Millisecond); e == nil {
			t.Fatal("quarantined caller admitted")
		}
	}
	if w.active.Load() != 1 {
		t.Fatal("additional native workers spawned")
	}
	if e = d.FlushWithOptions(db.FlushOptions{FlushType: db.FlushTypeWal}); e != nil {
		t.Fatal(e)
	}
	select {
	case e = <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Duration(c.DrainTimeoutSeconds) * time.Second):
		t.Fatal("native wait did not drain after flush")
	}
	if w.active.Load() != 0 || !w.destroyed.Load() || len(w.slot) != 0 || !w.quarantined.Load() {
		t.Fatal("lifecycle/quarantine invariant lost")
	}
}
