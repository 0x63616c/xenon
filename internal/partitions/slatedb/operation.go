package slatedb

import (
	"context"
	"sync"

	p "github.com/0x63616c/xenon/internal/partitions"
	native "slatedb.io/slatedb-go/uniffi"
)

// operation owns the existing writer gate across zero or more transactions.
// Native completions, not RPC cancellation or Release, end an active transaction.
type operation struct {
	w        *writer
	mu       sync.Mutex
	current  *transaction
	released bool
	owns     bool
	implicit bool
}

func (w *writer) BeginOperation(ctx context.Context) (p.Operation, error) {
	if w.retired.Load() {
		return nil, p.ErrRetired
	}
	if err := acquire(ctx, w.gate); err != nil {
		return nil, err
	}
	if w.retired.Load() {
		<-w.gate
		return nil, p.ErrRetired
	}
	return &operation{w: w, owns: true}, nil
}
func (o *operation) Begin(ctx context.Context) (p.Transaction, error) {
	o.mu.Lock()
	if o.released {
		o.mu.Unlock()
		return nil, p.ErrOperationDone
	}
	if o.w.retired.Load() {
		o.mu.Unlock()
		return nil, p.ErrRetired
	}
	if o.current != nil {
		o.mu.Unlock()
		return nil, p.ErrBusy
	}
	t := &transaction{w: o.w, operation: o, gate: make(chan struct{}, 1)}
	o.current = t
	o.mu.Unlock()
	_, err := invoke(ctx, t, func() (any, error) {
		var err error
		t.tx, err = o.w.db.Begin(native.IsolationLevelSerializableSnapshot)
		return nil, o.w.failure(err)
	})
	if err != nil {
		_ = t.Abort()
		return nil, err
	}
	return t, nil
}
func (o *operation) isReleased() bool { o.mu.Lock(); defer o.mu.Unlock(); return o.released }
func (o *operation) releaseIdle() {
	if o.released && o.current == nil && o.owns {
		o.owns = false
		<-o.w.gate
	}
}
func (o *operation) finished(t *transaction) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.current != t {
		return
	}
	o.current = nil
	if o.implicit {
		o.released = true
	}
	o.releaseIdle()
}

// Release invalidates admission first. Abort either destroys an idle transaction
// or returns Busy/Done, leaving its retained native worker to finish and release.
func (o *operation) Release() {
	o.mu.Lock()
	if o.released {
		o.mu.Unlock()
		return
	}
	o.released = true
	t := o.current
	o.releaseIdle()
	o.mu.Unlock()
	if t != nil {
		_ = t.Abort()
	}
}
