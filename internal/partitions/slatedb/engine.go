// Package slatedb implements the serialized partition engine with the pinned
// official Go binding and its Rust core. Native calls have no cancellation API.
package slatedb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"sync/atomic"

	p "github.com/0x63616c/xenon/internal/partitions"
	native "slatedb.io/slatedb-go/uniffi"
)

const barrierKey = "\x00xenon/authority"

type Engine struct{ backend string }

// New requires direct S3. Endpoint/credentials use the native object-store
// environment; credentials are never copied into a request or evidence.
func New(objectStoreURL string) (*Engine, error) {
	if !strings.HasPrefix(objectStoreURL, "s3://") || len(objectStoreURL) == 5 {
		return nil, p.ErrInvalid
	}
	return &Engine{backend: objectStoreURL}, nil
}
func (e *Engine) Open(ctx context.Context, r p.OpenRequest) (p.Writer, error) {
	if r.Path == "" || strings.HasPrefix(r.Path, "/") || (path.Clean(r.Path) != r.Path || r.Path == "." || r.Path == ".." || strings.HasPrefix(r.Path, "../")) || r.Partition.Validate() != nil || r.Reservation.Validate() != nil || r.Incarnation.Validate() != nil || r.AssignmentRevision == 0 || r.Generation == 0 {
		return nil, p.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return completeOpen(ctx, func() (*writer, error) {
		store, err := native.ObjectStoreResolve(e.backend)
		if err != nil {
			return nil, translate(err)
		}
		defer store.Destroy()
		builder := native.NewDbBuilder(r.Path, store)
		defer builder.Destroy()
		db, err := builder.Build()
		if err != nil {
			return nil, translate(err)
		}
		return newWriter(db), nil
	})
}

// completeOpen keeps native Build and any late cleanup on the caller's effect.
// The binding has no cancellation API: after dispatch, cancellation cannot return
// until native completion and handle cleanup. Drivers may run this call in their
// owned worker, but must retain its effect until it returns (or terminate process).
func completeOpen(ctx context.Context, build func() (*writer, error)) (p.Writer, error) {
	w, err := build()
	if canceled := ctx.Err(); canceled != nil {
		if w != nil {
			// This is a drain, not a new operation budget. Close must finish before
			// the Open effect completes, including any close failure.
			err = errors.Join(err, w.Close(context.Background()))
		}
		return nil, &p.UnknownOutcome{Cause: errors.Join(canceled, err)}
	}
	if err != nil {
		return nil, err
	}
	return w, nil
}

type writer struct {
	db       *native.Db
	gate     chan struct{}
	retired  atomic.Bool
	sequence atomic.Uint64
	closed   bool // only while owning gate
	closeErr error
}

func newWriter(db *native.Db) *writer { return &writer{db: db, gate: make(chan struct{}, 1)} }
func acquire(ctx context.Context, gate chan struct{}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-gate
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func translate(err error) error {
	if err == nil {
		return nil
	}
	var closed *native.ErrorClosed
	if errors.As(err, &closed) && closed.Reason == native.CloseReasonFenced {
		return fmt.Errorf("%w: %v", p.ErrFenced, err)
	}
	if errors.Is(err, native.ErrErrorTransaction) {
		return fmt.Errorf("%w: %v", p.ErrConflict, err)
	}
	if errors.Is(err, native.ErrErrorInvalid) {
		return fmt.Errorf("%w: %v", p.ErrInvalid, err)
	}
	return fmt.Errorf("%w: %v", p.ErrRetired, err)
}
func (w *writer) failure(err error) error {
	err = translate(err)
	if err != nil && !errors.Is(err, p.ErrInvalid) && !errors.Is(err, p.ErrConflict) {
		w.retired.Store(true)
	}
	return err
}
func (w *writer) Begin(ctx context.Context) (p.Transaction, error) {
	scope, err := w.BeginOperation(ctx)
	if err != nil {
		return nil, err
	}
	op := scope.(*operation)
	op.implicit = true
	tx, err := op.Begin(ctx)
	if err != nil {
		op.Release()
		return nil, err
	}
	return tx, nil
}

type transaction struct {
	w         *writer
	operation *operation
	tx        *native.DbTransaction
	gate      chan struct{}
	finished  bool
}

func (t *transaction) dispose() {
	if !t.finished {
		t.finished = true
		if t.tx != nil {
			t.tx.Destroy()
		}
		t.operation.finished(t)
	}
}

// invoke transfers the transaction lock to the native worker. Cancellation
// retires admission; only that worker can destroy the transaction after return.
func invoke(ctx context.Context, t *transaction, fn func() (any, error)) (any, error) {
	if err := acquire(ctx, t.gate); err != nil {
		return nil, err
	}
	if t.finished {
		<-t.gate
		return nil, p.ErrTransactionDone
	}
	if t.operation.isReleased() {
		t.dispose()
		<-t.gate
		return nil, p.ErrOperationDone
	}
	if t.w.retired.Load() {
		t.dispose()
		<-t.gate
		return nil, p.ErrRetired
	}
	type result struct {
		value any
		err   error
	}
	done := make(chan result, 1)
	decision := make(chan bool, 1)
	released := make(chan struct{})
	go func() {
		value, err := fn()
		done <- result{value, err}
		canceled := <-decision
		if canceled || err != nil || t.operation.isReleased() {
			t.dispose()
		}
		<-t.gate
		// Close the race where Release observed the native call as busy after
		// the worker checked invalidation but before it dropped the inner gate.
		if t.operation.isReleased() {
			_ = t.Abort()
		}
		close(released)
	}()
	select {
	case v := <-done:
		if err := ctx.Err(); err != nil {
			t.w.retired.Store(true)
			decision <- true
			return nil, &p.UnknownOutcome{Cause: err}
		}
		decision <- false
		<-released
		if t.operation.isReleased() && !t.operation.implicit && v.err == nil {
			return nil, p.ErrOperationDone
		}
		return v.value, v.err
	case <-ctx.Done():
		t.w.retired.Store(true)
		decision <- true
		return nil, &p.UnknownOutcome{Cause: ctx.Err()}
	}
}
func validKey(key []byte) bool { return len(key) > 0 && key[0] != 0 }
func (t *transaction) Get(ctx context.Context, key []byte) ([]byte, error) {
	if !validKey(key) {
		return nil, p.ErrInvalid
	}
	key = bytes.Clone(key)
	v, err := invoke(ctx, t, func() (any, error) {
		raw, err := t.tx.Get(key)
		if err != nil {
			return nil, t.w.failure(err)
		}
		if raw == nil {
			return []byte(nil), nil
		}
		return bytes.Clone(*raw), nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}
func (t *transaction) Put(key, value []byte) error {
	return t.stage(key, func() error { return t.tx.Put(key, value) })
}
func (t *transaction) Delete(key []byte) error {
	return t.stage(key, func() error { return t.tx.Delete(key) })
}
func (t *transaction) stage(key []byte, fn func() error) error {
	if !validKey(key) {
		return p.ErrInvalid
	}
	t.gate <- struct{}{}
	defer func() {
		<-t.gate
		if t.operation.isReleased() {
			_ = t.Abort()
		}
	}()
	if t.finished {
		return p.ErrTransactionDone
	}
	if t.operation.isReleased() {
		t.dispose()
		return p.ErrOperationDone
	}
	if t.w.retired.Load() {
		t.dispose()
		return p.ErrRetired
	}
	err := t.w.failure(fn())
	if t.operation.isReleased() && err == nil {
		err = p.ErrOperationDone
	}
	if err != nil {
		t.dispose()
	}
	return err
}
func (t *transaction) Scan(ctx context.Context, r p.ScanRequest) (p.ReadResult, error) {
	if r.Limit <= 0 || r.Limit > 100000 || (r.Start != nil && r.End != nil && bytes.Compare(r.Start, r.End) > 0) {
		return p.ReadResult{}, p.ErrInvalid
	}
	r.Start = bytes.Clone(r.Start)
	r.End = bytes.Clone(r.End)
	v, err := invoke(ctx, t, func() (any, error) {
		if r.Start != nil && r.End != nil && bytes.Equal(r.Start, r.End) && (r.StartExclusive || !r.EndInclusive) {
			return p.ReadResult{}, nil
		}
		bounds := native.KeyRange{StartInclusive: !r.StartExclusive, EndInclusive: r.EndInclusive}
		if r.Start != nil {
			bounds.Start = &r.Start
		}
		if r.End != nil {
			bounds.End = &r.End
		}
		order := native.IterationOrderAscending
		if r.Reverse {
			order = native.IterationOrderDescending
		}
		// Preserve native transaction scan defaults, changing only direction.
		// Read one extra application row for More; never materialize the range.
		durability := native.DurabilityLevelMemory
		if r.RemoteDurable {
			durability = native.DurabilityLevelRemote
		}
		it, err := t.tx.ScanWithOptions(bounds, native.ScanOptions{DurabilityFilter: durability, ReadAheadBytes: 1, MaxFetchTasks: 1, Order: &order})
		if err != nil {
			return nil, t.w.failure(err)
		}
		defer it.Destroy()
		result := p.ReadResult{}
		for {
			kv, err := it.Next()
			if err != nil {
				return nil, t.w.failure(err)
			}
			if kv == nil {
				break
			}
			if !validKey(kv.Key) {
				continue
			}
			if len(result.Entries) == r.Limit {
				result.More = true
				break
			}
			result.Entries = append(result.Entries, p.Entry{Key: bytes.Clone(kv.Key), Value: bytes.Clone(kv.Value)})
		}
		return result, nil
	})
	if err != nil {
		return p.ReadResult{}, err
	}
	return v.(p.ReadResult), nil
}

type receipt struct {
	owner *writer
	id    uint64
	done  chan struct{}
	err   error
}

func (r *receipt) MutationID() uint64 { return r.id }
func (t *transaction) Commit(ctx context.Context) (p.CommitReceipt, error) {
	if err := acquire(ctx, t.gate); err != nil {
		return nil, err
	}
	if t.finished {
		<-t.gate
		return nil, p.ErrTransactionDone
	}
	if t.operation.isReleased() {
		t.dispose()
		<-t.gate
		return nil, p.ErrOperationDone
	}
	if t.w.retired.Load() {
		t.dispose()
		<-t.gate
		return nil, p.ErrRetired
	}
	t.finished = true // irrevocable dispatch; Abort can no longer release the writer.
	<-t.gate
	r := &receipt{owner: t.w, id: t.w.sequence.Add(1), done: make(chan struct{})}
	committed := make(chan error, 1)
	decision := make(chan struct{})
	defer close(decision)
	go func() {
		// A nonempty WAL barrier also makes read-only/replay commits detect fencing.
		err := t.tx.Put([]byte(barrierKey), []byte{1})
		var h **native.WriteHandle
		if err == nil {
			h, err = t.tx.Commit()
		}
		t.tx.Destroy()
		err = t.w.failure(err)
		if err == nil && (h == nil || *h == nil) {
			t.w.retired.Store(true)
			err = p.ErrRetired
		}
		committed <- err
		if err == nil {
			err = t.w.failure((*h).AwaitDurable())
			(*h).Destroy()
		}
		if err != nil {
			t.w.retired.Store(true)
		}
		r.err = err
		<-decision
		t.operation.finished(t)
		close(r.done)
	}()
	select {
	case err := <-committed:
		if canceled := ctx.Err(); canceled != nil {
			t.w.retired.Store(true)
			return r, &p.UnknownOutcome{Cause: canceled}
		}
		if err != nil {
			return r, &p.UnknownOutcome{Cause: err}
		}
		return r, nil
	case <-ctx.Done():
		t.w.retired.Store(true)
		return r, &p.UnknownOutcome{Cause: ctx.Err()}
	}
}
func (t *transaction) Abort() error {
	select {
	case t.gate <- struct{}{}:
	default:
		return p.ErrBusy
	}
	defer func() { <-t.gate }()
	if t.finished {
		return p.ErrTransactionDone
	}
	t.dispose()
	return nil
}
func (w *writer) AwaitDurable(ctx context.Context, cap p.CommitReceipt) error {
	r, ok := cap.(*receipt)
	if !ok || r == nil || r.owner != w {
		return p.ErrInvalid
	}
	select {
	case <-r.done:
		if err := ctx.Err(); err != nil {
			w.retired.Store(true)
			return &p.UnknownOutcome{Cause: err}
		}
		if r.err != nil {
			return &p.UnknownOutcome{Cause: r.err}
		}
		return nil
	case <-ctx.Done():
		w.retired.Store(true)
		return &p.UnknownOutcome{Cause: ctx.Err()}
	}
}
func (w *writer) ReadDurable(ctx context.Context, r p.ReadRequest) (p.ReadResult, error) {
	tx, err := w.Begin(ctx)
	if err != nil {
		return p.ReadResult{}, err
	}
	defer tx.Abort()
	result := p.ReadResult{}
	for _, key := range r.Keys {
		value, err := tx.Get(ctx, key)
		if err != nil {
			return p.ReadResult{}, err
		}
		result.Entries = append(result.Entries, p.Entry{Key: bytes.Clone(key), Value: value})
	}
	if r.Scan != nil {
		scan, err := tx.Scan(ctx, *r.Scan)
		if err != nil {
			return p.ReadResult{}, err
		}
		result.Entries = append(result.Entries, scan.Entries...)
		result.More = scan.More
	}
	cap, err := tx.Commit(ctx)
	if err != nil {
		return p.ReadResult{}, err
	}
	if err = w.AwaitDurable(ctx, cap); err != nil {
		return p.ReadResult{}, err
	}
	return result, nil
}
func (w *writer) Close(ctx context.Context) error {
	w.retired.Store(true)
	if err := acquire(ctx, w.gate); err != nil {
		return err
	}
	if w.closed {
		<-w.gate
		return w.closeErr
	}
	done := make(chan error, 1)
	go func() {
		w.closeErr = translate(w.db.Shutdown())
		w.db.Destroy()
		w.closed = true
		done <- w.closeErr
		<-w.gate
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
