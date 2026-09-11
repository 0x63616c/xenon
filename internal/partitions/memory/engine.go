// Package memory provides a small, deterministic partition engine for tests.
// Data survives writer close and reopen for the lifetime of Engine; opening a
// second writer for a path fences the first writer.
package memory

import (
	"bytes"
	"context"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/0x63616c/xenon/internal/partitions"
)

type Engine struct {
	mu    sync.Mutex
	paths map[string]*database
}

func New() *Engine { return &Engine{paths: make(map[string]*database)} }

type database struct {
	mu      sync.Mutex
	data    map[string][]byte
	version uint64
	epoch   uint64
}

func (e *Engine) Open(ctx context.Context, r partitions.OpenRequest) (partitions.Writer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.Path == "" || strings.HasPrefix(r.Path, "/") || path.Clean(r.Path) != r.Path || r.Path == "." || r.Path == ".." || strings.HasPrefix(r.Path, "../") ||
		r.Partition.Validate() != nil || r.Reservation.Validate() != nil || r.Incarnation.Validate() != nil || r.AssignmentRevision == 0 || r.Generation == 0 {
		return nil, partitions.ErrInvalid
	}
	e.mu.Lock()
	db := e.paths[r.Path]
	if db == nil {
		db = &database{data: make(map[string][]byte)}
		e.paths[r.Path] = db
	}
	db.mu.Lock()
	db.epoch++
	w := &writer{db: db, epoch: db.epoch}
	db.mu.Unlock()
	e.mu.Unlock()
	return w, nil
}

type writer struct {
	mu     sync.Mutex
	db     *database
	epoch  uint64
	closed bool
	busy   bool
	nextID uint64
}

func (w *writer) usable() error {
	if w.closed {
		return partitions.ErrRetired
	}
	w.db.mu.Lock()
	fenced := w.db.epoch != w.epoch
	w.db.mu.Unlock()
	if fenced {
		return partitions.ErrFenced
	}
	return nil
}

func (w *writer) BeginOperation(ctx context.Context) (partitions.Operation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.usable(); err != nil {
		return nil, err
	}
	if w.busy {
		return nil, partitions.ErrBusy
	}
	w.busy = true
	return &operation{w: w}, nil
}

func (w *writer) Begin(ctx context.Context) (partitions.Transaction, error) {
	opi, err := w.BeginOperation(ctx)
	if err != nil {
		return nil, err
	}
	op := opi.(*operation)
	op.implicit = true
	tx, err := op.Begin(ctx)
	if err != nil {
		op.Release()
	}
	return tx, err
}

func (w *writer) AwaitDurable(ctx context.Context, capability partitions.CommitReceipt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r, ok := capability.(*receipt)
	if !ok || r == nil || r.owner != w {
		return partitions.ErrInvalid
	}
	return r.err
}

func (w *writer) ReadDurable(ctx context.Context, request partitions.ReadRequest) (partitions.ReadResult, error) {
	tx, err := w.Begin(ctx)
	if err != nil {
		return partitions.ReadResult{}, err
	}
	defer tx.Abort()
	var out partitions.ReadResult
	for _, key := range request.Keys {
		value, err := tx.Get(ctx, key)
		if err != nil {
			return partitions.ReadResult{}, err
		}
		out.Entries = append(out.Entries, partitions.Entry{Key: bytes.Clone(key), Value: value})
	}
	if request.Scan != nil {
		scan, err := tx.Scan(ctx, *request.Scan)
		if err != nil {
			return partitions.ReadResult{}, err
		}
		out.Entries = append(out.Entries, scan.Entries...)
		out.More = scan.More
	}
	r, err := tx.Commit(ctx)
	if err != nil {
		return partitions.ReadResult{}, err
	}
	return out, w.AwaitDurable(ctx, r)
}

func (w *writer) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}

type operation struct {
	mu       sync.Mutex
	w        *writer
	implicit bool
	released bool
	active   bool
	tx       *transaction
}

func (o *operation) Begin(ctx context.Context) (partitions.Transaction, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.released {
		return nil, partitions.ErrOperationDone
	}
	if o.active {
		return nil, partitions.ErrBusy
	}
	if err := o.w.usable(); err != nil {
		return nil, err
	}
	o.w.db.mu.Lock()
	snapshot := cloneMap(o.w.db.data)
	version := o.w.db.version
	o.w.db.mu.Unlock()
	o.active = true
	tx := &transaction{op: o, snapshot: snapshot, version: version, writes: make(map[string]*[]byte)}
	o.tx = tx
	return tx, nil
}

func (o *operation) Release() {
	o.mu.Lock()
	if !o.released {
		o.released = true
	}
	tx := o.tx
	o.mu.Unlock()
	if tx != nil {
		tx.abortReleased()
	} else {
		o.releaseWriter()
	}
}

func (o *operation) finish() {
	o.mu.Lock()
	o.active = false
	o.tx = nil
	release := o.implicit || o.released
	if o.implicit {
		o.released = true
	}
	o.mu.Unlock()
	if release {
		o.releaseWriter()
	}
}

func (o *operation) releaseWriter() {
	o.w.mu.Lock()
	o.w.busy = false
	o.w.mu.Unlock()
}

type transaction struct {
	mu       sync.Mutex
	op       *operation
	snapshot map[string][]byte
	version  uint64
	writes   map[string]*[]byte
	done     bool
}

func validKey(key []byte) bool { return len(key) > 0 && key[0] != 0 }

func (t *transaction) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if t.done {
		return partitions.ErrTransactionDone
	}
	t.op.mu.Lock()
	released := t.op.released && !t.op.implicit
	t.op.mu.Unlock()
	if released {
		return partitions.ErrOperationDone
	}
	return t.op.w.usable()
}

func (t *transaction) Get(ctx context.Context, key []byte) ([]byte, error) {
	if !validKey(key) {
		return nil, partitions.ErrInvalid
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return nil, err
	}
	if value, ok := t.writes[string(key)]; ok {
		if value == nil {
			return nil, nil
		}
		return bytes.Clone(*value), nil
	}
	return bytes.Clone(t.snapshot[string(key)]), nil
}

func (t *transaction) Put(key, value []byte) error {
	if !validKey(key) {
		return partitions.ErrInvalid
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(context.Background()); err != nil {
		return err
	}
	v := bytes.Clone(value)
	t.writes[string(bytes.Clone(key))] = &v
	return nil
}

func (t *transaction) Delete(key []byte) error {
	if !validKey(key) {
		return partitions.ErrInvalid
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(context.Background()); err != nil {
		return err
	}
	t.writes[string(bytes.Clone(key))] = nil
	return nil
}

func (t *transaction) Scan(ctx context.Context, r partitions.ScanRequest) (partitions.ReadResult, error) {
	if r.Limit <= 0 || r.Limit > 100000 || (r.Start != nil && r.End != nil && bytes.Compare(r.Start, r.End) > 0) {
		return partitions.ReadResult{}, partitions.ErrInvalid
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return partitions.ReadResult{}, err
	}
	view := cloneMap(t.snapshot)
	for key, value := range t.writes {
		if value == nil {
			delete(view, key)
		} else {
			view[key] = bytes.Clone(*value)
		}
	}
	keys := make([]string, 0, len(view))
	for key := range view {
		k := []byte(key)
		if r.Start != nil && (bytes.Compare(k, r.Start) < 0 || (r.StartExclusive && bytes.Equal(k, r.Start))) {
			continue
		}
		if r.End != nil && (bytes.Compare(k, r.End) > 0 || (!r.EndInclusive && bytes.Equal(k, r.End))) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if r.Reverse {
		sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	}
	result := partitions.ReadResult{More: len(keys) > r.Limit}
	if len(keys) > r.Limit {
		keys = keys[:r.Limit]
	}
	for _, key := range keys {
		result.Entries = append(result.Entries, partitions.Entry{Key: []byte(key), Value: bytes.Clone(view[key])})
	}
	return result, nil
}

func (t *transaction) Commit(ctx context.Context) (partitions.CommitReceipt, error) {
	t.mu.Lock()
	if err := t.check(ctx); err != nil {
		t.mu.Unlock()
		return nil, err
	}
	t.done = true
	w := t.op.w
	w.db.mu.Lock()
	var err error
	if w.db.epoch != w.epoch {
		err = partitions.ErrFenced
	} else if w.db.version != t.version {
		err = partitions.ErrConflict
	} else {
		for key, value := range t.writes {
			if value == nil {
				delete(w.db.data, key)
			} else {
				w.db.data[key] = bytes.Clone(*value)
			}
		}
		w.db.version++
	}
	w.db.mu.Unlock()
	w.mu.Lock()
	w.nextID++
	id := w.nextID
	w.mu.Unlock()
	t.mu.Unlock()
	t.op.finish()
	r := &receipt{owner: w, id: id, err: err}
	if err != nil {
		return r, err
	}
	return r, nil
}

func (t *transaction) Abort() error {
	t.mu.Lock()
	if t.done {
		t.mu.Unlock()
		return partitions.ErrTransactionDone
	}
	t.done = true
	t.mu.Unlock()
	t.op.finish()
	return nil
}

func (t *transaction) abortReleased() {
	t.mu.Lock()
	if t.done {
		t.mu.Unlock()
		return
	}
	t.done = true
	t.mu.Unlock()
	t.op.finish()
}

type receipt struct {
	owner *writer
	id    uint64
	err   error
}

func (r *receipt) MutationID() uint64 { return r.id }

func cloneMap(source map[string][]byte) map[string][]byte {
	result := make(map[string][]byte, len(source))
	for key, value := range source {
		result[key] = bytes.Clone(value)
	}
	return result
}
