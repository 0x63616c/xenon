package ownership

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/0x63616c/xenon/internal/directory"
	"github.com/0x63616c/xenon/internal/node"
	"github.com/0x63616c/xenon/internal/proof/openpause"
	"github.com/0x63616c/xenon/internal/routing"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	native "slatedb.io/slatedb-go/uniffi"
)

type managed struct {
	owner  *node.Owner
	record directory.Record
}

// Manager owns disposable process state only. Topology activation is explicit;
// callers must publish Identity().Incarnation before any partition can open.
type Manager struct {
	topology       *TopologyStore
	identity       directory.Identity
	objectURL      string
	maxOutcomes    uint64
	dispatchCounts map[string]LocalDispatch
	mu             sync.Mutex
	owners         map[string]*managed
	workers        map[string]bool
	directories    map[string]*directory.Directory
	closed         bool
	// Tests pause real opens at exact declared fault points; nil in the binary.
	openPause   *openpause.Control
	beforeOpen  func(directory.Record)
	beforeReady func(directory.Record)
}

func NewManager(topology *TopologyStore, nodeID, address, objectURL string, maxOutcomes uint64) (*Manager, error) {
	if maxOutcomes == 0 || topology == nil || nodeID == "" || address == "" || objectURL != "s3://"+topology.bucket {
		return nil, directory.ErrInvalid
	}
	return &Manager{topology: topology, identity: directory.Identity{Node: nodeID, Address: address, Incarnation: uuid.NewString()}, objectURL: objectURL, maxOutcomes: maxOutcomes, dispatchCounts: map[string]LocalDispatch{}, owners: map[string]*managed{}, workers: map[string]bool{}, directories: map[string]*directory.Directory{}}, nil
}

// SetOpenPause installs a proof control before Run; nil is the production default.
func (m *Manager) SetOpenPause(p *openpause.Control) { m.openPause = p }

func (m *Manager) Identity() directory.Identity { return m.identity }
func (m *Manager) desired(t Topology, id string) bool {
	a, ok := t.Partitions[id]
	member, exists := t.Members[m.identity.Node]
	return ok && exists && a.Node == m.identity.Node && member.Incarnation == m.identity.Incarnation && member.Address == m.identity.Address
}
func (m *Manager) directory(id, prefix string) (*directory.Directory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if d := m.directories[id]; d != nil {
		return d, nil
	}
	d, e := directory.New(m.topology.client, m.topology.bucket, m.topology.prefix+"/owners", id, prefix)
	if e == nil {
		m.directories[id] = d
	}
	return d, e
}
func (m *Manager) authority(ctx context.Context, d *directory.Directory, want directory.Record) error {
	topology, e := m.topology.Read(ctx)
	if e != nil {
		return e
	}
	if !m.desired(topology.data, want.Partition) || topology.data.Partitions[want.Partition].DataPrefix != want.DataPrefix {
		return directory.ErrConflict
	}
	observed, e := d.Read(ctx)
	if e != nil {
		return e
	}
	if observed.Record() != want {
		return directory.ErrConflict
	}
	return nil
}

// Run starts at most one serial opener per immutable partition in this process.
// A stuck FFI call retains that worker/resources; it cannot trigger another open.
func (m *Manager) Run(ctx context.Context) {
	defer m.retireAll()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		snapshot, e := m.topology.Read(ctx)
		if e == nil {
			m.mu.Lock()
			if m.closed {
				m.mu.Unlock()
				return
			}
			for id := range snapshot.data.Partitions {
				if !m.workers[id] {
					m.workers[id] = true
					go m.partition(ctx, id)
				}
			}
			m.mu.Unlock()
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
func (m *Manager) retireAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	for _, o := range m.owners {
		o.owner.Retire()
	}
}
func (m *Manager) partition(ctx context.Context, id string) {
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		_ = m.reconcile(ctx, id)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
func (m *Manager) reconcile(ctx context.Context, id string) error {
	m.mu.Lock()
	current := m.owners[id]
	closed := m.closed
	m.mu.Unlock()
	if closed {
		return context.Canceled
	}
	topology, e := m.topology.Read(ctx)
	if e != nil {
		return e
	}
	a, exists := topology.data.Partitions[id]
	if !exists {
		return directory.ErrInvalid
	}
	d, e := m.directory(id, a.DataPrefix)
	if e != nil {
		return e
	}
	if current != nil {
		if !current.owner.Quarantined() && m.authority(ctx, d, current.record) == nil {
			return nil
		}
		current.owner.Retire()
		// Close owns the gate and never frees resources still referenced by native work.
		// If shutdown remains blocked, retain this handle and do not open a replacement.
		if e = current.owner.Close(ctx); e != nil {
			return e
		}
		m.mu.Lock()
		delete(m.owners, id)
		m.mu.Unlock()
	}
	if !m.desired(topology.data, id) {
		return nil
	}
	// Fresh intention after retirement, before reservation.
	topology, e = m.topology.Read(ctx)
	if e != nil {
		return e
	}
	if !m.desired(topology.data, id) {
		return nil
	}
	prior, e := d.Read(ctx)
	var previous *directory.Snapshot
	if e == nil {
		previous = &prior
	} else if !errors.Is(e, directory.ErrMissing) {
		return e
	}
	reservation, e := d.Reserve(ctx, previous, m.identity)
	if e != nil {
		return e
	}
	record := reservation.Record()
	if m.openPause != nil {
		if e = m.openPause.Before(ctx, record); e != nil {
			return e
		}
	}
	if m.beforeOpen != nil {
		m.beforeOpen(record)
	}
	// Exactly one Build per reservation. Never retry this reservation after any
	// result, timeout or supersession. A blocked Build occupies this worker forever.
	store, e := native.ObjectStoreResolve(m.objectURL)
	if e != nil {
		return e
	}
	builder := native.NewDbBuilder(record.DataPrefix, store)
	db, e := builder.Build()
	builder.Destroy()
	store.Destroy()
	if e != nil {
		return e
	}
	config := m.ownerConfig(id)
	readyRecord := record
	readyRecord.State = "ready"
	config.Authority = func(c context.Context) error { return m.authority(c, d, readyRecord) }
	owner, e := node.NewOwner(db, config)
	if e != nil {
		return e
	}
	retire := func(err error) error {
		owner.Retire()
		// Keep an unclosed handle in the manager so reconciliation cannot open again
		// while Shutdown/native work remains blocked.
		m.mu.Lock()
		m.owners[id] = &managed{owner, readyRecord}
		m.mu.Unlock()
		if closeErr := owner.Close(ctx); closeErr != nil {
			return closeErr
		}
		m.mu.Lock()
		delete(m.owners, id)
		m.mu.Unlock()
		return err
	}
	if m.openPause != nil {
		if e = m.openPause.Observe(record, "build-succeeded"); e != nil {
			return retire(e)
		}
	}
	fresh, e := m.topology.Read(ctx)
	if e != nil {
		return retire(e)
	}
	if !m.desired(fresh.data, id) {
		err := retire(directory.ErrConflict)
		if m.openPause != nil && errors.Is(err, directory.ErrConflict) {
			if observeErr := m.openPause.Observe(record, "superseded-before-ready-retired"); observeErr != nil {
				return observeErr
			}
		}
		return err
	}
	if m.beforeReady != nil {
		m.beforeReady(record)
	}
	ready, e := d.Ready(ctx, reservation)
	if e != nil {
		return retire(e)
	}
	m.mu.Lock()
	closed = m.closed
	if !closed {
		m.owners[id] = &managed{owner, ready.Record()}
	}
	m.mu.Unlock()
	if closed {
		return retire(context.Canceled)
	}
	return nil
}
func (m *Manager) Resolve(ctx context.Context, id string, _ bool) (routing.Route, error) {
	t, e := m.topology.Read(ctx)
	if e != nil {
		return routing.Route{}, status.Error(codes.Unavailable, e.Error())
	}
	a, ok := t.data.Partitions[id]
	if !ok {
		return routing.Route{}, status.Error(codes.NotFound, "unknown partition")
	}
	d, e := m.directory(id, a.DataPrefix)
	if e != nil {
		return routing.Route{}, status.Error(codes.Unavailable, e.Error())
	}
	s, e := d.Read(ctx)
	if e != nil {
		return routing.Route{}, status.Error(codes.Unavailable, e.Error())
	}
	r := s.Record()
	member, ok := t.data.Members[a.Node]
	if !ok || r.State != "ready" || r.Node != a.Node || r.Incarnation != member.Incarnation || r.Address != member.Address {
		return routing.Route{}, status.Error(codes.Unavailable, "no activated ready owner")
	}
	return routing.Route{Node: r.Node + "/" + r.Incarnation, Address: r.Address}, nil
}
func (m *Manager) Owner(id string) (*node.Owner, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o := m.owners[id]
	if m.closed || o == nil || o.owner.Quarantined() {
		return nil, status.Error(codes.Unavailable, fmt.Sprintf("partition %s not locally ready", id))
	}
	return o.owner, nil
}

func (m *Manager) ownerConfig(id string) node.Config {
	c := node.DefaultConfig(id)
	c.MaxOutcomes = m.maxOutcomes
	return c
}
