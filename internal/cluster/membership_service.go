package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
)

var ErrMembership = errors.New("invalid or incomplete advisory membership")
var ErrMembershipFull = errors.New("advisory registration index is full")

type MembershipConfig struct {
	Prefix               registry.Key
	Cluster              identity.ClusterID
	ExpectedLayoutDigest [32]byte
	Self                 Owner
	FailureAfter         time.Duration
	MaxEntries           int
	MaxRecordBytes       int
	ReadsPerScan         int
}
type membershipIndex struct {
	Format  int                `json:"format"`
	Cluster identity.ClusterID `json:"cluster"`
	Layout  [32]byte           `json:"layout"`
	Entries []Owner            `json:"entries"`
}
type heartbeat struct {
	Format   int                `json:"format"`
	Cluster  identity.ClusterID `json:"cluster"`
	Layout   [32]byte           `json:"layout"`
	Owner    Owner              `json:"owner"`
	Sequence uint64             `json:"sequence"`
}
type membershipAttempt struct {
	key      registry.Key
	expected registry.Version
	write    registry.Write
}
type memberObservation struct {
	owner      Owner
	sequence   uint64
	since      Tick
	progressed bool
}
type membershipScan struct {
	index   membershipIndex
	record  registry.Record
	cursor  int
	started Tick
}

// Membership performs bounded synchronous registry work. Its host serializes calls,
// supplies monotonic ticks and contexts, and calls Heartbeat independently between
// Discover batches. Only the current coordinator calls Discover. No wall clock,
// timer, background goroutine, remote timestamp, or authority mutation is used.
// Historical heartbeat objects remain until separately reviewed offline cleanup.
type Membership struct {
	config      MembershipConfig
	store       registry.Store
	ids         identity.Source
	pending     map[registry.Key]membershipAttempt
	LastUnknown *registry.UnknownOutcome
	at          Tick
	tenure      Coordinator
	observed    map[identity.IncarnationID]memberObservation
	scan        *membershipScan
}

func NewMembership(c MembershipConfig, store registry.Store, ids identity.Source) (*Membership, error) {
	if store == nil || ids == nil || registry.ValidateKey(c.Prefix) != nil || c.Cluster.Validate() != nil || c.ExpectedLayoutDigest == ([32]byte{}) || !c.Self.valid() || c.FailureAfter <= 0 || c.MaxEntries <= 0 || c.MaxRecordBytes <= 0 || c.ReadsPerScan <= 0 || c.ReadsPerScan > c.MaxEntries {
		return nil, ErrMembership
	}
	if registry.ValidateKey(registry.Key(string(c.Prefix)+"/"+string(c.Self.Node)+"/"+string(c.Self.Incarnation))) != nil {
		return nil, ErrMembership
	}
	return &Membership{config: c, store: store, ids: ids, pending: map[registry.Key]membershipAttempt{}, observed: map[identity.IncarnationID]memberObservation{}}, nil
}
func (m *Membership) indexKey() registry.Key { return registry.Key(string(m.config.Prefix) + "/index") }
func (m *Membership) heartbeatKey(o Owner) registry.Key {
	return registry.Key(string(m.config.Prefix) + "/" + string(o.Node) + "/" + string(o.Incarnation))
}
func (m *Membership) tick(at Tick) error {
	if at < 0 || at < m.at {
		return ErrMembership
	}
	m.at = at
	return nil
}
func (m *Membership) decode(key registry.Key, r registry.Record, into any) error {
	if len(r.Body) > m.config.MaxRecordBytes {
		return ErrMembership
	}
	env, err := registry.Decode(key, r)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(env.Body, into); err != nil {
		return err
	}
	canonical, err := json.Marshal(into)
	if err != nil || !bytes.Equal(canonical, env.Body) {
		return ErrMembership
	}
	return nil
}
func (m *Membership) readIndex(ctx context.Context) (membershipIndex, registry.Record, error) {
	r, err := m.read(ctx, m.indexKey())
	if err != nil {
		return membershipIndex{}, r, err
	}
	var index membershipIndex
	if err = m.decode(m.indexKey(), r, &index); err != nil {
		return index, r, err
	}
	if index.Format != 1 || index.Cluster != m.config.Cluster || index.Layout != m.config.ExpectedLayoutDigest || len(index.Entries) > m.config.MaxEntries {
		return index, r, ErrMembership
	}
	seen := map[identity.IncarnationID]bool{}
	for _, o := range index.Entries {
		if !o.valid() || seen[o.Incarnation] {
			return index, r, ErrMembership
		}
		seen[o.Incarnation] = true
	}
	return index, r, nil
}
func (m *Membership) readHeartbeat(ctx context.Context, o Owner) (heartbeat, error) {
	key := m.heartbeatKey(o)
	r, err := m.read(ctx, key)
	if err != nil {
		return heartbeat{}, err
	}
	var h heartbeat
	if err = m.decode(key, r, &h); err != nil {
		return h, err
	}
	if h.Format != 1 || h.Cluster != m.config.Cluster || h.Layout != m.config.ExpectedLayoutDigest || h.Owner != o || h.Sequence == 0 {
		return h, ErrMembership
	}
	return h, nil
}

// read resolves an exact pending write before a new proposal. A newer record can
// support a fresh decision but never changes an unresolved historical outcome to
// failure. Original-version retries retain the original condition/body/identity.
func (m *Membership) read(ctx context.Context, key registry.Key) (registry.Record, error) {
	r, err := m.store.Read(ctx, key)
	p, ok := m.pending[key]
	if !ok {
		return r, err
	}
	resolution, _ := registry.Reconcile(key, p.expected, p.write, r, err)
	switch resolution {
	case registry.Published:
		delete(m.pending, key)
		if m.LastUnknown != nil && m.LastUnknown.Transition == p.write.Transition {
			m.LastUnknown = nil
		}
		return r, nil
	case registry.RetrySameWrite:
		return m.dispatch(ctx, p)
	default:
		if err == nil {
			delete(m.pending, key)
		}
		return r, err
	}
}
func (m *Membership) dispatch(ctx context.Context, p membershipAttempt) (registry.Record, error) {
	var r registry.Record
	var err error
	if p.expected == "" {
		r, err = m.store.Create(ctx, p.key, p.write)
	} else {
		r, err = m.store.Replace(ctx, p.key, p.expected, p.write)
	}
	var unknown *registry.UnknownOutcome
	if errors.As(err, &unknown) {
		m.pending[p.key] = p
		m.LastUnknown = unknown
	} else if err == nil {
		delete(m.pending, p.key)
	}
	return r, err
}
func (m *Membership) publish(ctx context.Context, key registry.Key, expected registry.Version, body any) error {
	if _, ok := m.pending[key]; ok {
		return ErrMembership
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	id, err := identity.NewTransitionID(m.ids)
	if err != nil {
		return err
	}
	w, err := registry.NewWrite(key, expected, id, raw)
	if err != nil {
		return err
	}
	encoded, err := registry.Encode(key, expected, w)
	if err != nil {
		return err
	}
	if len(encoded) > m.config.MaxRecordBytes {
		return ErrMembership
	}
	_, err = m.dispatch(ctx, membershipAttempt{key, expected, w})
	return err
}

// Heartbeat publishes only this incarnation's sequence, then verifies/rejoins the
// index. An uncertain publication stops this call. Full index admission is an
// explicit error; no membership or writer authority is inferred from it.
func (m *Membership) Heartbeat(ctx context.Context, at Tick) error {
	if err := m.tick(at); err != nil {
		return err
	}
	key := m.heartbeatKey(m.config.Self)
	r, err := m.read(ctx, key)
	h := heartbeat{1, m.config.Cluster, m.config.ExpectedLayoutDigest, m.config.Self, 1}
	var missing *registry.NotFound
	if err == nil {
		if err = m.decode(key, r, &h); err != nil {
			return err
		}
		if h.Format != 1 || h.Cluster != m.config.Cluster || h.Layout != m.config.ExpectedLayoutDigest || h.Owner != m.config.Self || h.Sequence == 0 || h.Sequence == math.MaxUint64 {
			return ErrMembership
		}
		h.Sequence++
	} else if !errors.As(err, &missing) {
		return err
	}
	if err = m.publish(ctx, key, r.Version, h); err != nil {
		return err
	}
	index, r, err := m.readIndex(ctx)
	if errors.As(err, &missing) {
		index = membershipIndex{Format: 1, Cluster: m.config.Cluster, Layout: m.config.ExpectedLayoutDigest}
	} else if err != nil {
		return err
	}
	for _, o := range index.Entries {
		if o.Incarnation == m.config.Self.Incarnation {
			if o != m.config.Self {
				return ErrMembership
			}
			return nil
		}
	}
	if len(index.Entries) >= m.config.MaxEntries {
		return ErrMembershipFull
	}
	index.Entries = append(index.Entries, m.config.Self)
	slices.SortFunc(index.Entries, func(a, b Owner) int {
		if a.Incarnation < b.Incarnation {
			return -1
		}
		if a.Incarnation > b.Incarnation {
			return 1
		}
		return 0
	})
	return m.publish(ctx, m.indexKey(), r.Version, index)
}

// Discover scans at most ReadsPerScan heartbeats per call. Every new tenure starts
// without a placement view. Failed/missing reads invalidate the entire scan; a
// partial scan never acts as an empty or singleton fleet. First reads establish
// baselines, and only strictly increasing sequences establish eligibility.
func (m *Membership) Discover(ctx context.Context, at Tick, c Coordinator) (MembershipView, error) {
	empty := MembershipView{}
	if err := m.tick(at); err != nil {
		return empty, err
	}
	if c.Incarnation != m.config.Self.Incarnation || c.Generation == 0 {
		m.scan = nil
		m.observed = map[identity.IncarnationID]memberObservation{}
		m.tenure = Coordinator{}
		return empty, ErrMembership
	}
	if c.Incarnation != m.tenure.Incarnation || c.Generation != m.tenure.Generation {
		m.scan = nil
		m.observed = map[identity.IncarnationID]memberObservation{}
		m.tenure = c
	}
	if m.scan != nil && at-m.scan.started >= Tick(m.config.FailureAfter) {
		m.scan = nil
		return empty, fmt.Errorf("%w: scan exceeded suspicion interval", ErrMembership)
	}
	if m.scan == nil {
		index, r, err := m.readIndex(ctx)
		if err != nil {
			return empty, err
		}
		m.scan = &membershipScan{index: index, record: r, started: at}
	}
	scan := m.scan
	for n := 0; n < m.config.ReadsPerScan && scan.cursor < len(scan.index.Entries); n++ {
		o := scan.index.Entries[scan.cursor]
		scan.cursor++
		h, err := m.readHeartbeat(ctx, o)
		if err != nil {
			m.scan = nil
			return empty, err
		}
		old, ok := m.observed[o.Incarnation]
		if ok && (old.owner != o || h.Sequence < old.sequence) {
			m.scan = nil
			old.progressed = false
			m.observed[o.Incarnation] = old
			return empty, ErrMembership
		}
		if !ok {
			old = memberObservation{owner: o, sequence: h.Sequence, since: at}
		} else if h.Sequence > old.sequence {
			old.sequence = h.Sequence
			old.since = at
			old.progressed = true
		}
		m.observed[o.Incarnation] = old
	}
	if scan.cursor < len(scan.index.Entries) {
		return empty, nil
	}
	m.scan = nil
	view := MembershipView{Coordinator: c.Incarnation, Generation: c.Generation, Ready: true}
	counts := map[identity.NodeID]int{}
	present := map[identity.IncarnationID]bool{}
	var expired []Owner
	for _, o := range scan.index.Entries {
		present[o.Incarnation] = true
		seen := m.observed[o.Incarnation]
		if at-seen.since >= Tick(m.config.FailureAfter) {
			expired = append(expired, o)
			continue
		}
		if !seen.progressed {
			view.Ready = false
		}
		counts[o.Node]++
	}
	for inc := range m.observed {
		if !present[inc] {
			delete(m.observed, inc)
		}
	}
	if view.Ready {
		for _, o := range scan.index.Entries {
			seen := m.observed[o.Incarnation]
			if counts[o.Node] == 1 && seen.progressed && at-seen.since < Tick(m.config.FailureAfter) {
				view.Members = append(view.Members, o)
			}
		}
	}
	// Prune at most one exact index entry from this scan. Re-read its heartbeat
	// before dispatch; a conflict discards observations so a later scan must
	// establish a new suspicion baseline (never rebase an old prune decision).
	if len(expired) > 0 {
		o := expired[0]
		h, err := m.readHeartbeat(ctx, o)
		if err != nil {
			return empty, err
		}
		if h.Sequence == m.observed[o.Incarnation].sequence {
			next := scan.index
			next.Entries = slices.Clone(next.Entries)
			next.Entries = slices.DeleteFunc(next.Entries, func(x Owner) bool { return x == o })
			err = m.publish(ctx, m.indexKey(), scan.record.Version, next)
			delete(m.observed, o.Incarnation)
			if err != nil {
				return empty, err
			}
		} else {
			delete(m.observed, o.Incarnation)
			return empty, nil
		}
	}
	return view, nil
}
