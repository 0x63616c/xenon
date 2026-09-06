package ownership

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/0x63616c/xenon/internal/directory"
)

// FailureTimeout is a liveness policy only. It never grants writer authority;
// conditional topology publication, the owner directory, and SlateDB fencing do.
const FailureTimeout = 15 * time.Second

type observation struct {
	member Member
	since  time.Time
}

// Membership advances this process heartbeat and removes one unchanged peer
// after a local monotonic suspicion interval. Step accepts time explicitly so
// production decisions can run unchanged under deterministic simulation.
type Membership struct {
	store        *TopologyStore
	identity     directory.Identity
	observed     map[string]observation
	failureAfter time.Duration
}

func NewMembership(store *TopologyStore, identity directory.Identity, failureAfter time.Duration) (*Membership, error) {
	if store == nil || identity.Node == "" || identity.Address == "" || identity.Incarnation == "" || failureAfter <= 0 {
		return nil, directory.ErrInvalid
	}
	return &Membership{store: store, identity: identity, observed: map[string]observation{}, failureAfter: failureAfter}, nil
}

func (m *Membership) Step(ctx context.Context, now time.Time) error {
	for attempt := 0; attempt < 8; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		snapshot, err := m.store.Read(ctx)
		if err != nil {
			return err
		}
		current, ok := snapshot.data.Members[m.identity.Node]
		if !ok || current.Address != m.identity.Address || current.Incarnation != m.identity.Incarnation {
			return directory.ErrConflict
		}
		m.observe(snapshot.data, now)
		next := snapshot.Record()
		current.Heartbeat++
		if current.Heartbeat == 0 {
			return directory.ErrInvalid
		}
		next.Members[m.identity.Node] = current
		if candidate := m.expired(snapshot.data, now); candidate != "" {
			delete(next.Members, candidate)
			delete(m.observed, candidate)
			rebalance(&next)
		}
		if _, err = m.store.Publish(ctx, &snapshot, next); err == nil {
			return nil
		} else if !errors.Is(err, directory.ErrConflict) {
			return err
		}
	}
	return directory.ErrConflict
}

func (m *Membership) observe(topology Topology, now time.Time) {
	for node := range m.observed {
		if _, ok := topology.Members[node]; !ok {
			delete(m.observed, node)
		}
	}
	for node, member := range topology.Members {
		if node == m.identity.Node {
			continue
		}
		old, ok := m.observed[node]
		if !ok || old.member != member {
			m.observed[node] = observation{member: member, since: now}
		}
	}
}

func (m *Membership) expired(topology Topology, now time.Time) string {
	var candidates []string
	for node, seen := range m.observed {
		if current, ok := topology.Members[node]; ok && current == seen.member && now.Sub(seen.since) >= m.failureAfter {
			candidates = append(candidates, node)
		}
	}
	sort.Strings(candidates)
	if len(candidates) == 0 {
		return ""
	}
	return candidates[0]
}
