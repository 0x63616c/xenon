// Package cluster owns membership, coordinator election and placement decisions.
package cluster

import (
	"errors"
	"math"
	"time"

	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
)

// Tick is process-local monotonic elapsed nanoseconds. Never persist it or
// compare it with another incarnation's clock.
type Tick int64

// Authority is the coordinator portion of a coherent control-record snapshot.
// Version covers the ENTIRE control record, including assignments and owners.
type Authority struct {
	Version     registry.Version
	Incarnation identity.IncarnationID
	Generation  uint64
	Renewal     uint64
}

func (a Authority) validate() error {
	if a.Version == "" || a.Generation == 0 || a.Incarnation.Validate() != nil {
		return errors.New("invalid coordinator authority")
	}
	return nil
}

// Election is a value-only suspicion observer. The driver serializes registry
// reads, calls Observe on completion, and obtains a fresh read before Propose.
// No wall clock, goroutine, network call or randomness influences this state.
// A new process must start with a zero Election; it cannot restore these ticks.
type Election struct {
	seen      bool
	authority Authority
	since     Tick
	last      Tick
}

func sameLeader(a, b Authority) bool {
	return a.Incarnation == b.Incarnation && a.Generation == b.Generation && a.Renewal == b.Renewal
}

// Observe resets suspicion only on coordinator progress, not unrelated owner
// edits. Regressing clocks and malformed observations leave state unchanged.
func (s Election) Observe(at Tick, a Authority) (Election, error) {
	if err := a.validate(); err != nil {
		return s, err
	}
	if at < 0 || (s.seen && at < s.last) {
		return s, errors.New("regressing local tick")
	}
	if !s.seen || !sameLeader(s.authority, a) {
		s.since = at
	}
	s.seen, s.authority, s.last = true, a, at
	return s, nil
}

// CoordinatorChange is a proposal, never authority to open a database or serve writes.
// Replace only the coordinator fields of the EXACT reread control record and
// publish the whole record using Expected. CAS failure requires another read;
// never substitute a newer version into an old proposal. Unknown publication
// must be reconciled through the registry transition ID before acting as leader.
type CoordinatorChange struct {
	Expected    registry.Version
	Incarnation identity.IncarnationID
	Generation  uint64
	Renewal     uint64
}

// Propose requires a fresh coherent reread after suspicion. A changed renewal
// rejects the proposal even if the suspicion timer fired. A wrong suspicion is
// allowed: the storage CAS, not failure detection accuracy, decides the winner.
func (s Election) Propose(at Tick, after time.Duration, self identity.IncarnationID, reread Authority) (CoordinatorChange, bool, error) {
	if err := reread.validate(); err != nil {
		return CoordinatorChange{}, false, err
	}
	if self.Validate() != nil || after <= 0 || at < 0 || (s.seen && at < s.last) {
		return CoordinatorChange{}, false, errors.New("invalid election input")
	}
	if !s.seen || self == reread.Incarnation || !sameLeader(s.authority, reread) || at-s.since < Tick(after) {
		return CoordinatorChange{}, false, nil
	}
	if reread.Generation == math.MaxUint64 {
		return CoordinatorChange{}, false, errors.New("coordinator generation exhausted")
	}
	return CoordinatorChange{Expected: reread.Version, Incarnation: self, Generation: reread.Generation + 1}, true, nil
}

// Renew can only be prepared from a snapshot that still names this incarnation.
// Its conditional version also protects against a concurrently elected leader.
func Renew(self identity.IncarnationID, snapshot Authority) (CoordinatorChange, error) {
	if err := snapshot.validate(); err != nil {
		return CoordinatorChange{}, err
	}
	if self != snapshot.Incarnation {
		return CoordinatorChange{}, errors.New("coordinator authority lost")
	}
	if snapshot.Renewal == math.MaxUint64 {
		return CoordinatorChange{}, errors.New("coordinator renewal exhausted")
	}
	return CoordinatorChange{Expected: snapshot.Version, Incarnation: self, Generation: snapshot.Generation, Renewal: snapshot.Renewal + 1}, nil
}
