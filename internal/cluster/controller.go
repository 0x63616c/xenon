package cluster

import (
	"encoding/json"
	"errors"
	"math"
	"slices"

	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
)

// Step consumes explicit local time and completions. Every new decision follows
// an authoritative read; success responses are receipts, never reusable authority.
// At most one control CAS is pending. No engine opens or background timers exist.
func Step(previous State, event Event) (State, []Effect) {
	s := previous.clone()
	if event.Incarnation != s.config.Incarnation {
		return s, nil
	}
	if s.config.Incarnation.Validate() != nil || !validEvent(event) {
		s.LastError = ErrInvalidController
		return s, nil
	}
	// Ignore duplicate/old completions before accepting their clock value.
	if event.Kind == ReadCompleted || event.Kind == PublishCompleted {
		if s.pending == nil || s.pending.ID != event.Effect || (s.pending.Kind == ReadControl) != (event.Kind == ReadCompleted) {
			return s, nil
		}
	}
	if event.At < s.at {
		s.LastError = ErrInvalidController
		return s, nil
	}
	s.at = event.At
	emit := func(e Effect) []Effect {
		if s.next == EffectID(math.MaxUint64) {
			s.LastError = ErrInvalidController
			s.stopping = true
			return nil
		}
		s.next++
		e.ID = s.next
		e.Key = s.config.Key
		e.Incarnation = s.config.Incarnation
		s.pending = &e
		return []Effect{e.clone()}
	}
	publish := func(expected registry.Version, w registry.Write, renewal bool) []Effect {
		e := Effect{Kind: PublishControl, Expected: expected, Write: w}
		effects := emit(e)
		if len(effects) != 0 {
			s.publication = &Publication{Effect: effects[0].clone(), renewal: renewal}
		}
		return effects
	}
	if event.Kind == Stop {
		s.stopping = true
		return s, nil
	}
	if event.Kind == Poll {
		if s.stopping {
			return s, nil
		}
		if !validMembers(event.Members) {
			s.LastError = ErrInvalidController
			return s, nil
		}
		s.members = slices.Clone(event.Members)
		if s.pending == nil {
			effects := emit(Effect{Kind: ReadControl})
			return s, effects
		}
		return s, nil
	}
	pending := s.pending.clone()
	s.pending = nil
	recordRenewal := func() {
		s.lastRenew = s.at
		s.renewed = true
		s.moveCredit = true
	}
	confirm := func(transition identity.TransitionID) {
		if s.publication != nil && s.publication.renewal {
			recordRenewal()
		}
		if s.LastUnknown != nil && s.LastUnknown.Effect.Write.Transition == transition {
			s.LastUnknown = nil
		}
		s.publication = nil
	}
	// Reject malformed/oversized reads before using them to reconcile history.
	var snap Snapshot
	if event.Kind == ReadCompleted && event.Err == nil && !s.stopping {
		var err error
		snap, err = DecodeControl(s.config.Key, event.Record, s.config.MaxControlBytes)
		if err == nil {
			err = snap.ValidateLayout(s.config.ExpectedLayoutDigest)
		}
		if err != nil {
			s.LastError = err
			return s, nil
		}
	}
	if event.Kind == PublishCompleted {
		p := s.publication
		var resolution registry.Resolution
		var reconcileErr error
		if len(event.Record.Body) > s.config.MaxControlBytes {
			reconcileErr = ErrControlLimit
		} else {
			resolution, reconcileErr = registry.Reconcile(pending.Key, pending.Expected, pending.Write, event.Record, event.Err)
		}
		if event.Err == nil && resolution == registry.Published {
			// Decoding also enforces the configured record bound and control schema.
			published, err := DecodeControl(pending.Key, event.Record, s.config.MaxControlBytes)
			if err == nil {
				err = published.ValidateLayout(s.config.ExpectedLayoutDigest)
			}
			if err == nil {
				confirm(pending.Write.Transition)
				s.seenControl = true

				s.LastError = nil
				return s, nil
			} else {
				reconcileErr = err
			}
		}
		var unknown *registry.UnknownOutcome
		if errors.As(event.Err, &unknown) || p.Unknown != nil || event.Err == nil {
			p.Unknown = &registry.UnknownOutcome{Key: pending.Key, Transition: pending.Write.Transition, Cause: errors.Join(event.Err, reconcileErr)}
			s.LastUnknown = p.clone()
			s.LastError = p.Unknown
		} else {
			s.LastError = event.Err
			s.publication = nil
		}
		return s, nil // explicit Poll controls retry cadence, including during outages
	}
	if s.stopping {
		s.LastError = event.Err
		return s, nil
	}
	// Resolve the exact prior ambiguous attempt before considering fresh work.
	if p := s.publication; p != nil {
		resolution, _ := registry.Reconcile(p.Effect.Key, p.Effect.Expected, p.Effect.Write, event.Record, event.Err)
		if resolution == registry.RetrySameWrite {
			effects := emit(p.Effect.clone()) // original bytes, transition and condition
			return s, effects
		}
		if resolution == registry.Published {
			confirm(p.Effect.Write.Transition)
		} else if event.Err == nil {
			// A coherent newer record allows a NEW decision. It says nothing about
			// the historical attempt retained in LastUnknown.
			if p.renewal {
				var proposed Control
				// Owner publications can replace the envelope while retaining this
				// coordinator tuple. It proves CURRENT renewal progress, not the
				// historical publication receipt; preserve LastUnknown below.
				if json.Unmarshal(p.Effect.Write.Body, &proposed) == nil && proposed.Coordinator.Incarnation == s.config.Incarnation && proposed.Coordinator == snap.Control().Coordinator {
					recordRenewal()
				}
			}
			s.publication = nil
		}
	}
	if event.Err != nil {
		s.LastError = event.Err
		var missing *registry.NotFound
		if errors.As(event.Err, &missing) && missing.Key == s.config.Key && s.config.FreshNamespace && !s.seenControl && s.publication == nil {
			w, err := BootstrapWrite(s.config.Key, event.Transition, *s.config.Bootstrap, s.config.MaxControlBytes)
			if err != nil {
				s.LastError = err
				return s, nil
			}
			effects := publish("", w, true)
			return s, effects
		}
		return s, nil
	}
	s.seenControl = true
	authority := snap.Authority()
	change, takeover, err := s.election.Propose(s.at, s.config.SuspectAfter, s.config.Incarnation, authority)
	if err != nil {
		s.LastError = err
		return s, nil
	}
	s.election, err = s.election.Observe(s.at, authority)
	if err != nil {
		s.LastError = err
		return s, nil
	}
	s.snapshot, s.haveSnapshot = snap, true
	s.LastError = nil
	if authority.Incarnation == s.config.Incarnation {
		// One confirmed renewal grants at most one placement attempt, even when
		// every Poll arrives after the renewal interval. Consuming the credit on
		// dispatch also prevents repeated assignment conflicts starving renewal.
		if !s.renewed || s.at-s.lastRenew >= Tick(s.config.RenewalInterval) && !s.moveCredit {
			change, err = Renew(s.config.Incarnation, authority)
		} else {
			var w registry.Write
			var move bool
			w, move, err = s.plan(snap, event.Transition)
			if err != nil {
				s.LastError = err
				return s, nil
			}
			if move {
				s.moveCredit = false
				effects := publish(authority.Version, w, false)
				return s, effects
			}
			if s.at-s.lastRenew < Tick(s.config.RenewalInterval) {
				return s, nil
			}
			change, err = Renew(s.config.Incarnation, authority)
		}
	} else if !takeover {
		s.renewed = false
		return s, nil
	}
	if err != nil {
		s.LastError = err
		return s, nil
	}
	w, err := snap.ChangeCoordinator(event.Transition, change)
	if err != nil {
		s.LastError = err
		return s, nil
	}
	effects := publish(authority.Version, w, true)
	return s, effects
}

// plan uses the same fresh snapshot as election; no cached authority is rebased.
func (s State) plan(snap Snapshot, transition identity.TransitionID) (registry.Write, bool, error) {
	if len(s.members) == 0 {
		return registry.Write{}, false, nil
	}
	nodes := make([]identity.NodeID, len(s.members))
	for i, member := range s.members {
		nodes[i] = member.Node
	}
	layout := snap.Control().Layout
	plan, err := PlanPlacement(layout.Placement, layout.slots(), nodes)
	if err != nil {
		return registry.Write{}, false, err
	}
	move, ok, err := SelectPlacementMove(layout.slots(), snap.Control(), plan, s.members)
	if err != nil || !ok {
		return registry.Write{}, false, err
	}
	w, err := snap.Assign(s.config.Incarnation, transition, map[identity.PartitionID]Owner{move.Partition: move.Owner})
	return w, err == nil, err
}

func validMembers(members []Owner) bool {
	seen := make(map[identity.NodeID]bool, len(members))
	for _, m := range members {
		if !m.valid() || seen[m.Node] {
			return false
		}
		seen[m.Node] = true
	}
	return true
}
func validEvent(e Event) bool {
	if e.At < 0 {
		return false
	}
	switch e.Kind {
	case Poll:
		return e.Effect == 0 && e.Transition == "" && e.Record.Version == "" && len(e.Record.Body) == 0 && e.Err == nil
	case Stop:
		return e.Effect == 0 && len(e.Members) == 0 && e.Transition == "" && e.Record.Version == "" && len(e.Record.Body) == 0 && e.Err == nil
	case ReadCompleted:
		return e.Effect != 0 && len(e.Members) == 0
	case PublishCompleted:
		return e.Effect != 0 && len(e.Members) == 0 && e.Transition == ""
	default:
		return false
	}
}
