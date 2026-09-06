package cluster

import (
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
	publish := func(expected registry.Version, w registry.Write) []Effect {
		e := Effect{Kind: PublishControl, Expected: expected, Write: w}
		effects := emit(e)
		if len(effects) != 0 {
			s.publication = &Publication{Effect: effects[0].clone()}
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
	confirm := func(transition identity.TransitionID) {
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
			if published, err := DecodeControl(pending.Key, event.Record, s.config.MaxControlBytes); err == nil {
				confirm(pending.Write.Transition)
				s.seenControl = true
				// Any publication with our coordinator updates renewal progress only if
				// it actually changed the coordinator tuple; placement must not starve it.
				c := published.Control()
				if !s.renewed || !s.haveSnapshot || c.Coordinator != s.snapshot.Control().Coordinator {
					s.lastRenew = s.at
					s.renewed = true
				}
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
			effects := publish("", w)
			return s, effects
		}
		return s, nil
	}
	s.seenControl = true
	c := snap.Control()
	if len(c.Partitions) != len(s.config.Slots) {
		s.LastError = ErrInvalidPlacement
		return s, nil
	}
	for _, id := range s.config.Slots {
		if _, ok := c.Partitions[id]; !ok {
			s.LastError = ErrInvalidPlacement
			return s, nil
		}
	}
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
		if !s.renewed || s.at-s.lastRenew >= Tick(s.config.RenewalInterval) {
			change, err = Renew(s.config.Incarnation, authority)
		} else {
			if len(s.members) == 0 {
				return s, nil
			}
			nodes := make([]identity.NodeID, len(s.members))
			for i, member := range s.members {
				nodes[i] = member.Node
			}
			plan, planErr := PlanPlacement(s.config.Placement, s.config.Slots, nodes)
			if planErr != nil {
				s.LastError = planErr
				return s, nil
			}
			move, ok, moveErr := SelectPlacementMove(s.config.Slots, c, plan, s.members)
			if moveErr != nil {
				s.LastError = moveErr
				return s, nil
			}
			if !ok {
				return s, nil
			}
			w, assignErr := snap.Assign(s.config.Incarnation, event.Transition, map[identity.PartitionID]Owner{move.Partition: move.Owner})
			if assignErr != nil {
				s.LastError = assignErr
				return s, nil
			}
			effects := publish(authority.Version, w)
			return s, effects
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
	effects := publish(authority.Version, w)
	return s, effects
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
