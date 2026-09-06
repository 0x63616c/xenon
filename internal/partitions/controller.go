package partitions

import (
	"errors"
	"math"

	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/registry"
)

// Step is the production decision function. It consumes explicit observations
// and emits ordered effects; no I/O, native handles, time or randomness enter it.
// Poll supplies retry/reconciliation cadence; no hidden periodic work is created.
func Step(previous State, event Event) (State, []Effect) {
	s := previous.clone()
	if s.pending == nil {
		s.LastError = ErrInvalid
		return s, nil
	}
	if event.Incarnation != s.config.Incarnation {
		return s, nil
	}
	if err := validEvent(event); err != nil {
		s.LastError = err
		return s, nil
	}
	effects := []Effect{}
	emit := func(e Effect) EffectID {
		if s.next == EffectID(math.MaxUint64) {
			s.LastError = ErrInvalid
			s.stopping = true
			return 0
		}
		s.next++
		e.ID = s.next
		e.Incarnation = s.config.Incarnation
		e.Key = s.config.Key
		s.pending[e.ID] = e.clone()
		effects = append(effects, e.clone())
		return e.ID
	}
	has := func(kind EffectKind) bool {
		for _, e := range s.pending {
			if e.Kind == kind {
				return true
			}
		}
		return false
	}
	read := func() {
		if !has(ReadControl) {
			emit(Effect{Kind: ReadControl, Handle: s.handle})
		}
	}
	closeHandle := func() {
		s.obsolete = true
		s.Phase = Closing
		if s.handle != 0 && !has(CloseEngine) {
			emit(Effect{Kind: CloseEngine, Handle: s.handle})
		}
	}
	publish := func() {
		p := s.publication
		p.retry = false
		emit(Effect{Kind: PublishControl, Expected: p.expected, Write: p.write})
	}
	current := func(a OpenRequest) bool {
		if !s.haveSnapshot {
			return false
		}
		p, ok := s.snapshot.Control().Partitions[s.config.Partition]
		return ok && p.Path == a.Path && p.Desired.Incarnation == a.Incarnation && p.AssignmentRevision == a.AssignmentRevision && p.Generation == a.Generation && p.Reservation == a.Reservation
	}
	openConfirmed := func() {
		if s.opening != 0 || s.handle != 0 || s.stopping {
			return
		}
		s.Phase = Opening
		s.obsolete = false
		s.opening = emit(Effect{Kind: OpenEngine, Open: s.attempt})
	}
	if event.Kind == Stop {
		s.stopping = true
		s.obsolete = true
		s.publication = nil
		if s.handle != 0 {
			closeHandle()
		}
		if len(s.pending) == 0 && s.handle == 0 {
			s.Phase = Stopped
		}
		return s, effects
	}
	if event.Kind == Fenced {
		if event.Handle == 0 || event.Handle != s.handle {
			return s, nil
		}
		s.LastError = ErrFenced
		s.publication = nil
		closeHandle()
		return s, effects
	}
	if event.Kind == Poll {
		if s.stopping {
			if s.handle != 0 {
				closeHandle()
			}
			if len(s.pending) == 0 && s.handle == 0 {
				s.Phase = Stopped
			}
			return s, effects
		}
		if s.Phase == Closing {
			if s.handle != 0 {
				closeHandle()
			}
			return s, effects
		}
		if !has(PublishControl) && !has(ReadControl) {
			if s.publication != nil && s.publication.retry {
				publish()
			} else {
				read()
			}
		}
		return s, effects
	}
	pending, ok := s.pending[event.Effect]
	if !ok || !matchesCompletion(pending.Kind, event.Kind) {
		return s, nil
	}
	delete(s.pending, event.Effect)
	if event.Kind == OpenCompleted {
		s.opening = 0
		if event.HasWriter {
			s.handle = event.Effect
		}
		if event.Err != nil {
			s.LastError = event.Err
			s.obsolete = true
		}
		if s.handle != 0 {
			if s.stopping || s.obsolete {
				closeHandle()
			} else {
				s.Phase = Activating
				read()
			}
		} else {
			s.Phase = Idle
		}
		if s.stopping && len(s.pending) == 0 && s.handle == 0 {
			s.Phase = Stopped
		}
		return s, effects
	}
	if event.Kind == CloseCompleted {
		if event.Err != nil && !errors.Is(event.Err, ErrFenced) {
			s.LastError = event.Err
			return s, nil
		}
		s.handle = 0
		s.obsolete = false
		s.publication = nil
		s.Phase = Idle
		if s.stopping {
			if len(s.pending) == 0 {
				s.Phase = Stopped
			}
		} else if !has(PublishControl) {
			read()
		}
		return s, effects
	}
	if s.stopping {
		if s.handle != 0 {
			closeHandle()
		}
		if len(s.pending) == 0 && s.handle == 0 {
			s.Phase = Stopped
		}
		return s, effects
	}
	if event.Kind == PublishCompleted {
		p := s.publication
		if p == nil || pending.Write.Transition != p.write.Transition || pending.Write.Digest != p.write.Digest || pending.Expected != p.expected {
			if s.handle == 0 && s.opening == 0 && !has(PublishControl) {
				read()
			}
			return s, effects
		}
		resolution, reconcileErr := registry.Reconcile(s.config.Key, p.expected, p.write, event.Record, event.Err)
		if event.Err == nil && resolution == registry.Published {
			snap, err := cluster.DecodeControl(s.config.Key, event.Record, s.config.MaxControlBytes)
			if err == nil {
				err = snap.ValidateLayout(s.config.ExpectedLayoutDigest)
			}
			if err != nil {
				s.LastError = err
				p.unknown = true
				read()
				return s, effects
			}
			s.snapshot = snap
			s.haveSnapshot = true
			s.publication = nil
			if p.ready {
				if s.handle != 0 && !s.obsolete && current(p.attempt) {
					s.Phase = Ready
				} else {
					closeHandle()
				}
			} else {
				s.attempt = p.attempt
				openConfirmed()
			}
			return s, effects
		}
		var unknown *registry.UnknownOutcome
		if errors.As(event.Err, &unknown) || p.unknown || event.Err == nil {
			p.unknown = true
			s.LastUnknown = &registry.UnknownOutcome{Key: s.config.Key, Transition: p.write.Transition, Cause: errors.Join(event.Err, reconcileErr)}
			s.LastError = s.LastUnknown
		} else {
			s.LastError = event.Err
		}
		read()
		return s, effects
	}
	// Read completions cannot authorize effects prepared against another version.
	// Every subsequent proposal uses this complete validated snapshot and its CAS.
	if event.Err != nil {
		s.LastError = event.Err
		return s, nil
	}
	snap, err := cluster.DecodeControl(s.config.Key, event.Record, s.config.MaxControlBytes)
	if err == nil {
		err = snap.ValidateLayout(s.config.ExpectedLayoutDigest)
	}
	if err != nil {
		s.LastError = err
		s.publication = nil
		s.obsolete = true
		if s.handle != 0 {
			closeHandle()
		}
		return s, effects
	}
	s.snapshot = snap
	s.haveSnapshot = true
	if s.obsolete && s.handle != 0 {
		closeHandle()
		return s, effects
	}
	part, exists := snap.Control().Partitions[s.config.Partition]
	assigned := exists && part.Desired.Incarnation == s.config.Incarnation
	if s.opening != 0 || s.handle != 0 {
		if !assigned || !current(s.attempt) {
			s.publication = nil
			s.obsolete = true
			if s.handle != 0 {
				closeHandle()
			}
			return s, effects
		}
	}
	if s.handle != 0 && pending.Handle != s.handle {
		read()
		return s, effects
	}
	if p := s.publication; p != nil {
		resolution, err := registry.Reconcile(s.config.Key, p.expected, p.write, event.Record, nil)
		// A retained reservation does not confirm its readiness publication.
		// If the prewrite version remains, preserve the exact ambiguous attempt.
		if p.ready && !part.Ready && resolution == registry.RetrySameWrite {
			p.retry = true
			return s, nil
		}
		// The retained tuple proves CURRENT reservation authority across unrelated
		// coordinator/renewal publications. It does not prove the old envelope's
		// historical outcome. Never replace it merely because that envelope changed.
		if current(p.attempt) {
			s.publication = nil
			if !p.ready {
				s.attempt = p.attempt
				openConfirmed()
				return s, effects
			}
			if part.Ready && s.handle != 0 && !s.obsolete {
				s.Phase = Ready
				return s, nil
			}
		} else if resolution == registry.RetrySameWrite {
			p.retry = true
			return s, nil // only an explicit Poll retries, retaining the ID
		} else {
			if p.unknown {
				s.LastUnknown = &registry.UnknownOutcome{Key: s.config.Key, Transition: p.write.Transition, Cause: err}
				s.LastError = s.LastUnknown
			}
			s.publication = nil
		}
	}
	if !assigned {
		s.Phase = Idle
		return s, nil
	}
	if s.opening != 0 {
		return s, nil
	}
	if s.handle != 0 && s.Phase == Ready {
		return s, nil
	}
	if event.Transition.Validate() != nil {
		s.LastError = ErrInvalid
		return s, nil
	}
	if s.handle != 0 {
		w, err := snap.MarkReady(s.config.Incarnation, s.config.Partition, s.attempt.AssignmentRevision, s.attempt.Generation, s.attempt.Reservation, event.Transition)
		if err != nil {
			s.LastError = err
			closeHandle()
			return s, effects
		}
		s.publication = &publication{expected: snap.Authority().Version, write: w, attempt: s.attempt, ready: true}
		s.Phase = Activating
		publish()
		return s, effects
	}
	w, err := snap.Reserve(s.config.Incarnation, s.config.Partition, part.AssignmentRevision, event.Transition)
	if err != nil {
		s.LastError = err
		return s, nil
	}
	attempt := OpenRequest{Path: part.Path, Partition: s.config.Partition, AssignmentRevision: part.AssignmentRevision, Reservation: event.Transition, Incarnation: s.config.Incarnation, Generation: part.Generation + 1}
	s.publication = &publication{expected: snap.Authority().Version, write: w, attempt: attempt}
	s.Phase = Reserving
	publish()
	return s, effects
}
func matchesCompletion(effect EffectKind, event EventKind) bool {
	return effect == ReadControl && event == ReadCompleted || effect == PublishControl && event == PublishCompleted || effect == OpenEngine && event == OpenCompleted || effect == CloseEngine && event == CloseCompleted
}
func validEvent(e Event) error {
	switch e.Kind {
	case Poll, Stop:
		if e.Effect != 0 || e.Handle != 0 || e.HasWriter || e.Err != nil || e.Record.Version != "" || len(e.Record.Body) != 0 || e.Transition != "" {
			return ErrInvalid
		}
	case Fenced:
		if e.Handle == 0 || e.Effect != 0 || e.HasWriter || e.Err != nil || e.Record.Version != "" || len(e.Record.Body) != 0 || e.Transition != "" {
			return ErrInvalid
		}
	case ReadCompleted:
		if e.Effect == 0 || e.Handle != 0 || e.HasWriter {
			return ErrInvalid
		}
	case PublishCompleted:
		if e.Effect == 0 || e.Handle != 0 || e.HasWriter || e.Transition != "" {
			return ErrInvalid
		}
	case OpenCompleted:
		if e.Effect == 0 || e.Handle != 0 || e.Record.Version != "" || len(e.Record.Body) != 0 || e.Transition != "" || !e.HasWriter && e.Err == nil {
			return ErrInvalid
		}
	case CloseCompleted:
		if e.Effect == 0 || e.Handle != 0 || e.HasWriter || e.Record.Version != "" || len(e.Record.Body) != 0 || e.Transition != "" {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
