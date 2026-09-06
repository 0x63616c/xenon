package cluster

import (
	"context"
	"sync"

	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
)

// Service serializes the controller and owns dispatched registry calls through
// completion. Its host supplies Poll ticks and eligibility; there is no real-time
// polling, suspicion or timeout here. Stop cancels work without assuming rollback;
// Drain uses the caller's explicit cancellation/budget and retains late outcomes.
type Service struct {
	mu      sync.Mutex
	state   State
	store   registry.Store
	ids     identity.Source
	ctx     context.Context
	cancel  context.CancelFunc
	changed chan struct{}
}

func NewService(ctx context.Context, c ControllerConfig, store registry.Store, ids identity.Source) (*Service, error) {
	if ctx == nil || store == nil || ids == nil {
		return nil, ErrInvalidController
	}
	state, err := NewState(c)
	if err != nil {
		return nil, err
	}
	work, cancel := context.WithCancel(ctx)
	return &Service{state: state, store: store, ids: ids, ctx: work, cancel: cancel, changed: make(chan struct{})}, nil
}
func (s *Service) Snapshot() State { s.mu.Lock(); defer s.mu.Unlock(); return s.state.clone() }
func (s *Service) Poll(at Tick, view MembershipView) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applyLocked(Event{Kind: Poll, At: at, Membership: view, Incarnation: s.state.config.Incarnation})
}
func (s *Service) Stop() {
	s.mu.Lock()
	s.applyLocked(Event{Kind: Stop, At: s.state.at, Incarnation: s.state.config.Incarnation})
	s.mu.Unlock()
	s.cancel()
}
func (s *Service) applyLocked(event Event) {
	next, effects := Step(s.state, event)
	s.state = next
	close(s.changed)
	s.changed = make(chan struct{})
	for _, effect := range effects {
		go s.execute(effect)
	}
}
func (s *Service) execute(effect Effect) {
	event := Event{Incarnation: effect.Incarnation, Effect: effect.ID}
	switch effect.Kind {
	case ReadControl:
		event.Kind = ReadCompleted
		event.Record, event.Err = s.store.Read(s.ctx, effect.Key)
		// One registry effect at a time also serializes the injected entropy source.
		// Missing keys may need an explicitly configured bootstrap transition.
		id, err := s.ids.NewID("trn")
		if err != nil {
			// Preserve read failures such as NotFound if entropy also fails, but prevent
			// proposals by leaving Transition empty.
			if event.Err == nil {
				event.Err = err
			}
		} else {
			event.Transition = identity.TransitionID(id)
		}
	case PublishControl:
		event.Kind = PublishCompleted
		if effect.Expected == "" {
			event.Record, event.Err = s.store.Create(s.ctx, effect.Key, effect.Write)
		} else {
			event.Record, event.Err = s.store.Replace(s.ctx, effect.Key, effect.Expected, effect.Write)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Time advances only on explicit host Polls. Completion observes that latest
	// supplied tick, never time.Now or an untracked process clock.
	event.At = s.state.at
	event.Record = event.Record.Clone()
	s.applyLocked(event)
}
func (s *Service) Drain(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidController
	}
	for {
		s.mu.Lock()
		done := s.state.Stopped()
		changed := s.changed
		s.mu.Unlock()
		if done {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
