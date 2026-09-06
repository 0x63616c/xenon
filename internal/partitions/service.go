package partitions

import (
	"context"
	"errors"
	"sync"

	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
)

// Service owns one partition's asynchronous effects and native writer handles.
// Its caller drives Poll explicitly (or with an injected timer). No polling,
// retry, watchdog or shutdown duration is chosen here. Call Stop then Drain;
// Drain timeout reports pending work and never frees an in-use native handle.
type Service struct {
	mu      sync.Mutex
	state   State
	store   registry.Store
	engine  Engine
	ids     identity.Source
	idsMu   sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	writers map[EffectID]Writer
	changed chan struct{}
}

func NewService(ctx context.Context, c ControllerConfig, store registry.Store, engine Engine, ids identity.Source) (*Service, error) {
	if ctx == nil || store == nil || engine == nil || ids == nil {
		return nil, ErrInvalid
	}
	state, err := NewState(c)
	if err != nil {
		return nil, err
	}
	work, cancel := context.WithCancel(ctx)
	return &Service{state: state, store: store, engine: engine, ids: ids, ctx: work, cancel: cancel, writers: map[EffectID]Writer{}, changed: make(chan struct{})}, nil
}

// Snapshot owns its pending/write bytes. Ready is only a cached observation;
// persistence admission must validate authoritative control and native fencing.
func (s *Service) Snapshot() State { s.mu.Lock(); defer s.mu.Unlock(); return s.state.clone() }

// Writer returns a borrowed ready handle and its exact reservation tuple. This is
// not a lease: callers must independently validate control at admission and must
// report ErrFenced via ObserveFence. The native writer protects close while in use.
func (s *Service) Writer() (Writer, OpenRequest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.writers[s.state.handle]
	return w, s.state.attempt, s.state.Phase == Ready && w != nil && !s.state.stopping
}
func (s *Service) Poll()                        { s.external(Event{Kind: Poll}) }
func (s *Service) ObserveFence(handle EffectID) { s.external(Event{Kind: Fenced, Handle: handle}) }
func (s *Service) Stop()                        { s.external(Event{Kind: Stop}); s.cancel() }
func (s *Service) external(event Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	event.Incarnation = s.state.config.Incarnation
	s.applyLocked(event)
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
	var opened Writer
	switch effect.Kind {
	case ReadControl:
		event.Kind = ReadCompleted
		event.Record, event.Err = s.store.Read(s.ctx, effect.Key)
		if event.Err == nil {
			s.idsMu.Lock()
			id, err := s.ids.NewID("trn")
			s.idsMu.Unlock()
			if err != nil {
				event.Err = err
			} else {
				event.Transition = identity.TransitionID(id)
			}
		}
	case PublishControl:
		event.Kind = PublishCompleted
		event.Record, event.Err = s.store.Replace(s.ctx, effect.Key, effect.Expected, effect.Write)
	case OpenEngine:
		event.Kind = OpenCompleted
		opened, event.Err = s.engine.Open(s.ctx, effect.Open)
		event.HasWriter = opened != nil
	case CloseEngine:
		event.Kind = CloseCompleted
		s.mu.Lock()
		w := s.writers[effect.Handle]
		s.mu.Unlock()
		if w == nil {
			event.Err = ErrInvalid
		} else {
			// A drain retains completion ownership even after the work context expires.
			// Caller Drain uses its own explicit budget while this call remains pending.
			event.Err = w.Close(context.WithoutCancel(s.ctx))
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if opened != nil {
		s.writers[effect.ID] = opened
	}
	if effect.Kind == CloseEngine && (event.Err == nil || errors.Is(event.Err, ErrFenced)) {
		delete(s.writers, effect.Handle)
	}
	event.Record = event.Record.Clone()
	s.applyLocked(event)
}
func (s *Service) Drain(ctx context.Context) error {
	for {
		s.mu.Lock()
		done := s.state.stopping && len(s.state.pending) == 0 && len(s.writers) == 0
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
