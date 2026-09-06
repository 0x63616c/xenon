package cluster

import (
	"bytes"
	"errors"
	"time"

	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
)

var ErrInvalidController = errors.New("invalid cluster controller input")

type EffectID uint64
type EventKind uint8

const (
	Poll EventKind = iota + 1
	Stop
	ReadCompleted
	PublishCompleted
)

type EffectKind uint8

const (
	ReadControl EffectKind = iota + 1
	PublishControl
)

// ControllerConfig pins an explicitly configured immutable persisted layout.
// FreshNamespace is an operator assertion of isolated fresh storage, not something
// inferred from NotFound. Bootstrap is never used to repair a missing live key.
type ControllerConfig struct {
	Key                  registry.Key
	Incarnation          identity.IncarnationID
	MaxControlBytes      int
	RenewalInterval      time.Duration
	SuspectAfter         time.Duration
	ExpectedLayoutDigest [32]byte
	FreshNamespace       bool
	Bootstrap            *Control
}

func (c ControllerConfig) clone() ControllerConfig {
	if c.Bootstrap != nil {
		b := c.Bootstrap.clone()
		c.Bootstrap = &b
	}
	return c
}

// Poll replaces the complete advisory eligible membership snapshot. Suspicion
// and renewal use only At, in this incarnation's monotonic time domain. A caller
// must derive eligibility externally; membership does not confer write authority.
// Transition is recorded entropy supplied with ReadCompleted for a new proposal.
type Event struct {
	Kind        EventKind
	At          Tick
	Incarnation identity.IncarnationID
	Effect      EffectID
	Membership  MembershipView
	Transition  identity.TransitionID
	Record      registry.Record
	Err         error
}
type Effect struct {
	ID          EffectID
	Kind        EffectKind
	Incarnation identity.IncarnationID
	Key         registry.Key
	Expected    registry.Version // empty only for explicitly permitted bootstrap Create
	Write       registry.Write
}

func (e Effect) clone() Effect { e.Write.Body = bytes.Clone(e.Write.Body); return e }

// Publication preserves the exact attempt after ambiguous completion, including
// its original condition. An unresolved historical attempt never becomes a
// definite failure merely because a newer control record permits fresh progress.
type Publication struct {
	Effect  Effect
	Unknown *registry.UnknownOutcome
	renewal bool
}

func (p *Publication) clone() *Publication {
	if p == nil {
		return nil
	}
	out := *p
	out.Effect = p.Effect.clone()
	if p.Unknown != nil {
		u := *p.Unknown
		out.Unknown = &u
	}
	return &out
}

// State has at most one registry effect in flight. Poll while it is in flight
// updates time/membership but cannot issue another publication. State and its
// exported views own their slices and publication bytes.
type State struct {
	config       ControllerConfig
	next         EffectID
	pending      *Effect
	publication  *Publication
	election     Election
	membership   MembershipView
	at           Tick
	lastRenew    Tick
	renewed      bool
	moveCredit   bool
	seenControl  bool
	stopping     bool
	snapshot     Snapshot
	haveSnapshot bool
	LastError    error
	LastUnknown  *Publication
}

func NewState(c ControllerConfig) (State, error) {
	if registry.ValidateKey(c.Key) != nil || c.Incarnation.Validate() != nil || c.MaxControlBytes <= 0 || c.RenewalInterval <= 0 || c.SuspectAfter <= c.RenewalInterval || c.ExpectedLayoutDigest == ([32]byte{}) || (c.Bootstrap != nil) != c.FreshNamespace {
		return State{}, ErrInvalidController
	}
	if c.Bootstrap != nil {
		if c.Bootstrap.validate() != nil || c.Bootstrap.Coordinator.Incarnation != c.Incarnation || (Snapshot{control: *c.Bootstrap}).ValidateLayout(c.ExpectedLayoutDigest) != nil {
			return State{}, ErrInvalidController
		}
	}
	return State{config: c.clone()}, nil
}
func (s State) Config() ControllerConfig { return s.config.clone() }
func (s State) Pending() []Effect {
	if s.pending == nil {
		return nil
	}
	return []Effect{s.pending.clone()}
}
func (s State) Publication() *Publication { return s.publication.clone() }
func (s State) Stopped() bool             { return s.stopping && s.pending == nil }
func (s State) Control() (Control, bool)  { return s.snapshot.Control(), s.haveSnapshot }
func (s State) clone() State {
	s.config = s.config.clone()
	s.membership = s.membership.clone()
	if s.pending != nil {
		e := s.pending.clone()
		s.pending = &e
	}
	s.publication = s.publication.clone()
	s.LastUnknown = s.LastUnknown.clone()
	return s
}
