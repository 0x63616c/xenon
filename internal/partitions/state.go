package partitions

import (
	"bytes"
	"maps"
	"slices"

	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
)

type EffectID uint64

type Phase uint8

const (
	Idle Phase = iota
	Reserving
	Opening
	Activating
	Ready
	Closing
	Stopped
)

type EventKind uint8

const (
	Poll EventKind = iota + 1
	Stop
	Fenced
	ReadCompleted
	PublishCompleted
	OpenCompleted
	CloseCompleted
)

type EffectKind uint8

const (
	ReadControl EffectKind = iota + 1
	PublishControl
	OpenEngine
	CloseEngine
)

type ControllerConfig struct {
	ExpectedLayoutDigest [32]byte
	Key                  registry.Key
	Partition            identity.PartitionID
	Incarnation          identity.IncarnationID
	MaxControlBytes      int
}

// Completion payloads are chosen by Kind. Native handles never enter pure state;
// Handle refers to the Open effect whose live writer belongs to the driver.
type Event struct {
	Kind        EventKind
	Incarnation identity.IncarnationID
	Effect      EffectID
	Handle      EffectID
	Record      registry.Record
	Transition  identity.TransitionID // recorded driver entropy for a new proposal
	HasWriter   bool
	Err         error
}
type Effect struct {
	ID          EffectID
	Kind        EffectKind
	Incarnation identity.IncarnationID
	Key         registry.Key
	Expected    registry.Version
	Write       registry.Write
	Open        OpenRequest
	Handle      EffectID
}

func (e Effect) clone() Effect { e.Write.Body = bytes.Clone(e.Write.Body); return e }

type publication struct {
	expected registry.Version
	write    registry.Write
	attempt  OpenRequest
	ready    bool
	unknown  bool
	retry    bool
}

// State is per physical partition and per process incarnation. Copies are pure
// values; Snapshot and Pending return owned views. LastUnknown preserves an
// unresolved historical publication even when current authority permits progress.
type State struct {
	config       ControllerConfig
	Phase        Phase
	LastError    error
	LastUnknown  *registry.UnknownOutcome
	next         EffectID
	pending      map[EffectID]Effect
	snapshot     cluster.Snapshot
	haveSnapshot bool
	publication  *publication
	attempt      OpenRequest
	handle       EffectID
	opening      EffectID
	stopping     bool
	obsolete     bool
}

func NewState(c ControllerConfig) (State, error) {
	if c.ExpectedLayoutDigest == ([32]byte{}) || registry.ValidateKey(c.Key) != nil || c.Partition.Validate() != nil || c.Incarnation.Validate() != nil || c.MaxControlBytes <= 0 {
		return State{}, ErrInvalid
	}
	return State{config: c, pending: map[EffectID]Effect{}}, nil
}
func (s State) Config() ControllerConfig { return s.config }
func (s State) Attempt() OpenRequest     { return s.attempt }
func (s State) Handle() EffectID         { return s.handle }
func (s State) Pending() []Effect {
	out := make([]Effect, 0, len(s.pending))
	for _, id := range slices.Sorted(maps.Keys(s.pending)) {
		out = append(out, s.pending[id].clone())
	}
	return out
}
func (s State) clone() State {
	s.pending = maps.Clone(s.pending)
	if s.LastUnknown != nil {
		unknown := *s.LastUnknown
		s.LastUnknown = &unknown
	}
	if s.publication != nil {
		p := *s.publication
		p.write.Body = bytes.Clone(p.write.Body)
		s.publication = &p
	}
	return s
}
