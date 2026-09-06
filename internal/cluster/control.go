package cluster

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"strings"

	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
)

var (
	ErrInvalidControl = errors.New("invalid cluster control")
	ErrStaleControl   = errors.New("stale cluster control authority")
	ErrControlLimit   = errors.New("cluster control capacity exceeded")
)

type Owner struct {
	Node        identity.NodeID        `json:"node"`
	Incarnation identity.IncarnationID `json:"incarnation"`
	Address     string                 `json:"address"`
}

func (o Owner) valid() bool {
	return o.Node.Validate() == nil && o.Incarnation.Validate() == nil && o.Address != ""
}

type Coordinator struct {
	Incarnation identity.IncarnationID `json:"incarnation"`
	Generation  uint64                 `json:"generation"`
	Renewal     uint64                 `json:"renewal"`
}

// PartitionControl names one indivisible physical database. AssignmentRevision
// changes only when this partition's desired owner changes, preventing A-B-A
// activation while leaving unrelated healthy owners intact.
type PartitionControl struct {
	Path               string                `json:"path"`
	Desired            Owner                 `json:"desired"`
	AssignmentRevision uint64                `json:"assignment_revision"`
	Generation         uint64                `json:"generation"`
	Reservation        identity.TransitionID `json:"reservation,omitempty"`
	Ready              bool                  `json:"ready"`
}

// Control is the complete bounded publication unit. Heartbeats are advisory
// separate records. No transient addresses belong in application data or tokens.
type Control struct {
	Format             uint32                                    `json:"format"`
	Cluster            identity.ClusterID                        `json:"cluster"`
	Coordinator        Coordinator                               `json:"coordinator"`
	AssignmentRevision uint64                                    `json:"assignment_revision"`
	ActiveMove         identity.PartitionID                      `json:"active_move,omitempty"`
	Partitions         map[identity.PartitionID]PartitionControl `json:"partitions"`
}

func (c Control) clone() Control { c.Partitions = maps.Clone(c.Partitions); return c }

func (c Control) validate() error {
	if c.Format != 1 || c.Cluster.Validate() != nil || c.Coordinator.Incarnation.Validate() != nil || c.Coordinator.Generation == 0 || c.AssignmentRevision == 0 || len(c.Partitions) == 0 {
		return ErrInvalidControl
	}
	paths := make(map[string]bool, len(c.Partitions))
	if c.ActiveMove != "" {
		p, exists := c.Partitions[c.ActiveMove]
		if !exists || p.Ready {
			return fmt.Errorf("%w: invalid active move", ErrInvalidControl)
		}
	}
	for _, id := range slices.Sorted(maps.Keys(c.Partitions)) {
		p := c.Partitions[id]
		if id.Validate() != nil || registry.ValidateKey(registry.Key(p.Path)) != nil || paths[p.Path] || !p.Desired.valid() || p.AssignmentRevision == 0 || p.AssignmentRevision > c.AssignmentRevision {
			return fmt.Errorf("%w: partition %s", ErrInvalidControl, id)
		}
		if (p.Reservation != "" && (p.Reservation.Validate() != nil || p.Generation == 0)) || (p.Ready && p.Reservation == "") {
			return fmt.Errorf("%w: reservation %s", ErrInvalidControl, id)
		}
		paths[p.Path] = true
	}
	for _, path := range slices.Sorted(maps.Keys(paths)) {
		for i := strings.LastIndexByte(path, '/'); i >= 0; i = strings.LastIndexByte(path, '/') {
			path = path[:i]
			if paths[path] {
				return fmt.Errorf("%w: overlapping database prefixes", ErrInvalidControl)
			}
		}
	}
	return nil
}

// Snapshot owns a validated immutable copy and the whole-record CAS condition.
// Proposals below do no I/O and never prove publication. The driver must retain
// the exact returned Write across ambiguous retries and validate current fields
// after recovery. Superseded reservations need no historical-success claim:
// they cannot authorize a new open or ready publication.
type Snapshot struct {
	key     registry.Key
	version registry.Version
	control Control
	limit   int
}

func (s Snapshot) Control() Control { return s.control.clone() }
func (s Snapshot) Authority() Authority {
	c := s.control.Coordinator
	return Authority{Version: s.version, Incarnation: c.Incarnation, Generation: c.Generation, Renewal: c.Renewal}
}

// DecodeControl refuses oversized/noncanonical/unknown-format input. It consumes
// the registry envelope, not bare application JSON; the limit includes envelope
// bytes. A format-1 control key must be provisioned by the migration/bootstrap
// gate, never discovered by falling back from a legacy directory read failure.
func DecodeControl(key registry.Key, r registry.Record, maxBytes int) (Snapshot, error) {
	if maxBytes <= 0 || len(r.Body) > maxBytes {
		return Snapshot{}, ErrControlLimit
	}
	envelope, err := registry.Decode(key, r)
	if err != nil {
		return Snapshot{}, err
	}
	var c Control
	d := json.NewDecoder(bytes.NewReader(envelope.Body))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return Snapshot{}, fmt.Errorf("%w: %v", ErrInvalidControl, err)
	}
	if err := d.Decode(new(json.RawMessage)); err != io.EOF {
		return Snapshot{}, ErrInvalidControl
	}
	if err := c.validate(); err != nil {
		return Snapshot{}, err
	}
	canonical, err := json.Marshal(c)
	if err != nil {
		return Snapshot{}, err
	}
	if !bytes.Equal(canonical, envelope.Body) {
		return Snapshot{}, fmt.Errorf("%w: noncanonical payload", ErrInvalidControl)
	}
	return Snapshot{key: key, version: r.Version, control: c, limit: maxBytes}, nil
}

func controlWrite(key registry.Key, expected registry.Version, id identity.TransitionID, c Control, maxBytes int) (registry.Write, error) {
	if err := c.validate(); err != nil {
		return registry.Write{}, err
	}
	body, err := json.Marshal(c)
	if err != nil {
		return registry.Write{}, err
	}
	w, err := registry.NewWrite(key, expected, id, body)
	if err != nil {
		return registry.Write{}, err
	}
	encoded, err := registry.Encode(key, expected, w)
	if err != nil {
		return registry.Write{}, err
	}
	if maxBytes <= 0 || len(encoded) > maxBytes {
		return registry.Write{}, ErrControlLimit
	}
	return w, nil
}

// BootstrapWrite only prepares absent-key creation. The app must enforce fresh
// namespace or completed offline cutover before dispatch; absence is not proof
// that no legacy participant has write access to these data paths.
func BootstrapWrite(key registry.Key, id identity.TransitionID, c Control, maxBytes int) (registry.Write, error) {
	return controlWrite(key, "", id, c, maxBytes)
}

func (s Snapshot) ChangeCoordinator(id identity.TransitionID, change CoordinatorChange) (registry.Write, error) {
	a := s.Authority()
	if change.Expected != s.version || change.Incarnation.Validate() != nil {
		return registry.Write{}, ErrStaleControl
	}
	renew := change.Incarnation == a.Incarnation && change.Generation == a.Generation && a.Renewal != math.MaxUint64 && change.Renewal == a.Renewal+1
	takeover := change.Incarnation != a.Incarnation && a.Generation != math.MaxUint64 && change.Generation == a.Generation+1 && change.Renewal == 0
	if !renew && !takeover {
		return registry.Write{}, ErrStaleControl
	}
	c := s.control.clone()
	c.Coordinator = Coordinator{change.Incarnation, change.Generation, change.Renewal}
	return controlWrite(s.key, s.version, id, c, s.limit)
}

// Assign changes only the specified desired owners. The database set and paths
// are immutable here; resizing populated storage needs a separate migration.
// Unchanged owners retain their reservation and readiness across plan updates.
func (s Snapshot) Assign(self identity.IncarnationID, id identity.TransitionID, desired map[identity.PartitionID]Owner) (registry.Write, error) {
	if self != s.control.Coordinator.Incarnation {
		return registry.Write{}, ErrStaleControl
	}
	c := s.control.clone()
	changed := false
	for _, partition := range slices.Sorted(maps.Keys(desired)) {
		owner := desired[partition]
		p, exists := c.Partitions[partition]
		if !exists || !owner.valid() {
			return registry.Write{}, ErrInvalidControl
		}
		if p.Desired == owner {
			continue
		}
		if changed || (c.ActiveMove != "" && c.ActiveMove != partition) {
			return registry.Write{}, fmt.Errorf("%w: another database move is pending", ErrControlLimit)
		}
		if c.AssignmentRevision == math.MaxUint64 {
			return registry.Write{}, ErrControlLimit
		}
		p.Desired, p.AssignmentRevision = owner, c.AssignmentRevision+1
		p.Reservation, p.Ready = "", false
		c.Partitions[partition] = p
		changed = true
		c.ActiveMove = partition
	}
	if !changed {
		return registry.Write{}, fmt.Errorf("%w: empty assignment change", ErrInvalidControl)
	}
	c.AssignmentRevision++
	return controlWrite(s.key, s.version, id, c, s.limit)
}

// Reserve returns a proposal for one new native-open attempt. It never opens an
// engine. Confirm current reservation publication before starting that attempt,
// and start it only once even when native completion or caller delivery is late.
func (s Snapshot) Reserve(self identity.IncarnationID, partition identity.PartitionID, assignment uint64, id identity.TransitionID) (registry.Write, error) {
	p, ok := s.control.Partitions[partition]
	if !ok || p.Desired.Incarnation != self || p.AssignmentRevision != assignment {
		return registry.Write{}, ErrStaleControl
	}
	if p.Generation == math.MaxUint64 {
		return registry.Write{}, ErrControlLimit
	}
	if id == p.Reservation {
		return registry.Write{}, fmt.Errorf("%w: reused reservation", ErrInvalidControl)
	}
	p.Generation++
	p.Reservation, p.Ready = id, false
	c := s.control.clone()
	c.Partitions[partition] = p
	return controlWrite(s.key, s.version, id, c, s.limit)
}

// MarkReady requires the exact current reservation, assignment and generation.
// The driver must additionally have completed native recovery/fencing and retain
// the associated live handle. This control record cannot fence native writes.
func (s Snapshot) MarkReady(self identity.IncarnationID, partition identity.PartitionID, assignment, generation uint64, reservation, transition identity.TransitionID) (registry.Write, error) {
	p, ok := s.control.Partitions[partition]
	if !ok || p.Desired.Incarnation != self || p.AssignmentRevision != assignment || p.Generation != generation || p.Reservation != reservation || reservation == "" {
		return registry.Write{}, ErrStaleControl
	}
	p.Ready = true
	c := s.control.clone()
	c.Partitions[partition] = p
	if c.ActiveMove == partition {
		c.ActiveMove = ""
	}
	return controlWrite(s.key, s.version, transition, c, s.limit)
}
