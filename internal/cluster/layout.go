package cluster

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
)

const ControlFormat = 2

var ErrLayoutMismatch = fmt.Errorf("%w: persisted layout is missing, unsupported, or differs from configured layout", ErrInvalidControl)

// PhysicalPartition binds one existing logical transaction domain to its stable
// physical identity and database path. Neither path nor ID is derived from name.
type PhysicalPartition struct {
	LogicalName string               `json:"logical_name"`
	ID          identity.PartitionID `json:"id"`
	Path        string               `json:"path"`
}

// Layout is immutable after bootstrap/offline migration. Slice order is the
// placement slot order; sorting it on restart would be an online reshard.
type Layout struct {
	Version    uint32              `json:"version"`
	Placement  PlacementConfig     `json:"placement"`
	Partitions []PhysicalPartition `json:"partitions"`
}

func (l Layout) clone() Layout { l.Partitions = slices.Clone(l.Partitions); return l }
func (l Layout) Validate() error {
	if l.Version != 1 || l.Placement.Validate() != nil || len(l.Partitions) == 0 {
		return ErrLayoutMismatch
	}
	names := map[string]bool{}
	ids := map[identity.PartitionID]bool{}
	paths := map[string]bool{}
	for _, p := range l.Partitions {
		if p.ID.Validate() != nil || registry.ValidateKey(registry.Key(p.LogicalName)) != nil || len(p.LogicalName) > 128 || registry.ValidateKey(registry.Key(p.Path)) != nil || names[p.LogicalName] || ids[p.ID] || paths[p.Path] {
			return ErrLayoutMismatch
		}
		names[p.LogicalName], ids[p.ID], paths[p.Path] = true, true, true
	}
	for path := range paths {
		for prefix := path; strings.Contains(prefix, "/"); {
			prefix = prefix[:strings.LastIndexByte(prefix, '/')]
			if paths[prefix] {
				return ErrLayoutMismatch
			}
		}
	}
	return nil
}

// Digest pins the complete explicit configuration, including control/layout
// versions, planner settings, order, logical names, IDs and physical paths. Hosts
// configure this pin before loading control; never adopt a loaded layout's digest
// as a restart default. The all-zero digest is invalid configuration.
func (l Layout) Digest() ([32]byte, error) {
	if err := l.Validate(); err != nil {
		return [32]byte{}, err
	}
	body, err := json.Marshal(struct {
		ControlFormat uint32 `json:"control_format"`
		Layout        Layout `json:"layout"`
	}{ControlFormat, l})
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(body), nil
}
func (l Layout) slots() []identity.PartitionID {
	out := make([]identity.PartitionID, len(l.Partitions))
	for i, p := range l.Partitions {
		out[i] = p.ID
	}
	return out
}
func (l Layout) Resolve(logicalName string) (PhysicalPartition, bool) {
	for _, p := range l.Partitions {
		if p.LogicalName == logicalName {
			return p, true
		}
	}
	return PhysicalPartition{}, false
}

// ValidateLayout is the activation/reconciliation gate. DecodeControl accepts
// format1 for inspection, but no current driver may act on it. A mismatch is not
// authorization to rewrite the persisted layout or repair legacy state.
func (s Snapshot) ValidateLayout(expected [32]byte) error {
	if expected == ([32]byte{}) || s.control.Format != ControlFormat || s.control.Layout == nil {
		return ErrLayoutMismatch
	}
	actual, err := s.control.Layout.Digest()
	if err != nil || actual != expected {
		return ErrLayoutMismatch
	}
	return nil
}
