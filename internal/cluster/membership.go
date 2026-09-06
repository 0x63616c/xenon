package cluster

import (
	"github.com/0x63616c/xenon/internal/identity"
	"slices"
)

// MembershipView is advisory placement eligibility observed during one coordinator
// tenure. An absent or incomplete view cannot trigger placement or retarget an
// active move. It does not constrain election/renewal or confer writer authority.
type MembershipView struct {
	Coordinator identity.IncarnationID `json:"coordinator"`
	Generation  uint64                 `json:"generation"`
	Ready       bool                   `json:"ready"`
	Members     []Owner                `json:"members,omitempty"`
}

func (v MembershipView) clone() MembershipView { v.Members = slices.Clone(v.Members); return v }
func (v MembershipView) valid() bool {
	if !v.Ready {
		return len(v.Members) == 0
	}
	return v.Coordinator.Validate() == nil && v.Generation != 0 && validMembers(v.Members)
}
func (v MembershipView) matches(c Coordinator) bool {
	return v.Ready && v.Coordinator == c.Incarnation && v.Generation == c.Generation
}
