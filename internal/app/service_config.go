package app

import (
	"fmt"
	"strings"
	"time"

	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/identity"
)

// ServiceStorageConfig explicitly selects the format2 service runtime. Its layout
// is an operator-provided immutable mapping, never learned from existing objects.
// Timing values are Go duration strings; all limits/cadences are explicit.
type ServiceStorageConfig struct {
	Format                 int                `json:"format"`
	ClusterID              identity.ClusterID `json:"cluster_id"`
	NodeID                 identity.NodeID    `json:"node_id"`
	Layout                 cluster.Layout     `json:"layout"`
	FreshNamespace         bool               `json:"fresh_namespace"`
	PollInterval           string             `json:"poll_interval"`
	HeartbeatInterval      string             `json:"heartbeat_interval"`
	DiscoveryInterval      string             `json:"discovery_interval"`
	RegistryTimeout        string             `json:"registry_timeout"`
	RenewalInterval        string             `json:"renewal_interval"`
	SuspectAfter           string             `json:"suspect_after"`
	MembershipFailureAfter string             `json:"membership_failure_after"`
	MaxControlBytes        int                `json:"max_control_bytes"`
	MaxMembershipBytes     int                `json:"max_membership_bytes"`
	MaxMembershipEntries   int                `json:"max_membership_entries"`
	MembershipReadBatch    int                `json:"membership_read_batch"`
	WALFlushIntervalMS     *int               `json:"wal_flush_interval_ms,omitempty"`
	MaxOutcomes            uint64             `json:"max_outcomes"`
}

// EffectiveWALFlushIntervalMS keeps omitted configuration compatible with SlateDB.
func (c ServiceStorageConfig) EffectiveWALFlushIntervalMS() int {
	if c.WALFlushIntervalMS == nil {
		return 100
	}
	return *c.WALFlushIntervalMS
}

func (c ServiceStorageConfig) Validate(prefix string) error {
	if value := c.EffectiveWALFlushIntervalMS(); value < 1 || value > 1000 {
		return fmt.Errorf("wal_flush_interval_ms must be an integer from 1 through 1000")
	}
	if c.Format != 2 || c.ClusterID.Validate() != nil || c.NodeID.Validate() != nil || c.Layout.Validate() != nil {
		return fmt.Errorf("invalid service storage identity or layout")
	}
	for _, p := range c.Layout.Partitions {
		if !strings.HasPrefix(p.Path, prefix+"/data/") {
			return fmt.Errorf("layout path %q is outside configured data namespace", p.Path)
		}
	}
	for _, value := range []string{c.PollInterval, c.HeartbeatInterval, c.DiscoveryInterval, c.RegistryTimeout, c.RenewalInterval, c.SuspectAfter, c.MembershipFailureAfter} {
		duration, err := time.ParseDuration(value)
		if err != nil || duration <= 0 {
			return fmt.Errorf("all service timing values must be explicit positive durations")
		}
	}
	renewal, _ := time.ParseDuration(c.RenewalInterval)
	suspect, _ := time.ParseDuration(c.SuspectAfter)
	heartbeat, _ := time.ParseDuration(c.HeartbeatInterval)
	failure, _ := time.ParseDuration(c.MembershipFailureAfter)
	if renewal >= suspect || heartbeat >= failure {
		return fmt.Errorf("renewal and heartbeat intervals must precede their suspicion intervals")
	}
	if c.MaxControlBytes <= 0 || c.MaxControlBytes > 1<<20 || c.MaxMembershipBytes <= 0 || c.MaxMembershipBytes > 1<<20 || c.MaxMembershipEntries <= 0 || c.MembershipReadBatch <= 0 || c.MembershipReadBatch > c.MaxMembershipEntries || c.MaxOutcomes == 0 {
		return fmt.Errorf("invalid explicit service limits")
	}
	return nil
}

// Clone owns the ordered layout slice across configuration/lifecycle boundaries.
func (c ServiceStorageConfig) Clone() ServiceStorageConfig {
	if c.WALFlushIntervalMS != nil {
		value := *c.WALFlushIntervalMS
		c.WALFlushIntervalMS = &value
	}
	c.Layout.Partitions = append([]cluster.PhysicalPartition(nil), c.Layout.Partitions...)
	return c
}
