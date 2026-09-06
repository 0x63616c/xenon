package app

import (
	"errors"
	"fmt"
	"testing"

	"github.com/0x63616c/xenon/internal/agent"
	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/storage"
)

func TestServiceLayoutRequiresExplicitTemporalDomains(t *testing.T) {
	if err := ValidateServiceLayout(agent.Config{}); !errors.Is(err, storage.ErrLegacyPrefix) {
		t.Fatal(err)
	}
	c := agent.Config{Cluster: "cluster", Node: "display", Bucket: "bucket", Prefix: "test", BindIP: "127.0.0.1", AdvertiseIP: "127.0.0.1", BasePort: 17230, PublicAddress: "localhost:17233", PublicHTTPAddress: "localhost:18233", HistoryShards: 4, ServiceStorage: &agent.ServiceStorageConfig{Format: 2, ClusterID: "clu_0000000000000000000001", NodeID: "nod_0000000000000000000001", Layout: cluster.Layout{Version: 1, Placement: cluster.DefaultPlacementConfig()}, PollInterval: "100ms", HeartbeatInterval: "1s", DiscoveryInterval: "100ms", RegistryTimeout: "10s", RenewalInterval: "1s", SuspectAfter: "5s", MembershipFailureAfter: "5s", MaxControlBytes: 1 << 20, MaxMembershipBytes: 1 << 20, MaxMembershipEntries: 10, MembershipReadBatch: 2, MaxOutcomes: 100}}
	names := []string{"global", "matching", "history-0", "history-1", "history-2", "history-3", "vis-v1-0", "vis-v1-1", "vis-v1-2", "vis-v1-3"}
	for i, name := range names {
		c.ServiceStorage.Layout.Partitions = append(c.ServiceStorage.Layout.Partitions, cluster.PhysicalPartition{LogicalName: name, ID: identity.PartitionID(fmt.Sprintf("prt_%022d", i+1)), Path: "test/data/" + name})
	}
	if err := ValidateServiceLayout(c); err != nil {
		t.Fatal(err)
	}
	for i, name := range names {
		t.Run(name, func(t *testing.T) {
			copy := *c.ServiceStorage
			copy.Layout.Partitions = append([]cluster.PhysicalPartition(nil), c.ServiceStorage.Layout.Partitions...)
			copy.Layout.Partitions[i].LogicalName = "unrelated"
			missing := c
			missing.ServiceStorage = &copy
			if ValidateServiceLayout(missing) == nil {
				t.Fatal("missing domain accepted")
			}
		})
	}
}
