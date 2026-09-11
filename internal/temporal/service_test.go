package temporal

import (
	"github.com/0x63616c/xenon/internal/temporal/adapter"
	"github.com/google/uuid"
	"go.temporal.io/server/common/searchattribute"
	"testing"
)

func TestGeneratedConfigurationKeepsTemporalBehindAgent(t *testing.T) {
	c := Config{Cluster: "example", ClusterID: "clu_0000000000000000000001", StorageAddress: "10.0.0.1:17241", BindIP: "0.0.0.0", AdvertiseIP: "10.0.0.1", BasePort: 17233, PublicAddress: "example:7233", PublicHTTPAddress: "example:7242", HistoryShards: 16}
	cfg, err := Configuration(c)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Services) != 4 || cfg.ClusterMetadata.CurrentClusterName != "example" || cfg.PublicClient.HostPort != c.PublicAddress {
		t.Fatal("cluster configuration escaped Xenon settings")
	}
	id := cfg.ClusterMetadata.ClusterInformation["example"].ClusterID
	if uuid.Validate(id) != nil {
		t.Fatal("stable Temporal cluster UUID missing", id)
	}
	ports := map[int]bool{}
	for _, s := range cfg.Services {
		for _, p := range []int{s.RPC.GRPCPort, s.RPC.MembershipPort} {
			if ports[p] {
				t.Fatal("colliding service ports")
			}
			ports[p] = true
		}
	}
	for _, s := range cfg.Persistence.DataStores {
		if s.CustomDataStoreConfig == nil || s.CustomDataStoreConfig.Options["address"] != c.StorageAddress {
			t.Fatal("persistence bypasses local Xenon router")
		}
	}
	if cfg.Services["frontend"].RPC.HTTPPort != c.BasePort+9 {
		t.Fatal("Nexus HTTP missing")
	}
	other, err := Configuration(c)
	if err != nil {
		t.Fatal(err)
	}
	if other.ClusterMetadata.ClusterInformation["example"].ClusterID != id {
		t.Fatal("same Xenon cluster generated a new Temporal identity")
	}
	c.ClusterID = "clu_0000000000000000000002"
	different, err := Configuration(c)
	if err != nil || different.ClusterMetadata.ClusterInformation["example"].ClusterID == id {
		t.Fatal("different Xenon clusters shared Temporal identity", err)
	}
	cfg.Persistence.DataStores["xenon-default"].CustomDataStoreConfig.Options["address"] = "modified"
	if other.Persistence.DataStores["xenon-default"].CustomDataStoreConfig.Options["address"] == "modified" {
		t.Fatal("configuration shared between agents")
	}
}

// Temporal's startup metadata initializer uses DataStore.GetIndexName, while
// workflow validation uses the visibility store's index. They must be identical
// before the first process starts, without relying on a later probe seed.
func TestStartupSearchAttributeIndexMatchesVisibilityStore(t *testing.T) {
	c := Config{Cluster: "example", ClusterID: "clu_0000000000000000000001", StorageAddress: "127.0.0.1:17241", BindIP: "127.0.0.1", AdvertiseIP: "127.0.0.1", BasePort: 17233, PublicAddress: "127.0.0.1:17233", PublicHTTPAddress: "127.0.0.1:17242", HistoryShards: 4}
	cfg, err := Configuration(c)
	if err != nil {
		t.Fatal(err)
	}
	ds := cfg.Persistence.GetVisibilityStoreConfig()
	visibility, err := (adapter.VisibilityFactory{}).NewVisibilityStore(*ds.CustomDataStoreConfig, searchattribute.NewTestProvider(), nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer visibility.Close()
	if ds.GetIndexName() == "" || ds.GetIndexName() != visibility.GetIndexName() {
		t.Fatalf("startup seeds index %q, runtime reads %q", ds.GetIndexName(), visibility.GetIndexName())
	}
}
