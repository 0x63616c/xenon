package temporalruntime

import (
	"github.com/0x63616c/xenon/internal/agent"
	"testing"
)

func TestGeneratedConfigurationKeepsTemporalBehindAgent(t *testing.T) {
	c := agent.Config{Cluster: "example", Node: "node-a", Bucket: "bucket", Prefix: "cluster", BindIP: "0.0.0.0", AdvertiseIP: "10.0.0.1", BasePort: 17233, PublicAddress: "example:7233", PublicHTTPAddress: "example:7242", HistoryShards: 16}
	cfg, err := Configuration(c)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Services) != 4 || cfg.ClusterMetadata.CurrentClusterName != "example" || cfg.PublicClient.HostPort != c.PublicAddress {
		t.Fatal("cluster configuration escaped Xenon settings")
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
		if s.CustomDataStoreConfig == nil || s.CustomDataStoreConfig.Options["address"] != c.Address(8) {
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
	cfg.Persistence.DataStores["xenon-default"].CustomDataStoreConfig.Options["address"] = "modified"
	if other.Persistence.DataStores["xenon-default"].CustomDataStoreConfig.Options["address"] == "modified" {
		t.Fatal("configuration shared between agents")
	}
}
