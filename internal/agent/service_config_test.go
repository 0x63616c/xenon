package agent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/0x63616c/xenon/internal/cluster"
)

func serviceConfig() Config {
	c := validConfig()
	c.Bootstrap = true
	c.ServiceStorage = &ServiceStorageConfig{Format: 2, ClusterID: "clu_0000000000000000000001", NodeID: "nod_0000000000000000000001", Layout: cluster.Layout{Version: 1, Placement: cluster.DefaultPlacementConfig(), Partitions: []cluster.PhysicalPartition{{LogicalName: "global", ID: "prt_0000000000000000000001", Path: c.Prefix + "/data/global"}}}, FreshNamespace: true, PollInterval: "100ms", HeartbeatInterval: "1s", DiscoveryInterval: "100ms", RegistryTimeout: "20s", RenewalInterval: "1s", SuspectAfter: "5s", MembershipFailureAfter: "5s", MaxControlBytes: 1 << 20, MaxMembershipBytes: 1 << 20, MaxMembershipEntries: 10, MembershipReadBatch: 2, MaxOutcomes: 1000}
	return c
}
func TestServiceConfigExplicitAndBounded(t *testing.T) {
	for name, change := range map[string]func(*Config){
		"format":               func(c *Config) { c.ServiceStorage.Format = 1 },
		"identity":             func(c *Config) { c.ServiceStorage.NodeID = "legacy-name" },
		"layout":               func(c *Config) { c.ServiceStorage.Layout.Partitions = nil },
		"outside data":         func(c *Config) { c.ServiceStorage.Layout.Partitions[0].Path = "other/data/global" },
		"sibling prefix":       func(c *Config) { c.ServiceStorage.Layout.Partitions[0].Path = c.Prefix + "/data-other/global" },
		"implicit duration":    func(c *Config) { c.ServiceStorage.HeartbeatInterval = "" },
		"negative duration":    func(c *Config) { c.ServiceStorage.RegistryTimeout = "-1s" },
		"late renewal":         func(c *Config) { c.ServiceStorage.RenewalInterval = c.ServiceStorage.SuspectAfter },
		"unbounded entries":    func(c *Config) { c.ServiceStorage.MaxMembershipEntries = 0 },
		"scan overflow":        func(c *Config) { c.ServiceStorage.MembershipReadBatch = 11 },
		"record overflow":      func(c *Config) { c.ServiceStorage.MaxControlBytes = 1 << 21 },
		"legacy manifest name": func(c *Config) { c.Cluster = " bad " },
	} {
		t.Run(name, func(t *testing.T) {
			c := serviceConfig()
			change(&c)
			if c.Validate() == nil {
				t.Fatal("accepted invalid service config")
			}
		})
	}
	c := serviceConfig()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	copied := c.ServiceStorage.Clone()
	copied.Layout.Partitions[0].Path = "changed"
	if c.ServiceStorage.Layout.Partitions[0].Path == "changed" {
		t.Fatal("config slice aliased")
	}
	raw, _ := json.Marshal(c)
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil || loaded.ServiceStorage == nil {
		t.Fatal(loaded, err)
	}
}

func TestExplicitNullServiceConfigCannotSelectLegacy(t *testing.T) {
	raw, _ := json.Marshal(validConfig())
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	fields["service_storage"] = json.RawMessage("null")
	raw, _ = json.Marshal(fields)
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("explicit null silently selected legacy runtime")
	}
}
