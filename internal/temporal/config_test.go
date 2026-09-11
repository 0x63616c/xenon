package temporal

import "testing"

func TestConfigRejectsInvalidOwnedSettings(t *testing.T) {
	for _, change := range []func(*Config){
		func(c *Config) { c.Cluster = "" },
		func(c *Config) { c.BindIP = "invalid" },
		func(c *Config) { c.AdvertiseIP = "0.0.0.0" },
		func(c *Config) { c.BasePort = 65526 },
		func(c *Config) { c.HistoryShards = 0 },
		func(c *Config) { c.PublicAddress = "host:0" },
		func(c *Config) { c.PublicHTTPAddress = "host:65536" },
		func(c *Config) { c.StorageAddress = "host:not-a-port" },
	} {
		c := Config{Cluster: "cluster", ClusterID: "clu_0000000000000000000001", BindIP: "0.0.0.0", AdvertiseIP: "127.0.0.1", BasePort: 17233, HistoryShards: 4, PublicAddress: "localhost:7233", PublicHTTPAddress: "localhost:7242", StorageAddress: "127.0.0.1:17241"}
		change(&c)
		if runtime, err := New(c); err == nil || runtime != nil {
			t.Fatalf("invalid owned settings accepted: %+v, %v", c, err)
		}
	}
}
