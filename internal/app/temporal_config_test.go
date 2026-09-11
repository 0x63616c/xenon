package app

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/0x63616c/xenon/internal/temporal"
)

// Digests were captured from Configuration(agent.Config) at 810c489 before
// moving the boundary. JSON marshaling covers the complete upstream config.
func TestTemporalConfigurationPreservesBaselineBytes(t *testing.T) {
	cases := []struct {
		config Config
		digest string
	}{
		{Config{Cluster: "example", Node: "a", Bucket: "bucket", Prefix: "cluster", BindIP: "0.0.0.0", AdvertiseIP: "10.0.0.1", BasePort: 17233, PublicAddress: "example:7233", PublicHTTPAddress: "example:7242", HistoryShards: 16}, "d4b78cde14779fa6ef5f373320cfc6759a063b773409d11e9f1b849f664f038b"},
		{Config{Cluster: "ipv6", Node: "b", Bucket: "bucket", Prefix: "nested/cluster", BindIP: "::", AdvertiseIP: "2001:db8::1", BasePort: 22000, PublicAddress: "[2001:db8::2]:7233", PublicHTTPAddress: "[2001:db8::2]:7242", HistoryShards: 4}, "ca2a1f90d026e8cbb0dc45031ae1ec71088f8473e9fe5aa680589c07c0bdcefc"},
		{Config{Cluster: "edge", Node: "c", Bucket: "bucket", Prefix: "cluster", BindIP: "127.0.0.1", AdvertiseIP: "127.0.0.1", BasePort: 65525, PublicAddress: "localhost:65535", PublicHTTPAddress: "localhost:1", HistoryShards: 16384}, "14ffe643c8f505cbc6dc0300f0ca8035e095253c4ad1e3683b5679c8e291d9a9"},
	}
	for _, tc := range cases {
		t.Run(tc.config.Cluster, func(t *testing.T) {
			projection, err := tc.config.TemporalConfig()
			if err != nil {
				t.Fatal(err)
			}
			generated, err := temporal.Configuration(projection)
			if err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(generated)
			if err != nil {
				t.Fatal(err)
			}
			if got := fmt.Sprintf("%x", sha256.Sum256(data)); got != tc.digest {
				t.Fatalf("upstream configuration changed: got %s, want %s", got, tc.digest)
			}
		})
	}
}

func TestTemporalProjectionRejectsInvalidApplicationConfiguration(t *testing.T) {
	for _, change := range []func(*Config){
		func(c *Config) { c.Bucket = "" },
		func(c *Config) { c.Prefix = "../wrong" },
		func(c *Config) { c.Node = "" },
		func(c *Config) { c.ServiceStorage = &ServiceStorageConfig{} },
	} {
		c := validConfig()
		change(&c)
		if got, err := c.TemporalConfig(); err == nil || got != (temporal.Config{}) {
			t.Fatalf("invalid app settings escaped conversion: %+v, %v", got, err)
		}
	}
}
