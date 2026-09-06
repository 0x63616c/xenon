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
		{Config{Cluster: "example", Node: "a", Bucket: "bucket", Prefix: "cluster", BindIP: "0.0.0.0", AdvertiseIP: "10.0.0.1", BasePort: 17233, PublicAddress: "example:7233", PublicHTTPAddress: "example:7242", HistoryShards: 16}, "392f98855f9fa59d414bb828bcc9f909d04c6767228294c75a57ac40b5cb40af"},
		{Config{Cluster: "ipv6", Node: "b", Bucket: "bucket", Prefix: "nested/cluster", BindIP: "::", AdvertiseIP: "2001:db8::1", BasePort: 22000, PublicAddress: "[2001:db8::2]:7233", PublicHTTPAddress: "[2001:db8::2]:7242", HistoryShards: 4}, "dd15273d9e4e0cffcc0cd97754cd9b1e4b57facf00addc2c6d84f57090b2c7c5"},
		{Config{Cluster: "edge", Node: "c", Bucket: "bucket", Prefix: "cluster", BindIP: "127.0.0.1", AdvertiseIP: "127.0.0.1", BasePort: 65525, PublicAddress: "localhost:65535", PublicHTTPAddress: "localhost:1", HistoryShards: 16384}, "37e171e0815fe4f99e22d1835e450223a49f7e7d38a827ba5f88125e2fb1ff19"},
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
