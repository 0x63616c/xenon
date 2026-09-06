package agent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validConfig() Config {
	return Config{Cluster: "cluster", Node: "node", Bucket: "bucket", Prefix: "xenon/test", BindIP: "0.0.0.0", AdvertiseIP: "127.0.0.1", BasePort: 17230, PublicAddress: "localhost:17233", PublicHTTPAddress: "localhost:18233", HistoryShards: 4}
}
func TestConfigRejectsInvalidInput(t *testing.T) {
	for name, change := range map[string]func(*Config){"prefix": func(c *Config) { c.Prefix = "a/../b" }, "advertise": func(c *Config) { c.AdvertiseIP = "0.0.0.0" }, "ports": func(c *Config) { c.BasePort = 65530 }, "public port": func(c *Config) { c.PublicAddress = "localhost:abc" }, "zero port": func(c *Config) { c.PublicAddress = "localhost:0" }, "shards": func(c *Config) { c.HistoryShards = 0 }} {
		t.Run(name, func(t *testing.T) {
			c := validConfig()
			change(&c)
			if c.Validate() == nil {
				t.Fatal("accepted invalid config")
			}
		})
	}
	c := validConfig()
	c.BindIP = "::"
	c.AdvertiseIP = "::1"
	if c.Validate() != nil || c.Address(3) != "[::1]:17233" || c.Listen(3) != "[::]:17233" {
		t.Fatal(c)
	}
}
func TestLoadConfigStrictAndBounded(t *testing.T) {
	data, _ := json.Marshal(validConfig())
	for name, input := range map[string]string{"valid": string(data), "trailing": string(data) + " {}", "unknown": strings.Replace(string(data), "\"node\"", "\"typo\"", 1), "oversized whitespace": string(data) + strings.Repeat(" ", 65537), "truncated": "{"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, []byte(input), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(path)
			if (err == nil) != (name == "valid") {
				t.Fatal(err)
			}
		})
	}
}
