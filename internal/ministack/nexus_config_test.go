package ministack

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNexusHTTPConfiguration(t *testing.T) {
	for _, name := range []string{"a", "b"} {
		path := "../../deploy/ministack/temporal-" + name + ".json"
		if err := CheckNexusConfig(path); err != nil {
			t.Fatal(name, err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var original map[string]any
		if err = json.Unmarshal(raw, &original); err != nil {
			t.Fatal(err)
		}
		for _, field := range []string{"listener", "cluster_address", "public_address"} {
			var m map[string]any
			_ = json.Unmarshal(raw, &m)
			switch field {
			case "listener":
				delete(m["services"].(map[string]any)["frontend"].(map[string]any)["rpc"].(map[string]any), "httpPort")
			case "cluster_address":
				delete(m["clusterMetadata"].(map[string]any)["clusterInformation"].(map[string]any)["active"].(map[string]any), "httpAddress")
			case "public_address":
				delete(m, "publicClient")
			}
			data, _ := json.Marshal(m)
			p := filepath.Join(t.TempDir(), "config.json")
			if err = os.WriteFile(p, data, 0600); err != nil {
				t.Fatal(err)
			}
			if CheckNexusConfig(p) == nil {
				t.Fatal("missing required HTTP field passed", name, field)
			}
		}
	}
	raw, err := os.ReadFile("../../deploy/ministack/haproxy.cfg")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"frontend temporal_http_ingress", "bind :17243", "default_backend temporal_http_nodes", "server temporal_http_a host.docker.internal:18243 check", "server temporal_http_b host.docker.internal:19243 check"} {
		if !strings.Contains(string(raw), required) {
			t.Fatal("missing ingress route", required)
		}
	}
	raw, err = os.ReadFile("../../deploy/ministack/compose.json")
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct{ Ports []string }
	}
	if err = json.Unmarshal(raw, &compose); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, p := range compose.Services["ingress"].Ports {
		found = found || p == "127.0.0.1:17243:17243"
	}
	if !found {
		t.Fatal("HTTP ingress not published")
	}
}
