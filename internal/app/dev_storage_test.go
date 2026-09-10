package app

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
)

func TestDevPreservedAuthorityCannotProvisionMissingOrReplacedCluster(t *testing.T) {
	c := serviceConfig()
	digest, _ := c.ServiceStorage.Layout.Digest()
	initialization := "trn_0000000000000000000001"
	manifest, _ := json.Marshal(serviceManifest{Format: 2, Cluster: c.Cluster, HistoryShards: c.HistoryShards, LayoutVersion: 1, WireVersion: 1, ClusterID: c.ServiceStorage.ClusterID, LayoutDigest: digest, Initialization: identity.TransitionID(initialization)})
	owner := cluster.Owner{Node: c.ServiceStorage.NodeID, Incarnation: "inc_0000000000000000000001", Address: "127.0.0.1:1234"}
	control := cluster.Control{Format: cluster.ControlFormat, Cluster: c.ServiceStorage.ClusterID, Layout: &c.ServiceStorage.Layout, Coordinator: cluster.Coordinator{Incarnation: owner.Incarnation, Generation: 2}, AssignmentRevision: 3, Partitions: map[identity.PartitionID]cluster.PartitionControl{}}
	for _, p := range c.ServiceStorage.Layout.Partitions {
		control.Partitions[p.ID] = cluster.PartitionControl{Path: p.Path, Desired: owner, AssignmentRevision: 3, Generation: 4, Reservation: "trn_0000000000000000000002", Ready: true}
	}
	write, err := cluster.BootstrapWrite(serviceControlKey, "trn_0000000000000000000003", control, c.ServiceStorage.MaxControlBytes)
	if err != nil {
		t.Fatal(err)
	}
	body, err := registry.Encode(serviceControlKey, "", write)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"intact", "missing bucket", "missing manifest", "missing control", "replacement identity"} {
		t.Run(name, func(t *testing.T) {
			var mutations atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					mutations.Add(1)
					w.WriteHeader(500)
					return
				}
				isManifest := strings.HasSuffix(r.URL.Path, "cluster.json")
				if name == "missing bucket" || name == "missing manifest" && isManifest || name == "missing control" && !isManifest {
					w.WriteHeader(404)
					_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code></Error>`))
					return
				}
				w.Header().Set("ETag", `"version"`)
				if isManifest {
					_, _ = w.Write(manifest)
				} else {
					_, _ = w.Write(body)
				}
			}))
			defer server.Close()
			_, portText, _ := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
			port, _ := strconv.Atoi(portText)
			expected := initialization
			if name == "replacement identity" {
				expected = "trn_0000000000000000000002"
			}
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			err := devStorage(ctx, port, c, true, expected)
			if (err == nil) != (name == "intact") || mutations.Load() != 0 {
				t.Fatalf("err=%v mutations=%d", err, mutations.Load())
			}
		})
	}
}
