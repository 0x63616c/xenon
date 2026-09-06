//go:build integration_s3

package storage

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/0x63616c/xenon/internal/agent"
	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/ownership"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type bootstrapTransport struct {
	base         http.RoundTripper
	after        func()
	barrier      bool
	waiting      atomic.Int32
	release      chan struct{}
	manifestPuts atomic.Int32
	controlPuts  atomic.Int32
}

func (f *bootstrapTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == "PUT" && strings.HasSuffix(r.URL.Path, "/registry/cluster/control") {
		f.controlPuts.Add(1)
	}
	if r.Method == "PUT" && strings.HasSuffix(r.URL.Path, "/metadata/cluster.json") {
		f.manifestPuts.Add(1)
		if f.barrier {
			if f.waiting.Add(1) == 2 {
				close(f.release)
			}
			select {
			case <-f.release:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		}
		response, err := f.base.RoundTrip(r)
		if err == nil && response.StatusCode == 200 && f.after != nil {
			f.after()
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
			return nil, io.ErrUnexpectedEOF
		}
		return response, err
	}
	return f.base.RoundTrip(r)
}
func bootstrapClient(endpoint string, transport http.RoundTripper) *s3.Client {
	return s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("xenon-local", "xenon-local-test-only", ""), HTTPClient: &http.Client{Transport: transport}, RetryMaxAttempts: 1}, func(o *s3.Options) { o.BaseEndpoint = aws.String(endpoint); o.UsePathStyle = true })
}
func bootstrapConfig(bucket, prefix string) agent.Config {
	return agent.Config{Cluster: "cluster", Node: "display-node", Bucket: bucket, Prefix: prefix, BindIP: "127.0.0.1", AdvertiseIP: "127.0.0.1", BasePort: 17230, PublicAddress: "localhost:17233", PublicHTTPAddress: "localhost:18233", HistoryShards: 4, Bootstrap: true, ServiceStorage: &agent.ServiceStorageConfig{Format: 2, ClusterID: "clu_0000000000000000000001", NodeID: "nod_0000000000000000000001", Layout: cluster.Layout{Version: 1, Placement: cluster.DefaultPlacementConfig(), Partitions: []cluster.PhysicalPartition{{LogicalName: "global", ID: "prt_0000000000000000000001", Path: prefix + "/data/global"}, {LogicalName: "history-0", ID: "prt_0000000000000000000002", Path: prefix + "/data/history-0"}}}, FreshNamespace: true, PollInterval: "100ms", HeartbeatInterval: "1s", DiscoveryInterval: "100ms", RegistryTimeout: "10s", RenewalInterval: "1s", SuspectAfter: "5s", MembershipFailureAfter: "5s", MaxControlBytes: 1 << 20, MaxMembershipBytes: 1 << 20, MaxMembershipEntries: 10, MembershipReadBatch: 2, MaxOutcomes: 1000}}
}
func TestS3FreshBootstrap(t *testing.T) {
	var fixture struct {
		Schema           int
		Endpoint, Bucket string
		Schedule         []string
	}
	raw, err := os.ReadFile("../../test/scenarios/bootstrap/s3.json")
	if err != nil || json.Unmarshal(raw, &fixture) != nil || fixture.Schema != 1 {
		t.Fatal("invalid fixture", err)
	}
	wantSchedule := []string{"fresh_and_restart", "old_marker", "occupied_data", "occupied_topology", "lost_manifest_response", "interrupted_initialization", "deleted_control", "metadata_mismatch", "old_new_concurrent", "new_new_concurrent"}
	if !reflect.DeepEqual(fixture.Schedule, wantSchedule) {
		t.Fatal("unexpected bootstrap schedule")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client := bootstrapClient(fixture.Endpoint, http.DefaultTransport)
	if _, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(fixture.Bucket)}); err != nil {
		t.Fatal(err)
	}
	run := func(name string, fn func(*testing.T, agent.Config)) {
		t.Run(name, func(t *testing.T) { fn(t, bootstrapConfig(fixture.Bucket, "fresh/"+name)) })
	}
	run("fresh_and_restart", func(t *testing.T, c agent.Config) {
		p, err := PrepareServiceStorage(ctx, c, client, identity.Generator{})
		if err != nil {
			t.Fatal(err)
		}
		initial := p.Control()
		if initial.Coordinator.Generation != 1 || initial.Coordinator.Incarnation != p.Owner().Incarnation || initial.AssignmentRevision != 1 {
			t.Fatal(initial)
		}
		for id, part := range initial.Partitions {
			if part.Desired != p.Owner() || part.Generation != 0 || part.Ready || part.Reservation != "" || part.AssignmentRevision != 1 {
				t.Fatal(id, part)
			}
		}
		if p.ClusterConfig().FreshNamespace || p.ClusterConfig().Bootstrap != nil || len(p.PartitionConfigs()) != 2 {
			t.Fatal("reusable bootstrap authority escaped")
		}
		c.Bootstrap = false
		c.ServiceStorage.FreshNamespace = false
		restart, err := PrepareServiceStorage(ctx, c, client, identity.Generator{})
		if err != nil {
			t.Fatal(err)
		}
		if restart.Owner().Incarnation == p.Owner().Incarnation || restart.Control().Coordinator != initial.Coordinator {
			t.Fatal("restart rewrote initial authority")
		}
		copied := p.Config()
		copied.ServiceStorage.Layout.Partitions[0].Path = "mutated"
		if p.Config().ServiceStorage.Layout.Partitions[0].Path == "mutated" {
			t.Fatal("prepared config aliases")
		}
		old, _ := ownership.NewTopologyStore(client, c.Bucket, c.Prefix+"/metadata")
		if err = ownership.EnsureCluster(ctx, old, ownership.ClusterManifest{Format: 1, Cluster: c.Cluster, HistoryShards: c.HistoryShards, LayoutVersion: 1, WireVersion: 1}, true); err == nil {
			t.Fatal("legacy runtime accepted new marker")
		}
	})
	run("old_marker", func(t *testing.T, c agent.Config) {
		old, _ := ownership.NewTopologyStore(client, c.Bucket, c.Prefix+"/metadata")
		if err := ownership.EnsureCluster(ctx, old, ownership.ClusterManifest{Format: 1, Cluster: c.Cluster, HistoryShards: c.HistoryShards, LayoutVersion: 1, WireVersion: 1}, true); err != nil {
			t.Fatal(err)
		}
		if _, err := PrepareServiceStorage(ctx, c, client, identity.Generator{}); !errors.Is(err, ErrLegacyPrefix) {
			t.Fatal(err)
		}
	})
	for _, kind := range []string{"data", "topology"} {
		run("occupied_"+kind, func(t *testing.T, c agent.Config) {
			key := c.Prefix + "/data/global/existing"
			if kind == "topology" {
				key = c.Prefix + "/metadata/topology.json"
			}
			if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(key), Body: strings.NewReader("existing")}); err != nil {
				t.Fatal(err)
			}
			if _, err := PrepareServiceStorage(ctx, c, client, identity.Generator{}); !errors.Is(err, ErrLegacyPrefix) {
				t.Fatal(err)
			}
			if _, err := readServiceManifest(ctx, client, c.Bucket, c.Prefix+"/metadata/cluster.json"); !missingObject(err) {
				t.Fatal("created marker in occupied namespace", err)
			}
		})
	}
	run("lost_manifest_response", func(t *testing.T, c agent.Config) {
		fault := &bootstrapTransport{base: http.DefaultTransport, after: func() {}}
		p, err := PrepareServiceStorage(ctx, c, bootstrapClient(fixture.Endpoint, fault), identity.Generator{})
		if err != nil || p == nil {
			t.Fatal(err)
		}
		if fault.manifestPuts.Load() != 1 {
			t.Fatal("hidden manifest retries")
		}
	})
	run("interrupted_initialization", func(t *testing.T, c agent.Config) {
		cut, stop := context.WithCancel(ctx)
		defer stop()
		fault := &bootstrapTransport{base: http.DefaultTransport, after: stop}
		if _, err := PrepareServiceStorage(cut, c, bootstrapClient(fixture.Endpoint, fault), identity.Generator{}); err == nil {
			t.Fatal("canceled claim succeeded")
		}
		if _, err := PrepareServiceStorage(ctx, c, client, identity.Generator{}); !errors.Is(err, ErrIncompleteInitialization) {
			t.Fatal("restart repaired missing control", err)
		}
	})
	run("deleted_control", func(t *testing.T, c agent.Config) {
		if _, err := PrepareServiceStorage(ctx, c, client, identity.Generator{}); err != nil {
			t.Fatal(err)
		}
		// Deliberate corruption of this test's isolated authority, not supported cleanup.
		if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(c.Prefix + "/metadata/registry/cluster/control")}); err != nil {
			t.Fatal(err)
		}
		if _, err := PrepareServiceStorage(ctx, c, client, identity.Generator{}); !errors.Is(err, ErrIncompleteInitialization) {
			t.Fatal("persisted bootstrap flags recreated authority", err)
		}
	})
	run("metadata_mismatch", func(t *testing.T, c agent.Config) {
		if _, err := PrepareServiceStorage(ctx, c, client, identity.Generator{}); err != nil {
			t.Fatal(err)
		}
		c.HistoryShards++
		if _, err := PrepareServiceStorage(ctx, c, client, identity.Generator{}); !errors.Is(err, ErrBootstrap) {
			t.Fatal("history count mismatch accepted", err)
		}
	})
	run("old_new_concurrent", func(t *testing.T, c agent.Config) {
		fault := &bootstrapTransport{base: http.DefaultTransport, barrier: true, release: make(chan struct{})}
		racing := bootstrapClient(fixture.Endpoint, fault)
		old, _ := ownership.NewTopologyStore(racing, c.Bucket, c.Prefix+"/metadata")
		oldResult := make(chan error, 1)
		newResult := make(chan error, 1)
		go func() {
			oldResult <- ownership.EnsureCluster(ctx, old, ownership.ClusterManifest{Format: 1, Cluster: c.Cluster, HistoryShards: c.HistoryShards, LayoutVersion: 1, WireVersion: 1}, true)
		}()
		go func() { _, err := PrepareServiceStorage(ctx, c, racing, identity.Generator{}); newResult <- err }()
		oldErr, newErr := <-oldResult, <-newResult
		if (oldErr == nil) == (newErr == nil) {
			t.Fatal("exactly one manifest protocol must win", oldErr, newErr)
		}
		if fault.manifestPuts.Load() != 2 {
			t.Fatal("did not race both conditional claims")
		}
	})
	run("new_new_concurrent", func(t *testing.T, c agent.Config) {
		fault := &bootstrapTransport{base: http.DefaultTransport, barrier: true, release: make(chan struct{})}
		racing := bootstrapClient(fixture.Endpoint, fault)
		type result struct {
			p   *Prepared
			err error
		}
		results := make(chan result, 2)
		for range 2 {
			go func() {
				p, err := PrepareServiceStorage(ctx, c, racing, identity.Generator{})
				results <- result{p, err}
			}()
		}
		a, b := <-results, <-results
		if a.err != nil && b.err != nil {
			t.Fatal("neither new claimant initialized", a.err, b.err)
		}
		if fault.manifestPuts.Load() != 2 || fault.controlPuts.Load() != 1 {
			t.Fatal("bootstrap right transferred between matching claimants", fault.manifestPuts.Load(), fault.controlPuts.Load())
		}
		if a.err == nil && b.err == nil && a.p.Control().Coordinator != b.p.Control().Coordinator {
			t.Fatal("two initial coordinator authorities")
		}
	})

}
