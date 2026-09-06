package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type inspectTransport func(*http.Request) (*http.Response, error)

func (f inspectTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestInspectReadsOnlyAuthoritativeObjects(t *testing.T) {
	c := serviceConfig()
	digest, err := c.ServiceStorage.Layout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := json.Marshal(serviceManifest{Format: 2, Cluster: c.Cluster, HistoryShards: c.HistoryShards, LayoutVersion: 1, WireVersion: 1, ClusterID: c.ServiceStorage.ClusterID, LayoutDigest: digest, Initialization: "trn_0000000000000000000001"})
	if err != nil {
		t.Fatal(err)
	}
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
	for _, tc := range []struct {
		name, failPath, response, status string
		code, reads                      int
	}{
		{name: "observed", status: "observed", reads: 2},
		{name: "absent manifest", failPath: "cluster.json", response: `<Error><Code>NoSuchKey</Code></Error>`, code: 404, status: "unknown", reads: 1},
		{name: "malformed manifest", failPath: "cluster.json", response: `{}`, code: 200, status: "unknown", reads: 1},
		{name: "absent control", failPath: "cluster/control", response: `<Error><Code>NoSuchKey</Code></Error>`, code: 404, status: "unknown", reads: 2},
		{name: "corrupt control", failPath: "cluster/control", response: `{}`, code: 200, status: "unknown", reads: 2},
		{name: "authority denied", failPath: "cluster/control", response: `<Error><Code>AccessDenied</Code></Error>`, code: 403, status: "unavailable", reads: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reads := 0
			client := s3.NewFromConfig(aws.Config{Region: "test", Credentials: aws.AnonymousCredentials{}, Retryer: func() aws.Retryer { return aws.NopRetryer{} }, HTTPClient: &http.Client{Transport: inspectTransport(func(r *http.Request) (*http.Response, error) {
				reads++
				if r.Method != http.MethodGet || r.URL.RawQuery != "x-id=GetObject" {
					t.Fatalf("unexpected request %s %s", r.Method, r.URL)
				}
				data := body
				if strings.HasSuffix(r.URL.Path, "/metadata/cluster.json") {
					data = manifest
				} else if !strings.HasSuffix(r.URL.Path, "/metadata/registry/cluster/control") {
					t.Fatalf("unexpected key %s", r.URL.Path)
				}
				code := 200
				if tc.failPath != "" && strings.HasSuffix(r.URL.Path, tc.failPath) {
					data = []byte(tc.response)
					code = tc.code
				}
				return &http.Response{StatusCode: code, Header: http.Header{"Etag": []string{`"authority-version"`}}, Body: io.NopCloser(strings.NewReader(string(data))), Request: r}, nil
			})}})
			got, err := inspectService(context.Background(), c, client)
			if string(got.Status) != tc.status || reads != tc.reads || got.Schema != 1 {
				t.Fatalf("got %+v reads=%d err=%v", got, reads, err)
			}
			if tc.status == "observed" {
				if err != nil || got.AuthorityVersion != `"authority-version"` || got.Control == nil || !reflect.DeepEqual(*got.Control, control) {
					t.Fatalf("snapshot differs: %+v err=%v", got, err)
				}
			} else if err == nil || got.Control != nil || got.Error == "" {
				t.Fatalf("fabricated authority: %+v err=%v", got, err)
			}
		})
	}
}

func TestInspectValidationAndCancellationBeforeClient(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	c, err := Load("../../test/scenarios/agent/a.json")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, err := Inspect(ctx, c)
	if got.Status != InspectionUnavailable || !errors.Is(err, context.Canceled) {
		t.Fatal(got, err)
	}
	got, err = Inspect(nil, c)
	if got.Status != InspectionInvalid || err == nil {
		t.Fatal(got, err)
	}
	c.ServiceStorage = nil
	got, err = Inspect(context.Background(), c)
	if got.Status != InspectionInvalid || err == nil {
		t.Fatal(got, err)
	}
}

func TestInspectionAuthorityReadHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	client := s3.NewFromConfig(aws.Config{Region: "test", Credentials: aws.AnonymousCredentials{}, Retryer: func() aws.Retryer { return aws.NopRetryer{} }, HTTPClient: &http.Client{Transport: inspectTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		cancel()
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}})
	got, err := inspectService(ctx, serviceConfig(), client)
	if calls != 1 || got.Status != InspectionUnavailable || got.Control != nil || !errors.Is(err, context.Canceled) {
		t.Fatal(calls, got, err)
	}
}
