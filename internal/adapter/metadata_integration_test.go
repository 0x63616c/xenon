package adapter

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type namespaceCase struct {
	NearLimitDataBytes    int      `json:"near_limit_data_bytes"`
	ResponseBudgetBytes   int      `json:"response_budget_bytes"`
	SchemaVersion         int      `json:"schema_version"`
	Backend               string   `json:"backend"`
	Partition             string   `json:"partition"`
	Prefix                string   `json:"prefix"`
	IDs                   []string `json:"ids"`
	Names                 []string `json:"names"`
	InitialData           []byte   `json:"initial_data"`
	UpdatedData           []byte   `json:"updated_data"`
	Encoding              int32    `json:"encoding"`
	PageSize              int      `json:"page_size"`
	StartupTimeoutSeconds int      `json:"startup_timeout_seconds"`
	TestTimeoutSeconds    int      `json:"test_timeout_seconds"`
	Fault                 string   `json:"fault"`
}

func startNamespaceNode(t *testing.T, cfg namespaceCase) string {
	t.Helper()
	binary, err := filepath.Abs("../../target/debug/xenon-node")
	if err != nil {
		t.Fatal(err)
	}
	if value := os.Getenv("XENON_NODE_BINARY"); value != "" {
		binary = value
	}
	command := exec.Command(binary)
	command.Env = append(os.Environ(), "XENON_BACKEND="+cfg.Backend, "XENON_PARTITION="+cfg.Partition, "XENON_PREFIX="+cfg.Prefix, "XENON_LISTEN=127.0.0.1:0")
	command.Stderr = os.Stderr
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = command.Start(); err != nil {
		t.Fatal("build xenon-node before namespace fixture", err)
	}
	t.Cleanup(func() { _ = command.Process.Kill(); _ = command.Wait() })
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if address, ok := strings.CutPrefix(scanner.Text(), "READY "); ok {
				ready <- address
				return
			}
		}
		ready <- ""
	}()
	select {
	case address := <-ready:
		if address == "" {
			t.Fatal("node exited before readiness")
		}
		return address
	case <-time.After(time.Duration(cfg.StartupTimeoutSeconds) * time.Second):
		t.Fatal("namespace node startup timeout")
	}
	return ""
}
func TestNamespaceRPC(t *testing.T) {
	fixture, err := os.ReadFile("../../proof/namespace/case.json")
	if err != nil {
		t.Fatal(err)
	}
	var cfg namespaceCase
	if err = json.Unmarshal(fixture, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.SchemaVersion != 1 || cfg.Backend != "memory" || cfg.Fault != "drop_first_completed_rename_response" || len(cfg.IDs) != 3 || len(cfg.Names) != 3 || cfg.PageSize != 1 {
		t.Fatal("unknown namespace fixture")
	}
	address := startNamespaceNode(t, cfg)
	store, err := NewMetadataStore(address, cfg.Partition)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.TestTimeoutSeconds)*time.Second)
	defer cancel()
	initial := &commonpb.DataBlob{Data: cfg.InitialData, EncodingType: enumspb.EncodingType(cfg.Encoding)}
	updated := &commonpb.DataBlob{Data: cfg.UpdatedData, EncodingType: enumspb.EncodingType(cfg.Encoding)}
	assertVersion := func(want int64) {
		t.Helper()
		m, e := store.GetMetadata(ctx)
		if e != nil || m.NotificationVersion != want {
			t.Fatalf("metadata version want %d: %v %v", want, m, e)
		}
	}
	assertUnavailable := func(e error) {
		t.Helper()
		var typed *serviceerror.Unavailable
		if !errors.As(e, &typed) {
			t.Fatalf("want typed Unavailable, got %T %v", e, e)
		}
	}
	assertMissing := func(r *persistence.GetNamespaceRequest) {
		t.Helper()
		_, e := store.GetNamespace(ctx, r)
		var typed *serviceerror.NamespaceNotFound
		if !errors.As(e, &typed) {
			t.Fatalf("want NamespaceNotFound, got %T %v", e, e)
		}
	}
	assertVersion(1)
	for i := 0; i < 2; i++ {
		r, e := store.CreateNamespace(ctx, &persistence.InternalCreateNamespaceRequest{ID: cfg.IDs[i], Name: cfg.Names[i], Namespace: initial, IsGlobal: i == 0})
		if e != nil || r.ID != cfg.IDs[i] {
			t.Fatalf("create %v %v", r, e)
		}
	}
	assertVersion(3)
	// Both unique ID and unique name conflict without incrementing catalog version.
	for _, r := range []*persistence.InternalCreateNamespaceRequest{{ID: cfg.IDs[0], Name: "other", Namespace: initial}, {ID: cfg.IDs[2], Name: cfg.Names[0], Namespace: initial}} {
		_, e := store.CreateNamespace(ctx, r)
		var typed *serviceerror.NamespaceAlreadyExists
		if !errors.As(e, &typed) {
			t.Fatalf("want NamespaceAlreadyExists: %T %v", e, e)
		}
	}
	assertVersion(3)
	for _, r := range []*persistence.GetNamespaceRequest{{ID: cfg.IDs[0]}, {Name: cfg.Names[0]}} {
		item, e := store.GetNamespace(ctx, r)
		if e != nil || !proto.Equal(item.Namespace, initial) || !item.IsGlobal || item.NotificationVersion != 1 {
			t.Fatalf("opaque blob/global/version changed: %v %v", item, e)
		}
	}
	assertMissing(&persistence.GetNamespaceRequest{ID: cfg.IDs[2]})
	for _, r := range []*persistence.GetNamespaceRequest{{}, {ID: cfg.IDs[0], Name: cfg.Names[0]}, {ID: "not-uuid"}} {
		_, e := store.GetNamespace(ctx, r)
		var typed *serviceerror.InvalidArgument
		if !errors.As(e, &typed) {
			t.Fatalf("invalid request wrong error: %T %v", e, e)
		}
	}
	update := &persistence.InternalUpdateNamespaceRequest{Id: cfg.IDs[0], Name: cfg.Names[0], Namespace: updated, NotificationVersion: 2, IsGlobal: false}
	assertUnavailable(store.UpdateNamespace(ctx, update))
	assertVersion(3)
	update.NotificationVersion = 3
	if e := store.UpdateNamespace(ctx, update); e != nil {
		t.Fatal(e)
	}
	assertVersion(4)
	item, e := store.GetNamespace(ctx, &persistence.GetNamespaceRequest{ID: cfg.IDs[0]})
	if e != nil || !proto.Equal(item.Namespace, updated) || item.IsGlobal || item.NotificationVersion != 3 {
		t.Fatalf("update fields: %v %v", item, e)
	}
	// Collision must roll back old name/index, payload and global counter together.
	update.Name = cfg.Names[1]
	update.NotificationVersion = 4
	update.Namespace = initial
	assertUnavailable(store.RenameNamespace(ctx, &persistence.InternalRenameNamespaceRequest{InternalUpdateNamespaceRequest: update, PreviousName: cfg.Names[0]}))
	assertVersion(4)
	item, e = store.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: cfg.Names[0]})
	if e != nil || !proto.Equal(item.Namespace, updated) {
		t.Fatalf("rename collision partially changed state: %v %v", item, e)
	}
	// Pinned SQL uses ID + notification guard, not PreviousName as a condition.
	update.Name = cfg.Names[2]
	update.Namespace = updated
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	proxy := &metadataDropProxy{backend: store.client}
	server := grpc.NewServer()
	wire.RegisterMetadataPersistenceServer(server, proxy)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	retrying, e := NewMetadataStore(listener.Addr().String(), cfg.Partition)
	if e != nil {
		t.Fatal(e)
	}
	defer retrying.Close()
	if e = retrying.RenameNamespace(ctx, &persistence.InternalRenameNamespaceRequest{InternalUpdateNamespaceRequest: update, PreviousName: "ignored-by-pinned-SQL"}); e != nil {
		t.Fatal(e)
	}
	proxy.mu.Lock()
	ids := append([]string(nil), proxy.ids...)
	proxy.mu.Unlock()
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("lost response changed retry identity: %v", ids)
	}
	assertVersion(5)
	assertMissing(&persistence.GetNamespaceRequest{Name: cfg.Names[0]})
	if _, e = store.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: cfg.Names[2]}); e != nil {
		t.Fatal(e)
	}
	// Frozen-data pagination sorts UUID bytes and returns opaque complete blobs.
	page, e := store.ListNamespaces(ctx, &persistence.InternalListNamespacesRequest{PageSize: cfg.PageSize})
	if e != nil || len(page.Namespaces) != 1 || !proto.Equal(page.Namespaces[0].Namespace, updated) || len(page.NextPageToken) != 16 {
		t.Fatalf("page1: %v %v", page, e)
	}
	page, e = store.ListNamespaces(ctx, &persistence.InternalListNamespacesRequest{PageSize: cfg.PageSize, NextPageToken: page.NextPageToken})
	if e != nil || len(page.Namespaces) != 1 || !proto.Equal(page.Namespaces[0].Namespace, initial) {
		t.Fatalf("page2: %v %v", page, e)
	}
	page, e = store.ListNamespaces(ctx, &persistence.InternalListNamespacesRequest{PageSize: cfg.PageSize, NextPageToken: page.NextPageToken})
	if e != nil || len(page.Namespaces) != 0 || len(page.NextPageToken) != 0 {
		t.Fatalf("final page: %v %v", page, e)
	}
	// Raw replay remains stable after later mutations; cross-family IDs are rejected.
	raw := &wire.MetadataCommand{Kind: wire.MetadataCommand_GET_METADATA}
	encoded, _ := proto.MarshalOptions{Deterministic: true}.Marshal(raw)
	hash := sha256.Sum256(encoded)
	request := &wire.MetadataRequest{ProtocolVersion: 1, Partition: cfg.Partition, OperationId: "global-family-replay", CommandSha256: hash[:], Command: raw}
	original, e := store.client.Execute(ctx, request)
	if e != nil || original.NotificationVersion != 5 {
		t.Fatalf("raw catalog: %v %v", original, e)
	}
	update.NotificationVersion = 5
	if e = store.UpdateNamespace(ctx, update); e != nil {
		t.Fatal(e)
	}
	assertVersion(6)
	replay, e := store.client.Execute(ctx, request)
	if e != nil || !proto.Equal(original, replay) {
		t.Fatalf("journal replay changed: %v %v", replay, e)
	}
	shard := wire.NewShardPersistenceClient(store.connection)
	sc := &wire.ShardCommand{Kind: wire.ShardCommand_GET, ShardId: 7}
	encoded, _ = proto.MarshalOptions{Deterministic: true}.Marshal(sc)
	hash = sha256.Sum256(encoded)
	_, e = shard.Execute(ctx, &wire.ShardRequest{ProtocolVersion: 1, Partition: cfg.Partition, OperationId: request.OperationId, CommandSha256: hash[:], Command: sc})
	if status.Code(e) != codes.InvalidArgument {
		t.Fatalf("cross-family ID accepted: %v", e)
	}
	// Deletion is idempotent and leaves notification version unchanged.
	if e = store.DeleteNamespace(ctx, &persistence.DeleteNamespaceRequest{ID: cfg.IDs[0]}); e != nil {
		t.Fatal(e)
	}
	if e = store.DeleteNamespace(ctx, &persistence.DeleteNamespaceRequest{ID: cfg.IDs[0]}); e != nil {
		t.Fatal(e)
	}
	assertMissing(&persistence.GetNamespaceRequest{Name: cfg.Names[2]})
	if e = store.DeleteNamespaceByName(ctx, &persistence.DeleteNamespaceByNameRequest{Name: cfg.Names[1]}); e != nil {
		t.Fatal(e)
	}
	if e = store.DeleteNamespaceByName(ctx, &persistence.DeleteNamespaceByNameRequest{Name: cfg.Names[1]}); e != nil {
		t.Fatal(e)
	}
	assertVersion(6)
	all, e := store.ListNamespaces(ctx, &persistence.InternalListNamespacesRequest{PageSize: 10})
	if e != nil || len(all.Namespaces) != 0 {
		t.Fatalf("delete index left records: %v %v", all, e)
	}
}

// This tests the default Go receive limit, not a raised transport allowance.
func TestNamespaceByteBoundedPagination(t *testing.T) {
	fixture, err := os.ReadFile("../../proof/namespace/case.json")
	if err != nil {
		t.Fatal(err)
	}
	var cfg namespaceCase
	if err = json.Unmarshal(fixture, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.SchemaVersion != 1 || cfg.Backend != "memory" || cfg.NearLimitDataBytes != 1048576 || cfg.ResponseBudgetBytes != 3145728 || len(cfg.IDs) != 3 {
		t.Fatal("unknown byte-budget fixture")
	}
	address := startNamespaceNode(t, cfg)
	store, err := NewMetadataStore(address, cfg.Partition)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.TestTimeoutSeconds)*time.Second)
	defer cancel()
	for i, id := range cfg.IDs {
		_, err = store.CreateNamespace(ctx, &persistence.InternalCreateNamespaceRequest{ID: id, Name: cfg.Names[i], Namespace: &commonpb.DataBlob{Data: bytes.Repeat([]byte{byte(i + 1)}, cfg.NearLimitDataBytes), EncodingType: enumspb.EncodingType(cfg.Encoding)}})
		if err != nil {
			t.Fatal(err)
		}
	}
	raw := &wire.MetadataCommand{Kind: wire.MetadataCommand_LIST, PageSize: 1000}
	encoded, _ := proto.MarshalOptions{Deterministic: true}.Marshal(raw)
	digest := sha256.Sum256(encoded)
	request := &wire.MetadataRequest{ProtocolVersion: 1, Partition: cfg.Partition, OperationId: "byte-bounded-page", CommandSha256: digest[:], Command: raw}
	first, err := store.client.Execute(ctx, request)
	if err != nil {
		t.Fatal("near-limit page undeliverable", err)
	}
	if len(first.Namespaces) != 2 || len(first.NextPageToken) != 16 || proto.Size(first) > cfg.ResponseBudgetBytes {
		t.Fatalf("byte cap or continuation incorrect: count=%d bytes=%d token=%x", len(first.Namespaces), proto.Size(first), first.NextPageToken)
	}
	replay, err := store.client.Execute(ctx, request)
	if err != nil || !proto.Equal(first, replay) {
		t.Fatal("journaled near-limit page cannot replay", err)
	}
	seen := map[byte]bool{}
	token := []byte(nil)
	pages := 0
	for {
		page, e := store.ListNamespaces(ctx, &persistence.InternalListNamespacesRequest{PageSize: 1000, NextPageToken: token})
		if e != nil {
			t.Fatal(e)
		}
		pages++
		for _, item := range page.Namespaces {
			data := item.Namespace.Data
			if len(data) != cfg.NearLimitDataBytes || data[0] < 1 || data[0] > 3 || !bytes.Equal(data, bytes.Repeat(data[:1], cfg.NearLimitDataBytes)) || seen[data[0]] {
				t.Fatal("partial, duplicate or corrupt near-limit record")
			}
			seen[data[0]] = true
		}
		token = page.NextPageToken
		if len(token) == 0 {
			break
		}
		if pages > 3 {
			t.Fatal("pagination did not advance")
		}
	}
	if pages != 2 || len(seen) != 3 {
		t.Fatalf("incomplete traversal: pages=%d records=%d", pages, len(seen))
	}
}

type metadataDropProxy struct {
	wire.UnimplementedMetadataPersistenceServer
	backend wire.MetadataPersistenceClient
	mu      sync.Mutex
	ids     []string
}

func (p *metadataDropProxy) Execute(ctx context.Context, r *wire.MetadataRequest) (*wire.MetadataResult, error) {
	result, err := p.backend.Execute(ctx, r)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.ids = append(p.ids, r.OperationId)
	first := len(p.ids) == 1
	p.mu.Unlock()
	if first {
		return nil, status.Error(codes.Unavailable, "injected response loss after durable namespace rename")
	}
	return result, nil
}
