package adapter

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"google.golang.org/grpc"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type shardCase struct {
	Fault                 string `json:"fault"`
	SchemaVersion         int    `json:"schema_version"`
	Backend               string `json:"backend"`
	Partition             string `json:"partition"`
	Prefix                string `json:"prefix"`
	ShardID               int32  `json:"shard_id"`
	InitialRange          int64  `json:"initial_range"`
	UpdatedRange          int64  `json:"updated_range"`
	InitialData           []byte `json:"initial_data"`
	UpdatedData           []byte `json:"updated_data"`
	Encoding              int32  `json:"encoding"`
	StartupTimeoutSeconds int    `json:"startup_timeout_seconds"`
	TestTimeoutSeconds    int    `json:"test_timeout_seconds"`
}

func TestShardRPC(t *testing.T) {
	data, err := os.ReadFile("../../proof/shard/case.json")
	if err != nil {
		t.Fatal(err)
	}
	var cfg shardCase
	if err = json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Fault != "drop_first_completed_update_response" {
		t.Fatal("unknown fault schedule")
	}
	if cfg.SchemaVersion != 1 {
		t.Fatal("unknown fixture version")
	}
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
		t.Fatalf("build the pinned node via scripts/test-shard.sh: %v", err)
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
	var address string
	select {
	case address = <-ready:
		if address == "" {
			t.Fatal("node exited before readiness")
		}
	case <-time.After(time.Duration(cfg.StartupTimeoutSeconds) * time.Second):
		t.Fatal("node startup timeout")
	}
	store, err := NewShardStore(address, cfg.Partition, "proof-cluster")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.TestTimeoutSeconds)*time.Second)
	defer cancel()
	calls := 0
	create := &persistence.InternalGetOrCreateShardRequest{ShardID: cfg.ShardID, CreateShardInfo: func() (int64, *commonpb.DataBlob, error) {
		calls++
		return cfg.InitialRange, &commonpb.DataBlob{Data: cfg.InitialData, EncodingType: enumspb.EncodingType(cfg.Encoding)}, nil
	}}
	result, err := store.GetOrCreateShard(ctx, create)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || !proto.Equal(result.ShardInfo, &commonpb.DataBlob{Data: cfg.InitialData, EncodingType: enumspb.EncodingType(cfg.Encoding)}) {
		t.Fatalf("create callback/blob mismatch: %v", result)
	}
	create.CreateShardInfo = func() (int64, *commonpb.DataBlob, error) {
		t.Error("callback invoked for existing shard")
		return 0, nil, errors.New("must not run")
	}
	if _, err = store.GetOrCreateShard(ctx, create); err != nil {
		t.Fatal(err)
	}
	if _, err = store.GetOrCreateShard(ctx, &persistence.InternalGetOrCreateShardRequest{ShardID: cfg.ShardID + 1}); err == nil {
		t.Fatal("missing shard returned success")
	} else {
		var missing *serviceerror.NotFound
		if !errors.As(err, &missing) {
			t.Fatalf("wrong missing error %T", err)
		}
	}
	missingUpdate := &persistence.InternalUpdateShardRequest{ShardID: cfg.ShardID + 100, ShardInfo: &commonpb.DataBlob{}}
	if missingErr := store.UpdateShard(ctx, missingUpdate); missingErr == nil {
		t.Fatal("missing update succeeded")
	} else {
		var unavailable *serviceerror.Unavailable
		if !errors.As(missingErr, &unavailable) {
			t.Fatalf("missing update wrong type: %T", missingErr)
		}
	}
	update := &persistence.InternalUpdateShardRequest{ShardID: cfg.ShardID, PreviousRangeID: cfg.InitialRange, RangeID: cfg.UpdatedRange, ShardInfo: &commonpb.DataBlob{Data: cfg.UpdatedData, EncodingType: enumspb.EncodingType(cfg.Encoding)}}
	if err = store.UpdateShard(ctx, update); err != nil {
		t.Fatal(err)
	}
	if err = store.UpdateShard(ctx, update); err == nil {
		t.Fatal("stale update returned success")
	} else {
		var lost *persistence.ShardOwnershipLostError
		if !errors.As(err, &lost) || lost.ShardID != cfg.ShardID {
			t.Fatalf("typed ownership error lost: %T %v", err, err)
		}
	}
	if err = store.AssertShardOwnership(ctx, &persistence.AssertShardOwnershipRequest{ShardID: cfg.ShardID, RangeID: cfg.InitialRange}); err == nil {
		t.Fatal("assert is a no-op")
	}
	if err = store.AssertShardOwnership(ctx, &persistence.AssertShardOwnershipRequest{ShardID: cfg.ShardID, RangeID: cfg.UpdatedRange}); err != nil {
		t.Fatal(err)
	}
	raw := &wire.ShardCommand{Kind: wire.ShardCommand_UPDATE, ShardId: cfg.ShardID, PreviousRangeId: cfg.UpdatedRange, RangeId: cfg.UpdatedRange + 1, Data: cfg.InitialData, Encoding: cfg.Encoding}
	encoded, _ := proto.MarshalOptions{Deterministic: true}.Marshal(raw)
	digest := sha256.Sum256(encoded)
	request := &wire.ShardRequest{ProtocolVersion: 1, Partition: cfg.Partition, OperationId: "lost-response-replay", CommandSha256: digest[:], Command: raw}
	original, err := store.client.Execute(ctx, request)
	if err != nil || original.Error != wire.ShardResult_NONE {
		t.Fatalf("raw mutation: %v %v", original, err)
	}
	update.PreviousRangeID = cfg.UpdatedRange + 1
	update.RangeID = cfg.UpdatedRange + 2
	if err = store.UpdateShard(ctx, update); err != nil {
		t.Fatal(err)
	}
	replay, err := store.client.Execute(ctx, request)
	if err != nil || !proto.Equal(original, replay) {
		t.Fatalf("durable result replay differs: %v %v", replay, err)
	}
	if err = store.AssertShardOwnership(ctx, &persistence.AssertShardOwnershipRequest{ShardID: cfg.ShardID, RangeID: cfg.UpdatedRange + 2}); err != nil {
		t.Fatal("replay mutated newer state", err)
	}
	request.Command.RangeId++
	encoded, _ = proto.MarshalOptions{Deterministic: true}.Marshal(request.Command)
	digest = sha256.Sum256(encoded)
	request.CommandSha256 = digest[:]
	if _, err = store.client.Execute(ctx, request); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("changed payload reused ID: %v", err)
	}

	// Drop the first completed Rust response at a real forwarding gRPC boundary.
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	proxy := &dropResponseProxy{backend: store.client}
	server := grpc.NewServer()
	wire.RegisterShardPersistenceServer(server, proxy)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	retrying, e := NewShardStore(listener.Addr().String(), cfg.Partition, "proof-cluster")
	if e != nil {
		t.Fatal(e)
	}
	defer retrying.Close()
	update.PreviousRangeID = cfg.UpdatedRange + 2
	update.RangeID = cfg.UpdatedRange + 3
	if e = retrying.UpdateShard(ctx, update); e != nil {
		t.Fatal("lost response failed to reconcile", e)
	}
	proxy.mu.Lock()
	ids := append([]string(nil), proxy.ids...)
	proxy.mu.Unlock()
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("transport retry changed identity: %v", ids)
	}
	if e = store.AssertShardOwnership(ctx, &persistence.AssertShardOwnershipRequest{ShardID: cfg.ShardID, RangeID: cfg.UpdatedRange + 3}); e != nil {
		t.Fatal(e)
	}

	// Independent invocations racing to create converge on one durable blob.
	var wg sync.WaitGroup
	responses := make(chan *persistence.InternalGetOrCreateShardResponse, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(value byte) {
			defer wg.Done()
			res, e := store.GetOrCreateShard(ctx, &persistence.InternalGetOrCreateShardRequest{ShardID: cfg.ShardID + 2, CreateShardInfo: func() (int64, *commonpb.DataBlob, error) { return 1, &commonpb.DataBlob{Data: []byte{value}}, nil }})
			responses <- res
			errs <- e
		}(byte(i))
	}
	wg.Wait()
	for i := 0; i < 2; i++ {
		if err = <-errs; err != nil {
			t.Fatal(err)
		}
	}
	first, second := <-responses, <-responses
	if !proto.Equal(first.ShardInfo, second.ShardInfo) {
		t.Fatal("competing creates did not converge")
	}
}

type dropResponseProxy struct {
	wire.UnimplementedShardPersistenceServer
	backend wire.ShardPersistenceClient
	mu      sync.Mutex
	ids     []string
}

func (p *dropResponseProxy) Execute(ctx context.Context, req *wire.ShardRequest) (*wire.ShardResult, error) {
	result, err := p.backend.Execute(ctx, req)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.ids = append(p.ids, req.OperationId)
	first := len(p.ids) == 1
	p.mu.Unlock()
	if first {
		return nil, status.Error(codes.Unavailable, "injected response loss after durable backend completion")
	}
	return result, nil
}

type statusProxy struct {
	wire.UnimplementedShardPersistenceServer
	code codes.Code
	hang bool
}

func (p *statusProxy) Execute(ctx context.Context, _ *wire.ShardRequest) (*wire.ShardResult, error) {
	if p.hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return nil, status.Error(p.code, "injected backend status")
}
func TestShardTransportBoundsAndTypes(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.ResourceExhausted, codes.InvalidArgument, codes.OK} {
		t.Run(code.String(), func(t *testing.T) {
			listener, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			server := grpc.NewServer()
			wire.RegisterShardPersistenceServer(server, &statusProxy{code: code, hang: code == codes.OK})
			go func() { _ = server.Serve(listener) }()
			defer server.Stop()
			store, e := NewShardStore(listener.Addr().String(), "p", "c")
			if e != nil {
				t.Fatal(e)
			}
			defer store.Close()
			store.invocationTimeout = 200 * time.Millisecond
			started := time.Now()
			_, e = store.GetOrCreateShard(context.Background(), &persistence.InternalGetOrCreateShardRequest{ShardID: 1})
			switch code {
			case codes.Unavailable:
				// The same operation now spends its original budget on admission.
				if !errors.Is(e, context.DeadlineExceeded) {
					t.Fatalf("%T %v", e, e)
				}
			case codes.ResourceExhausted:
				var typed *serviceerror.ResourceExhausted
				if !errors.As(e, &typed) {
					t.Fatalf("%T %v", e, e)
				}
			case codes.InvalidArgument:
				var typed *serviceerror.InvalidArgument
				if !errors.As(e, &typed) {
					t.Fatalf("%T %v", e, e)
				}
			case codes.OK:
				// The server may close the HTTP/2 stream with CANCEL when its
				// derived deadline fires before the client timer. Both paths must
				// remain typed and bounded; exact remote mappings are tested below.
				if !errors.Is(e, context.DeadlineExceeded) && !errors.Is(e, context.Canceled) {
					t.Fatalf("hung invocation: %v", e)
				}
				if time.Since(started) > time.Second {
					t.Fatal("unbounded invocation")
				}
			}
		})
	}
}

// Return transport cancellation while the caller context is still live, so the
// result cannot depend on which of the remote and local timers fires first.
type cancellationClient struct{ code codes.Code }

func (c cancellationClient) Execute(ctx context.Context, _ *wire.ShardRequest, _ ...grpc.CallOption) (*wire.ShardResult, error) {
	if ctx.Err() != nil {
		panic("test requires a live context")
	}
	return nil, status.Error(c.code, "remote cancellation")
}
func TestShardRemoteCancellationTypes(t *testing.T) {
	for _, tc := range []struct {
		code codes.Code
		want error
	}{
		{codes.DeadlineExceeded, context.DeadlineExceeded},
		{codes.Canceled, context.Canceled},
	} {
		t.Run(tc.code.String(), func(t *testing.T) {
			store := &ShardStore{client: cancellationClient{tc.code}, invocationTimeout: time.Minute}
			_, err := store.invoke(context.Background(), &wire.ShardCommand{Kind: wire.ShardCommand_GET})
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %T %v, want %v", err, err, tc.want)
			}
		})
	}
}
