//go:build integration_s3

package adapter

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type compatibilityCase struct {
	SchemaVersion         int      `json:"schema_version"`
	Backend               string   `json:"backend"`
	Partition             string   `json:"partition"`
	Prefix                string   `json:"prefix"`
	ShardID               int32    `json:"shard_id"`
	InitialRange          int64    `json:"initial_range"`
	RustUpdatedRange      int64    `json:"rust_updated_range"`
	GoUpdatedRange        int64    `json:"go_updated_range"`
	InitialData           []byte   `json:"initial_data"`
	RustUpdatedData       []byte   `json:"rust_updated_data"`
	GoUpdatedData         []byte   `json:"go_updated_data"`
	Encoding              int32    `json:"encoding"`
	StartupTimeoutSeconds int      `json:"startup_timeout_seconds"`
	TestTimeoutSeconds    int      `json:"test_timeout_seconds"`
	Schedule              []string `json:"schedule"`
}

func compatibilityNode(t *testing.T, binary string, cfg compatibilityCase) (wire.ShardPersistenceClient, func()) {
	t.Helper()
	if binary == "" {
		t.Fatal("explicit freshly built compatibility node binary required")
	}
	command := exec.Command(binary)
	command.Env = append(os.Environ(), "XENON_BACKEND=s3", "XENON_PARTITION="+cfg.Partition, "XENON_PREFIX="+cfg.Prefix, "XENON_LISTEN=127.0.0.1:0")
	command.Stderr = os.Stderr
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stopProcess := func() {
		once.Do(func() {
			_ = command.Process.Kill()
			done := make(chan error, 1)
			go func() { done <- command.Wait() }()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("terminated node failed to exit")
			}
		})
	}
	t.Cleanup(stopProcess)
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
		t.Fatal("node startup deadline")
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return wire.NewShardPersistenceClient(conn), func() { _ = conn.Close(); stopProcess() }
}

func TestShardStoredCompatibility(t *testing.T) {
	data, err := os.ReadFile("../../proof/go-shard-compat/case.json")
	if err != nil {
		t.Fatal(err)
	}
	var cfg compatibilityCase
	if err = json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.SchemaVersion != 1 || cfg.Backend != "s3-emulator" || strings.Join(cfg.Schedule, ",") != "rust_durable_write,kill_rust,go_recover_replay_update,kill_go,rust_recover_replay" {
		t.Fatal("unknown compatibility fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.TestTimeoutSeconds)*time.Second)
	defer cancel()
	request := func(id string, command *wire.ShardCommand) *wire.ShardRequest {
		payload, e := proto.MarshalOptions{Deterministic: true}.Marshal(command)
		if e != nil {
			t.Fatal(e)
		}
		hash := sha256.Sum256(payload)
		return &wire.ShardRequest{ProtocolVersion: 1, Partition: cfg.Partition, OperationId: id, CommandSha256: hash[:], Command: command}
	}
	call := func(t *testing.T, client wire.ShardPersistenceClient, r *wire.ShardRequest) *wire.ShardResult {
		t.Helper()
		result, e := client.Execute(ctx, r)
		if e != nil || result.Error != wire.ShardResult_NONE {
			t.Fatalf("durable RPC: %v %v", result, e)
		}
		return result
	}
	check := func(t *testing.T, result *wire.ShardResult, rangeID int64, blob []byte) {
		t.Helper()
		want := &wire.ShardResult{ShardId: cfg.ShardID, RangeId: rangeID, Data: blob, Encoding: cfg.Encoding}
		if !proto.Equal(result, want) {
			t.Fatalf("stored payload/range/encoding mismatch: got %v want %v", result, want)
		}
	}
	create := request("compat-rust-create", &wire.ShardCommand{Kind: wire.ShardCommand_CREATE_OR_GET, ShardId: cfg.ShardID, RangeId: cfg.InitialRange, Data: cfg.InitialData, Encoding: cfg.Encoding})
	rustUpdate := request("compat-rust-update", &wire.ShardCommand{Kind: wire.ShardCommand_UPDATE, ShardId: cfg.ShardID, PreviousRangeId: cfg.InitialRange, RangeId: cfg.RustUpdatedRange, Data: cfg.RustUpdatedData, Encoding: cfg.Encoding})
	goUpdate := request("compat-go-update", &wire.ShardCommand{Kind: wire.ShardCommand_UPDATE, ShardId: cfg.ShardID, PreviousRangeId: cfg.RustUpdatedRange, RangeId: cfg.GoUpdatedRange, Data: cfg.GoUpdatedData, Encoding: cfg.Encoding})
	var initialResult, rustResult, goResult *wire.ShardResult
	if !t.Run("RustWrites", func(t *testing.T) {
		client, stop := compatibilityNode(t, os.Getenv("XENON_COMPAT_RUST_BINARY"), cfg)
		initialResult = call(t, client, create)
		check(t, initialResult, cfg.InitialRange, cfg.InitialData)
		rustResult = call(t, client, rustUpdate)
		check(t, rustResult, cfg.RustUpdatedRange, cfg.RustUpdatedData)
		stop()
	}) {
		return
	}
	if !t.Run("GoReadsReplaysAndWrites", func(t *testing.T) {
		client, stop := compatibilityNode(t, os.Getenv("XENON_COMPAT_GO_BINARY"), cfg)
		observed := call(t, client, request("compat-go-read-rust", &wire.ShardCommand{Kind: wire.ShardCommand_GET, ShardId: cfg.ShardID}))
		check(t, observed, cfg.RustUpdatedRange, cfg.RustUpdatedData)
		if !proto.Equal(call(t, client, create), initialResult) || !proto.Equal(call(t, client, rustUpdate), rustResult) {
			t.Fatal("Go did not replay Rust-written durable results")
		}
		changed := request(create.OperationId, &wire.ShardCommand{Kind: wire.ShardCommand_GET, ShardId: cfg.ShardID})
		if _, e := client.Execute(ctx, changed); status.Code(e) != codes.InvalidArgument {
			t.Fatal("Go accepted changed payload for Rust-written request identity", e)
		}
		goResult = call(t, client, goUpdate)
		check(t, goResult, cfg.GoUpdatedRange, cfg.GoUpdatedData)
		stop()
	}) {
		return
	}
	t.Run("RustReadsReplaysGoWrites", func(t *testing.T) {
		client, stop := compatibilityNode(t, os.Getenv("XENON_COMPAT_RUST_BINARY"), cfg)
		defer stop()
		observed := call(t, client, request("compat-rust-read-go", &wire.ShardCommand{Kind: wire.ShardCommand_GET, ShardId: cfg.ShardID}))
		check(t, observed, cfg.GoUpdatedRange, cfg.GoUpdatedData)
		if !proto.Equal(call(t, client, goUpdate), goResult) {
			t.Fatal("Rust did not replay Go-written durable result")
		}
		if !proto.Equal(call(t, client, create), initialResult) {
			t.Fatal("original Rust result lost after Go owner update")
		}
		check(t, call(t, client, request("compat-final-state", &wire.ShardCommand{Kind: wire.ShardCommand_GET, ShardId: cfg.ShardID})), cfg.GoUpdatedRange, cfg.GoUpdatedData)
	})
}
