//go:build integration_s3

package node

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/processcut"
	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	native "slatedb.io/slatedb-go/uniffi"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestS3ExecutionDiscoveryCuts(t *testing.T) {
	var f struct {
		Schema                                                      int
		Backend, Endpoint, Bucket, Partition, RPC, Control, Session string
		ShardID                                                     int32  `json:"shard_id"`
		OperationID                                                 string `json:"operation_id"`
		PauseTimeout                                                int    `json:"pause_timeout_ms"`
		Stages                                                      []string
	}
	raw, e := os.ReadFile("../../proof/process-cut/case.json")
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(raw, &f); e != nil || f.Schema != 1 || strings.Join(f.Stages, ",") != processcut.BeforeAwait+","+processcut.AfterAwait+","+processcut.BeforeReply || f.Endpoint != "http://127.0.0.1:19010" {
		t.Fatal("invalid process-cut fixture", e)
	}
	for _, stage := range f.Stages {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			ns := "12345678-1234-1234-1234-123456789abc"
			run := "12345678-1234-1234-1234-123456789def"
			stateBytes, _ := proto.Marshal(&persistencespb.WorkflowExecutionState{RunId: run, State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING})
			image := &wire.ExecutionImage{NamespaceId: ns, WorkflowId: "controlled-real-execution", RunId: run, ExecutionStateProto: stateBytes, ExecutionInfoBlob: &wire.HistoryBlob{Data: []byte("before"), Encoding: 2}, ExecutionStateBlob: &wire.HistoryBlob{Data: stateBytes, Encoding: 3}, NextEventId: 2, DbRecordVersion: 1}
			request := func(id string, c *wire.ExecutionCommand) *wire.ExecutionRequest {
				q := executionRequest(id, c)
				q.Partition = f.Partition
				return q
			}
			next := proto.Clone(image).(*wire.ExecutionImage)
			next.DbRecordVersion = 2
			next.ExecutionInfoBlob.Data = []byte("after")
			target := request(uuid.NewString(), &wire.ExecutionCommand{Kind: wire.ExecutionCommand_UPDATE, ShardId: f.ShardID, RangeId: 1, Mode: 2, Mutation: &wire.ExecutionMutation{Upsert: next}})
			f.OperationID = target.OperationId
			expected := processcut.Selector{OperationID: target.OperationId, Partition: f.Partition, Family: "execution", Kind: "UPDATE", Digest: hex.EncodeToString(target.CommandSha256)}
			plan := processcut.Plan{Schema: 1, Session: f.Session, Listen: f.Control, Discovery: true, Stage: stage, TimeoutMS: f.PauseTimeout}

			planPath := filepath.Join(t.TempDir(), "plan.json")
			b, _ := json.Marshal(plan)
			if e = os.WriteFile(planPath, b, 0600); e != nil {
				t.Fatal(e)
			}
			prefix := "execution-discovery/" + stage
			start := func(withPlan bool) (*exec.Cmd, func()) {
				directory := t.TempDir()
				child := exec.CommandContext(ctx, os.Getenv("XENON_NODE_BINARY"))
				child.Dir = directory
				env := os.Environ()
				for _, pair := range []string{"XENON_TOPOLOGY_PREFIX=", "XENON_BACKEND=s3", "XENON_BUCKET=" + f.Bucket, "XENON_PREFIX=" + prefix, "XENON_PARTITION=" + f.Partition, "XENON_LISTEN=" + f.RPC, "XENON_PROCESS_CUT_PLAN="} {
					env = append(env, pair)
				}
				if withPlan {
					env = append(env, "XENON_PROCESS_CUT_PLAN="+planPath)
				}
				child.Env = env
				output, e := child.StdoutPipe()
				if e != nil {
					t.Fatal(e)
				}
				child.Stderr = child.Stdout
				if e = child.Start(); e != nil {
					t.Fatal(e)
				}
				ready := make(chan struct{})
				drained := make(chan struct{})
				go func() {
					defer close(drained)
					scanner := bufio.NewScanner(output)
					for scanner.Scan() {
						if strings.HasPrefix(scanner.Text(), "READY ") {
							select {
							case <-ready:
							default:
								close(ready)
							}
						}
					}
				}()
				stopped := false
				stop := func() {
					if stopped {
						return
					}
					stopped = true
					_ = child.Process.Kill()
					_ = child.Wait()
					select {
					case <-drained:
					case <-time.After(2 * time.Second):
						t.Error("child output did not drain")
					}
				}
				t.Cleanup(stop)
				select {
				case <-ready:
				case <-ctx.Done():
					stop()
					t.Fatal("node readiness deadline")
				}
				return child, stop
			}
			child, stop := start(true)
			conn, e := grpc.NewClient(f.RPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if e != nil {
				t.Fatal(e)
			}
			defer conn.Close()
			client := wire.NewExecutionPersistenceClient(conn)
			shard := &wire.ShardCommand{Kind: wire.ShardCommand_CREATE_OR_GET, ShardId: f.ShardID, RangeId: 1}
			enc, _ := proto.MarshalOptions{Deterministic: true}.Marshal(shard)
			digest := sha256.Sum256(enc)
			if r, e := wire.NewShardPersistenceClient(conn).Execute(ctx, &wire.ShardRequest{ProtocolVersion: 1, Partition: f.Partition, OperationId: "shard", CommandSha256: digest[:], Command: shard}); e != nil || r.Error != wire.ShardResult_NONE {
				t.Fatal(r, e)
			}
			baseline, e := client.Execute(ctx, request("baseline", &wire.ExecutionCommand{Kind: wire.ExecutionCommand_SET, ShardId: f.ShardID, RangeId: 1, Snapshot: image}))
			if e != nil || baseline.Error != wire.ExecutionResult_NONE {
				t.Fatal("baseline not acknowledged", e)
			}

			httpClient := &http.Client{Timeout: time.Second}
			state := func() processcut.State {
				t.Helper()
				response, e := httpClient.Get("http://" + f.Control + "/state")
				if e != nil {
					t.Fatal(e)
				}
				defer response.Body.Close()
				var s processcut.State
				if e = json.NewDecoder(response.Body).Decode(&s); e != nil {
					t.Fatal(e)
				}
				return s
			}
			initial := state()
			if initial.PID != child.Process.Pid || initial.State != "unarmed" {
				t.Fatal("wrong controlled process", initial)
			}
			arm, _ := json.Marshal(map[string]any{"session": initial.Session, "incarnation": initial.Incarnation, "watch": processcut.Workflow{Namespace: ns, Workflow: image.WorkflowId, Run: run}})
			response, e := httpClient.Post("http://"+f.Control+"/watch", "application/json", bytes.NewReader(arm))
			if e != nil {
				t.Fatal(e)
			}
			_ = response.Body.Close()
			if response.StatusCode != 202 {
				t.Fatal("arm rejected")
			}
			type rpcResult struct {
				result *wire.ExecutionResult
				err    error
			}
			completed := make(chan rpcResult, 1)
			go func() {
				r, e := client.Execute(ctx, target)
				completed <- rpcResult{r, e}
			}()
			var hit processcut.State
			for deadline := time.Now().Add(3 * time.Second); ; {
				hit = state()
				if hit.State == "candidate" {
					if hit.Selector != expected || hit.Incarnation != initial.Incarnation {
						t.Fatal("wrong discovered candidate", hit)
					}
					arm, _ := json.Marshal(map[string]any{"session": initial.Session, "incarnation": initial.Incarnation, "selector": hit.Selector})
					response, e := httpClient.Post("http://"+f.Control+"/arm", "application/json", bytes.NewReader(arm))
					if e != nil {
						t.Fatal(e)
					}
					response.Body.Close()
					if response.StatusCode != 202 {
						t.Fatal("candidate arm rejected")
					}
				}
				if hit.State == "paused" {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("declared barrier never observed", hit)
				}
				select {
				case e := <-completed:
					t.Fatal("request ended before cut", e)
				case <-time.After(time.Millisecond):
				}
			}
			if hit.PID != child.Process.Pid || hit.Incarnation != initial.Incarnation || hit.Selector != expected || hit.Stage != stage || hit.HitUTC == "" {
				t.Fatal("wrong barrier identity", hit)
			}
			// This is the injected fault: kill the exact live OS process, not an error return.
			hitTime, e := time.Parse(time.RFC3339Nano, hit.HitUTC)
			if e != nil || !time.Now().Before(hitTime.Add(time.Duration(f.PauseTimeout)*time.Millisecond)) {
				t.Fatal("barrier expired before injection")
			}
			select {
			case reply := <-completed:
				t.Fatal("RPC already ended before signal", reply)
			default:
			}
			if e = child.Process.Kill(); e != nil {
				t.Fatal("SIGKILL injection failed", e)
			}
			stop()
			waitStatus, ok := child.ProcessState.Sys().(syscall.WaitStatus)
			if !ok || !waitStatus.Signaled() || waitStatus.Signal() != syscall.SIGKILL {
				t.Fatal("process was not killed by declared signal")
			}
			select {
			case reply := <-completed:
				if reply.err == nil || reply.result != nil {
					t.Fatal("lost RPC appeared successful", reply.result)
				}
			case <-ctx.Done():
				t.Fatal("RPC did not fail after kill")
			}
			inspect := func(requireTarget bool) bool {
				store, e := native.ObjectStoreResolve("s3://" + f.Bucket)
				if e != nil {
					t.Fatal(e)
				}
				defer store.Destroy()
				builder := native.NewDbBuilder(prefix, store)
				db, e := builder.Build()
				builder.Destroy()
				if e != nil {
					t.Fatal(e)
				}
				defer func() {
					if e := db.Shutdown(); e != nil {
						t.Error(e)
					}
					db.Destroy()
				}()
				value, e := db.Get([]byte(execKey(f.ShardID, ns, image.WorkflowId, run)))
				if e != nil || value == nil {
					t.Fatal("acknowledged baseline lost", e)
				}
				row := new(wire.ExecutionImage)
				if e = proto.Unmarshal(*value, row); e != nil {
					t.Fatal(e)
				}
				journal, e := db.Get([]byte("v1/outcome/" + f.OperationID))
				if e != nil {
					t.Fatal(e)
				}
				present := journal != nil
				count, e := db.Get([]byte("v1/outcome_count"))
				if e != nil || count == nil || len(*count) != 8 {
					t.Fatal("missing atomic count", e)
				}
				if !present {
					if requireTarget || row.DbRecordVersion != 1 || string(row.ExecutionInfoBlob.Data) != "before" || binary.BigEndian.Uint64(*count) != 2 {
						t.Fatal("absent journal with changed/required state", row.DbRecordVersion)
					}
					return false
				}
				out := new(wire.StoredOutcome)
				if e = proto.Unmarshal(*journal, out); e != nil {
					t.Fatal(e)
				}
				r := out.GetExecutionResult()
				if r == nil || r.Error != wire.ExecutionResult_NONE || !bytes.Equal(out.CommandSha256, target.CommandSha256) || row.DbRecordVersion != 2 || string(row.ExecutionInfoBlob.Data) != "after" || binary.BigEndian.Uint64(*count) != 3 {
					t.Fatal("partial or invalid durable batch")
				}
				return true
			}
			present := inspect(stage != processcut.BeforeAwait)
			_ = conn.Close()
			_, recoveredStop := start(false)
			recoveredConn, e := grpc.NewClient(f.RPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if e != nil {
				t.Fatal(e)
			}
			defer recoveredConn.Close()
			replayed, e := wire.NewExecutionPersistenceClient(recoveredConn).Execute(ctx, target)
			if e != nil || replayed.Error != wire.ExecutionResult_NONE {
				t.Fatal("recovery/replay failed", e, replayed)
			}
			recoveredStop()
			inspect(true)
			event, _ := json.Marshal(map[string]any{"stage": stage, "hit": hit, "injected_signal": "SIGKILL", "journal_present_before_retry": present, "raw_state_journal_count_atomic": true, "replay_preserved": true})
			t.Log(string(event))
		})
	}
}
