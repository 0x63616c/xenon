package simulation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/ownership"
	"github.com/0x63616c/xenon/internal/routing"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// This drives the production Router and replay journal. The controls decide
// only when/how the transport delivers; they do not reproduce routing or
// idempotency decisions in the simulator.
func TestProductionTransportDropDelayDuplicate(t *testing.T) {
	for _, mode := range []string{"dropped", "delayed", "duplicated"} {
		t.Run(mode, func(t *testing.T) {
			ctx := logicalContext{Context: context.Background(), deadline: time.Unix(2_000_000_000, 0)}
			store, err := ownership.NewTopologyStore(&topologyS3{}, "bucket", "metadata")
			if err != nil {
				t.Fatal(err)
			}
			a, b := identity("a", 101), identity("b", 102)
			if err = ownership.Join(ctx, store, a, "data", true); err != nil {
				t.Fatal(err)
			}
			if err = ownership.Join(ctx, store, b, "data", false); err != nil {
				t.Fatal(err)
			}
			snapshot, err := store.Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			partition := ""
			for id, assignment := range snapshot.Record().Partitions {
				if assignment.Node == "b" {
					partition = id
					break
				}
			}
			if partition == "" {
				t.Fatal("transport target owns no partition")
			}

			d := &disk{durable: map[string][]byte{}}
			handler := shardHandler(store, "b", d, nil)
			router := &routing.Router{Node: "a", Directory: topologyResolver{store}, Local: shardHandler(store, "a", d, nil)}
			digest := sha256.Sum256([]byte("transport-fault"))
			req := &wire.ShardRequest{Partition: partition, OperationId: "op_transport_fault", CommandSha256: digest[:]}
			entered, release := make(chan struct{}, 1), make(chan struct{})
			calls := 0
			router.Invoke = func(c context.Context, _ string, method string, request, response proto.Message) error {
				calls++
				if mode == "dropped" && calls == 1 {
					return status.Error(codes.Unavailable, "request dropped")
				}
				if mode == "delayed" {
					entered <- struct{}{}
					select {
					case <-release:
					case <-c.Done():
						return c.Err()
					}
				}
				deliver := func() error {
					result, invokeErr := handler(c, method, request)
					if invokeErr == nil {
						proto.Merge(response, result)
					}
					return invokeErr
				}
				if err := deliver(); err != nil {
					return err
				}
				if mode == "duplicated" {
					return deliver()
				}
				return nil
			}
			invoke := router.Interceptor(func(string) proto.Message { return new(wire.ShardResult) })
			call := func() (*wire.ShardResult, error) {
				result, callErr := invoke(ctx, req, &grpc.UnaryServerInfo{FullMethod: wire.ShardPersistence_Execute_FullMethodName}, nil)
				if callErr != nil {
					return nil, callErr
				}
				return result.(*wire.ShardResult), nil
			}

			var result *wire.ShardResult
			switch mode {
			case "dropped":
				if _, err = call(); status.Code(err) != codes.Unavailable || len(d.durable) != 0 {
					t.Fatalf("dropped request reached owner: %v %+v", err, d.durable)
				}
				result, err = call()
			case "delayed":
				done := make(chan error, 1)
				go func() { result, err = call(); done <- err }()
				select {
				case <-entered:
				case <-time.After(3 * time.Second):
					t.Fatal("delay trigger not reached")
				}
				if len(d.durable) != 0 {
					t.Fatal("delayed request applied before release")
				}
				close(release)
				err = <-done
			default:
				result, err = call()
			}
			if err != nil || result == nil || !bytes.Equal(result.Data, []byte{1}) || !bytes.Equal(d.durable["value"], []byte{1}) {
				t.Fatalf("transport recovery failed: result=%v err=%v", result, err)
			}
			count := d.durable["v1/outcome_count"]
			if len(count) != 8 || binary.BigEndian.Uint64(count) != 1 {
				t.Fatal("delivery fault duplicated logical application")
			}
			wantCalls := 1
			if mode == "dropped" {
				wantCalls = 2
			}
			if calls != wantCalls {
				t.Fatalf("fault occurrence count=%d want=%d", calls, wantCalls)
			}
		})
	}
}

func TestCoupledOwnershipCutOccurrenceReceipt(t *testing.T) {
	c, _ := loadCoupled(t)
	result, err := RunCoupled(c, "")
	if err != nil {
		t.Fatal(err)
	}
	cuts := map[string]int{
		"reservation_publication":       0,
		"native_open_after_reservation": 0,
		"ready_after_open":              0,
		"renewal":                       0,
		"takeover":                      0,
		"stale_plan_after_takeover":     0,
		"stale_ready_after_replacement": 0,
		"displaced_writer_fenced":       0,
	}
	for _, entry := range result.Trace {
		i := entry.Input
		switch {
		case i.Actor == "writer-target" && i.Action == "publish" && i.Effect == 2 && entry.Accepted:
			cuts["reservation_publication"]++
		case i.Actor == "writer-target" && i.Action == "open" && i.Effect == 3 && entry.Accepted:
			cuts["native_open_after_reservation"]++
		case i.Actor == "writer-current" && i.Action == "publish" && i.Effect == 5 && entry.Accepted:
			cuts["ready_after_open"]++
		case i.Actor == "coordinator-old" && i.Action == "publish" && i.Effect == 4 && entry.Accepted:
			cuts["renewal"]++
		case i.Actor == "coordinator-new" && i.Action == "publish" && i.Effect == 3 && entry.Accepted:
			cuts["takeover"]++
		case i.Actor == "coordinator-old" && i.Action == "publish" && i.Effect == 6 && entry.Result == "conflict":
			cuts["stale_plan_after_takeover"]++
		case i.Actor == "writer-target" && i.Action == "publish" && i.Effect == 5 && entry.Result == "conflict":
			cuts["stale_ready_after_replacement"]++
		case i.Actor == "writer-current" && i.Action == "commit" && i.Effect == 3 && entry.Result == "fenced":
			cuts["displaced_writer_fenced"]++
		}
	}
	for cut, count := range cuts {
		if count != 1 {
			t.Fatalf("cut %s occurred %d times, want exactly one", cut, count)
		}
	}

	aba, err := RunCoupled(assignmentABAScenario(t), "")
	if err != nil {
		t.Fatal(err)
	}
	moves := 0
	for _, entry := range aba.Trace {
		if entry.Input.Actor != "coordinator-new" || entry.Input.Action != "publish" || (entry.Input.Effect != 7 && entry.Input.Effect != 9) || !entry.Accepted {
			continue
		}
		before, beforeErr := checkerControl(entry.Before)
		after, afterErr := checkerControl(entry.After)
		if beforeErr != nil || afterErr != nil {
			t.Fatal(errors.Join(beforeErr, afterErr))
		}
		if before.Partitions[c.RequiredPartition].Desired != after.Partitions[c.RequiredPartition].Desired {
			moves++
		}
	}
	if moves != 2 {
		t.Fatalf("assignment ABA moves=%d want=2", moves)
	}

	lost := c
	lost.Steps = append([]CoupledInput(nil), c.Steps...)
	lost.Steps[9].Fault = "lost_publish_response"
	lostResult, err := RunCoupled(lost, "")
	if err != nil {
		t.Fatal(err)
	}
	unknown := 0
	for _, entry := range lostResult.Trace {
		if entry.Input.Actor == "coordinator-old" && entry.Input.Action == "deliver" && entry.Input.Effect == 2 && entry.Result == "unknown_publication" {
			unknown++
		}
	}
	if unknown != 1 {
		t.Fatalf("lost renewal response occurrences=%d want=1", unknown)
	}
}
