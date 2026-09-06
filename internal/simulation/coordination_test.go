package simulation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"
	"time"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/directory"
	"github.com/0x63616c/xenon/internal/ownership"
	"github.com/0x63616c/xenon/internal/persistence"
	"github.com/0x63616c/xenon/internal/routing"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type coordinationSchedule struct {
	Schema                int      `json:"schema"`
	Seed                  int      `json:"seed"`
	OperationID           string   `json:"operation_id"`
	Input                 string   `json:"input"`
	LogicalStartUnix      int64    `json:"logical_start_unix"`
	RequestDeadlineUnix   int64    `json:"request_deadline_unix"`
	FailureTimeoutSeconds int64    `json:"failure_timeout_seconds"`
	ExpectedTrace         []string `json:"expected_trace"`
}

type topologyS3 struct {
	data    []byte
	version int
}

func (s *topologyS3) GetObject(ctx context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.data == nil {
		return nil, &smithy.GenericAPIError{Code: "NoSuchKey"}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(s.data)), ETag: aws.String(fmt.Sprint(s.version))}, nil
}

func (s *topologyS3) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if (in.IfNoneMatch != nil && s.data != nil) || (in.IfMatch != nil && *in.IfMatch != fmt.Sprint(s.version)) {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed"}
	}
	b, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	s.data = bytes.Clone(b)
	s.version++
	return &s3.PutObjectOutput{ETag: aws.String(fmt.Sprint(s.version))}, nil
}

type topologyResolver struct{ store *ownership.TopologyStore }

func (r topologyResolver) Resolve(ctx context.Context, partition string, _ bool) (routing.Route, error) {
	snapshot, err := r.store.Read(ctx)
	if err != nil {
		return routing.Route{}, err
	}
	topology := snapshot.Record()
	assignment, ok := topology.Partitions[partition]
	if !ok {
		return routing.Route{}, status.Error(codes.NotFound, "partition missing")
	}
	member, ok := topology.Members[assignment.Node]
	if !ok {
		return routing.Route{}, status.Error(codes.Unavailable, "owner missing")
	}
	return routing.Route{Node: assignment.Node, Address: member.Address}, nil
}

type logicalContext struct {
	context.Context
	deadline time.Time
}

func (c logicalContext) Deadline() (time.Time, bool) { return c.deadline, true }

func readCoordinationSchedule(t *testing.T) coordinationSchedule {
	t.Helper()
	b, err := os.ReadFile("../../test/scenarios/simulation/lost-response-crash-move.json")
	if err != nil {
		t.Fatal(err)
	}
	var schedule coordinationSchedule
	if err := json.Unmarshal(b, &schedule); err != nil {
		t.Fatal(err)
	}
	if schedule.Schema != 1 || schedule.Seed != 0 || schedule.OperationID == "" || schedule.RequestDeadlineUnix <= schedule.LogicalStartUnix || schedule.FailureTimeoutSeconds <= 0 {
		t.Fatal("invalid deterministic schedule")
	}
	return schedule
}

func identity(node string, suffix int) directory.Identity {
	return directory.Identity{Node: node, Address: node + ":7235", Incarnation: fmt.Sprintf("00000000-0000-4000-8000-%012d", suffix)}
}

type coordinationObservation struct {
	disk              *disk
	topology          ownership.Topology
	partition         string
	result            *wire.ShardResult
	trace             []string
	expectedTrace     []string
	staleAcknowledged bool
}

func checkCoordination(observed coordinationObservation) error {
	count := observed.disk.durable["v1/outcome_count"]
	if observed.result == nil || !bytes.Equal(observed.result.Data, []byte{1}) || !bytes.Equal(observed.disk.durable["value"], []byte{1}) {
		return fmt.Errorf("mutation or original result was not preserved exactly once")
	}
	if len(count) != 8 || binary.BigEndian.Uint64(count) != 1 || observed.disk.commits != 2 {
		return fmt.Errorf("durable replay outcome/count missing")
	}
	if _, ok := observed.topology.Members["b"]; ok || observed.topology.Partitions[observed.partition].Node != "a" {
		return fmt.Errorf("ownership did not move to surviving node")
	}
	if observed.staleAcknowledged {
		return fmt.Errorf("stale owner acknowledged after movement")
	}
	if !reflect.DeepEqual(observed.trace, observed.expectedTrace) {
		return fmt.Errorf("event trace mismatch: %v", observed.trace)
	}
	return nil
}

func shardHandler(store *ownership.TopologyStore, node string, d *disk, trace *[]string) routing.Local {
	return func(ctx context.Context, _ string, message proto.Message) (proto.Message, error) {
		req := message.(*wire.ShardRequest)
		snapshot, err := store.Read(ctx)
		if err != nil {
			return nil, err
		}
		if snapshot.Record().Partitions[req.Partition].Node != node {
			return nil, status.Error(codes.Unavailable, "stale owner")
		}
		before := len(d.durable["v1/outcome/"+req.OperationId])
		tx := d.begin()
		outcome, err := persistence.RunReplay(tx.effects(), req.OperationId, req.CommandSha256, 10)
		if err != nil {
			return nil, err
		}
		if trace != nil {
			if before == 0 {
				*trace = append(*trace, "owner-"+node+":commit")
			} else {
				*trace = append(*trace, "owner-"+node+":replay")
			}
		}
		return proto.Clone(outcome.GetShardResult()), nil
	}
}

func TestDeterministicCoordinationLostResponseCrashMove(t *testing.T) {
	schedule := readCoordinationSchedule(t)
	ctx := logicalContext{Context: context.Background(), deadline: time.Unix(schedule.RequestDeadlineUnix, 0)}
	fake := &topologyS3{}
	store, err := ownership.NewTopologyStore(fake, "bucket", "metadata")
	if err != nil {
		t.Fatal(err)
	}
	transition := 100
	if err := store.SetTransitionSource(func() string {
		transition++
		return fmt.Sprintf("00000000-0000-4000-8000-%012d", transition)
	}); err != nil {
		t.Fatal(err)
	}
	a, b := identity("a", 1), identity("b", 2)
	trace := []string{}
	if err := ownership.Join(ctx, store, a, "data", true); err != nil {
		t.Fatal(err)
	}
	trace = append(trace, "join:a")
	if err := ownership.Join(ctx, store, b, "data", false); err != nil {
		t.Fatal(err)
	}
	trace = append(trace, "join:b")
	snapshot, _ := store.Read(ctx)
	partition := ""
	for id, assignment := range snapshot.Record().Partitions {
		if assignment.Node == "b" && (partition == "" || id < partition) {
			partition = id
		}
	}
	if partition == "" {
		t.Fatal("join assigned no partition to b")
	}

	d := &disk{durable: map[string][]byte{}}
	handlers := map[string]routing.Local{"a": shardHandler(store, "a", d, &trace), "b": shardHandler(store, "b", d, &trace)}
	events := make(chan routing.Event, 16)
	router := &routing.Router{Node: "a", Directory: topologyResolver{store}, Local: handlers["a"], Events: events}
	digest := sha256.Sum256([]byte(schedule.Input))
	req := &wire.ShardRequest{Partition: partition, OperationId: schedule.OperationID, CommandSha256: digest[:]}
	forwardCalls := 0
	router.Invoke = func(c context.Context, address, method string, request, response proto.Message) error {
		forwardCalls++
		deadline, ok := c.Deadline()
		md, _ := metadata.FromOutgoingContext(c)
		if !ok || !deadline.Equal(ctx.deadline) || !proto.Equal(request, req) || method != wire.ShardPersistence_Execute_FullMethodName || !reflect.DeepEqual(md.Get("x-xenon-forward-hops"), []string{"1"}) {
			t.Fatal("forwarding changed operation identity, digest, deadline, method, or hop")
		}
		if address != b.Address {
			t.Fatalf("forwarded to %q", address)
		}
		result, err := handlers["b"](c, method, request)
		if err != nil {
			return err
		}
		proto.Merge(response, result)
		trace = append(trace, "transport:drop-response")
		return status.Error(codes.Unavailable, "response lost")
	}
	interceptor := router.Interceptor(func(string) proto.Message { return new(wire.ShardResult) })
	trace = append(trace, "submit:stable-endpoint")
	if _, err := interceptor(ctx, req, &grpc.UnaryServerInfo{FullMethod: wire.ShardPersistence_Execute_FullMethodName}, nil); status.Code(err) != codes.Unavailable || forwardCalls != 1 {
		t.Fatalf("untyped transport failure was retried by origin: calls=%d err=%v", forwardCalls, err)
	}

	trace = append(trace, "node-b:crash")
	membership, err := ownership.NewMembership(store, a, time.Duration(schedule.FailureTimeoutSeconds)*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(schedule.LogicalStartUnix, 0)
	if err := membership.Step(ctx, start); err != nil {
		t.Fatal(err)
	}
	trace = append(trace, "membership:observe")
	if err := membership.Step(ctx, start.Add(time.Duration(schedule.FailureTimeoutSeconds)*time.Second)); err != nil {
		t.Fatal(err)
	}
	trace = append(trace, "membership:evict-b", "retry:stable-endpoint")

	reply, err := interceptor(ctx, req, &grpc.UnaryServerInfo{FullMethod: wire.ShardPersistence_Execute_FullMethodName}, nil)
	if err != nil || !bytes.Equal(reply.(*wire.ShardResult).Data, []byte{1}) {
		t.Fatal("retry did not recover original result", reply, err)
	}
	trace = append(trace, "retry:original-result")
	changed := sha256.Sum256([]byte("different command"))
	changedReq := proto.Clone(req).(*wire.ShardRequest)
	changedReq.CommandSha256 = changed[:]
	if _, err := interceptor(ctx, changedReq, &grpc.UnaryServerInfo{FullMethod: wire.ShardPersistence_Execute_FullMethodName}, nil); status.Code(err) != codes.InvalidArgument {
		t.Fatal("changed input was accepted", err)
	}
	trace = append(trace, "changed-input:rejected")
	if _, err := handlers["b"](ctx, wire.ShardPersistence_Execute_FullMethodName, req); status.Code(err) != codes.Unavailable {
		t.Fatal("stale owner acknowledged", err)
	}
	trace = append(trace, "stale-owner:rejected")

	final, _ := store.Read(ctx)
	if err := checkCoordination(coordinationObservation{disk: d, topology: final.Record(), partition: partition, result: reply.(*wire.ShardResult), trace: trace, expectedTrace: schedule.ExpectedTrace}); err != nil {
		t.Fatal("independent invariant failed:", err)
	}
	encodedTrace, _ := json.Marshal(trace)
	t.Logf("coordination_trace=%s", encodedTrace)
	routingEvents := []routing.Event{}
	for len(events) > 0 {
		routingEvents = append(routingEvents, <-events)
	}
	if len(routingEvents) != 6 || routingEvents[1].Code != codes.Unavailable || routingEvents[3].Kind != "local" || routingEvents[5].Code != codes.InvalidArgument {
		t.Fatal("unexpected production routing events", routingEvents)
	}
}

func TestCheckerRejectsStaleOwnerAcknowledgement(t *testing.T) {
	// This observation is otherwise valid and injects the single faulty effect:
	// admission acknowledged on the withdrawn owner. It uses the same checker as
	// the coupled scenario, so deleting the checker invariant fails this control.
	d := &disk{durable: map[string][]byte{"value": {1}, "v1/outcome_count": {0, 0, 0, 0, 0, 0, 0, 1}}, commits: 2}
	topology := ownership.Topology{Members: map[string]ownership.Member{"a": {}}, Partitions: map[string]ownership.Assignment{"p": {Node: "a"}}}
	result := &wire.ShardResult{Data: []byte{1}}
	if checkCoordination(coordinationObservation{disk: d, topology: topology, partition: "p", result: result, staleAcknowledged: true}) == nil {
		t.Fatal("checker accepted faulty stale owner")
	}
}
