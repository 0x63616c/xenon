package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/adapter"
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type matchingFixture struct {
	Schema           int
	Namespace, Queue string
	Range            int64
	Tasks            []int64
	Subqueues        []int
	BlobBytes        int `json:"blob_bytes"`
	PageSize         int `json:"page_size"`
	Schedule         []string
}

func matchingCase(t *testing.T) matchingFixture {
	b, e := os.ReadFile("../../proof/go-matching/case.json")
	if e != nil {
		t.Fatal(e)
	}
	var f matchingFixture
	if e = json.Unmarshal(b, &f); e != nil || f.Schema != 1 || len(f.Tasks) != 3 || len(f.Subqueues) != 2 || strings.Join(f.Schedule, ",") != "drop_first_create_response,stale_batch_rollback,duplicate_batch_rollback,multi_subqueue_commit,page_and_complete,reopen_replay" {
		t.Fatal("invalid matching fixture", e)
	}
	return f
}
func matchingReq(id string, c *wire.MatchingCommand) *wire.MatchingRequest {
	b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	h := sha256.Sum256(b)
	return &wire.MatchingRequest{ProtocolVersion: 1, Partition: "p", OperationId: id, CommandSha256: h[:], Command: c}
}
func TestGoOwnerMatchingRPC(t *testing.T) {
	f := matchingCase(t)
	o := owner(t, engine(t, objects(t), cfg(t).Prefix+"-matching", false))
	defer closeOwner(t, o)
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	var dropped atomic.Bool
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		r, e := h(ctx, req)
		if q, ok := req.(*wire.MatchingRequest); ok && q.Command.Kind == wire.MatchingCommand_CREATE_QUEUE && e == nil && dropped.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "completed response loss")
		}
		return r, e
	}))
	wire.RegisterMatchingPersistenceServer(server, &MatchingServer{Owner: o})
	go server.Serve(listener)
	defer server.Stop()
	s, e := adapter.NewMatchingStore(listener.Addr().String(), "p")
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	blob := &commonpb.DataBlob{Data: []byte{0, 255, 7}, EncodingType: enumspb.ENCODING_TYPE_PROTO3}
	typ := enumspb.TASK_QUEUE_TYPE_WORKFLOW
	getq := &p.InternalGetTaskQueueRequest{NamespaceID: f.Namespace, TaskQueue: f.Queue, TaskType: typ}
	var missing *serviceerror.NotFound
	if _, e = s.GetTaskQueue(ctx, getq); !errors.As(e, &missing) {
		t.Fatal("missing", e)
	}
	create := &p.InternalCreateTaskQueueRequest{NamespaceID: f.Namespace, TaskQueue: f.Queue, TaskType: typ, RangeID: f.Range, TaskQueueInfo: blob}
	if e = s.CreateTaskQueue(ctx, create); e != nil || !dropped.Load() {
		t.Fatal("lost response retry", e)
	}
	var cond *p.ConditionFailedError
	if e = s.CreateTaskQueue(ctx, create); !errors.As(e, &cond) {
		t.Fatal("duplicate queue", e)
	}
	q, e := s.GetTaskQueue(ctx, getq)
	if e != nil || q.RangeID != f.Range || !proto.Equal(q.TaskQueueInfo, blob) {
		t.Fatal(q, e)
	}
	update := &p.InternalUpdateTaskQueueRequest{NamespaceID: f.Namespace, TaskQueue: f.Queue, TaskType: typ, PrevRangeID: f.Range - 1, RangeID: f.Range + 1, TaskQueueInfo: blob}
	if _, e = s.UpdateTaskQueue(ctx, update); !errors.As(e, &cond) {
		t.Fatal("stale queue", e)
	}
	tasks := &p.InternalCreateTasksRequest{NamespaceID: f.Namespace, TaskQueue: f.Queue, TaskType: typ, RangeID: f.Range - 1}
	for _, sub := range f.Subqueues {
		for _, id := range f.Tasks {
			tasks.Tasks = append(tasks.Tasks, &p.InternalCreateTask{TaskId: id, Subqueue: sub, Task: &commonpb.DataBlob{Data: []byte{byte(id)}, EncodingType: enumspb.ENCODING_TYPE_PROTO3}})
		}
	}
	if _, e = s.CreateTasks(ctx, tasks); !errors.As(e, &cond) {
		t.Fatal("stale batch", e)
	}
	query := &p.GetTasksRequest{NamespaceID: f.Namespace, TaskQueue: f.Queue, TaskType: typ, ExclusiveMaxTaskID: 4, PageSize: f.PageSize}
	page, e := s.GetTasks(ctx, query)
	if e != nil || len(page.Tasks) != 0 {
		t.Fatal("stale batch leaked", page, e)
	}
	tasks.RangeID = f.Range
	if r, e := s.CreateTasks(ctx, tasks); e != nil || r.UpdatedMetadata {
		t.Fatal(r, e)
	}
	// Batch puts a new ID first then hits a duplicate; no partial new ID may appear.
	dup := *tasks
	dup.Tasks = []*p.InternalCreateTask{{TaskId: 0, Task: blob}, tasks.Tasks[0]}
	var unavailable *serviceerror.Unavailable
	if _, e = s.CreateTasks(ctx, &dup); !errors.As(e, &unavailable) {
		t.Fatal("duplicate batch", e)
	}
	for _, sub := range f.Subqueues {
		query.Subqueue = sub
		query.NextPageToken = nil
		var values []byte
		for i := 0; i < 5; i++ {
			page, e = s.GetTasks(ctx, query)
			if e != nil {
				t.Fatal(e)
			}
			for _, v := range page.Tasks {
				values = append(values, v.Data[0])
			}
			if len(page.NextPageToken) == 0 {
				break
			}
			query.NextPageToken = page.NextPageToken
		}
		if !bytes.Equal(values, []byte{1, 2, 3}) {
			t.Fatal("order/rollback/subqueue", values)
		}
	}
	n, e := s.CompleteTasksLessThan(ctx, &p.CompleteTasksLessThanRequest{NamespaceID: f.Namespace, TaskQueueName: f.Queue, TaskType: typ, ExclusiveMaxTaskID: 3, Limit: 1})
	if e != nil || n != 1 {
		t.Fatal(n, e)
	}
	query.Subqueue = 0
	query.NextPageToken = nil
	query.PageSize = 100
	page, e = s.GetTasks(ctx, query)
	if e != nil || len(page.Tasks) != 2 || page.Tasks[0].Data[0] != 2 {
		t.Fatal(page, e)
	}
	list, e := s.ListTaskQueue(ctx, &p.ListTaskQueueRequest{PageSize: 1})
	if e != nil || len(list.Items) != 1 || !proto.Equal(list.Items[0].TaskQueue, blob) {
		t.Fatal(list, e)
	}
	update.PrevRangeID = f.Range
	if _, e = s.UpdateTaskQueue(ctx, update); e != nil {
		t.Fatal(e)
	}
	del := &p.DeleteTaskQueueRequest{TaskQueue: &p.TaskQueueKey{NamespaceID: f.Namespace, TaskQueueName: f.Queue, TaskQueueType: typ}, RangeID: f.Range}
	if e = s.DeleteTaskQueue(ctx, del); !errors.As(e, &cond) {
		t.Fatal("stale delete", e)
	}
	del.RangeID++
	if e = s.DeleteTaskQueue(ctx, del); e != nil {
		t.Fatal(e)
	}
	if _, e = s.GetTaskQueue(ctx, getq); !errors.As(e, &missing) {
		t.Fatal(e)
	}
}
func TestGoOwnerMatchingRecovery(t *testing.T) {
	f := matchingCase(t)
	objects := objects(t)
	path := cfg(t).Prefix + "-matching-recovery"
	o := owner(t, engine(t, objects, path, false))
	s := &MatchingServer{Owner: o}
	id := uuid.MustParse(f.Namespace)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req := matchingReq("matching-create", &wire.MatchingCommand{Kind: wire.MatchingCommand_CREATE_QUEUE, NamespaceId: id[:], Queue: f.Queue, RangeId: f.Range, Data: []byte{0, 255}, Encoding: 2})
	first, e := s.Execute(ctx, req)
	if e != nil || first.Error != 0 {
		t.Fatal(first, e)
	}

	taskCommand := proto.Clone(req.Command).(*wire.MatchingCommand)
	taskCommand.Kind = wire.MatchingCommand_CREATE_TASKS
	taskCommand.Tasks = []*wire.MatchingTask{{Id: 1, Subqueue: 1, Data: []byte{0, 254}, Encoding: 2}}
	taskReq := matchingReq("matching-task-create", taskCommand)
	taskResult, e := s.Execute(ctx, taskReq)
	if e != nil || taskResult.Error != 0 {
		t.Fatal(taskResult, e)
	}
	closeOwner(t, o)
	o = owner(t, engine(t, objects, path, false))
	defer closeOwner(t, o)
	s = &MatchingServer{Owner: o}
	again, e := s.Execute(ctx, req)
	if e != nil || !proto.Equal(first, again) {
		t.Fatal("replay", again, e)
	}
	taskAgain, e := s.Execute(ctx, taskReq)
	if e != nil || !proto.Equal(taskAgain, taskResult) {
		t.Fatal("task replay", taskAgain, e)
	}
	taskGet := proto.Clone(taskCommand).(*wire.MatchingCommand)
	taskGet.Kind = wire.MatchingCommand_GET_TASKS
	taskGet.Tasks = nil
	taskGet.Subqueue = 1
	taskGet.MaxId = 2
	taskGet.PageSize = 10
	stored, e := s.Execute(ctx, matchingReq("matching-task-get", taskGet))
	if e != nil || len(stored.Tasks) != 1 || !proto.Equal(stored.Tasks[0], taskCommand.Tasks[0]) {
		t.Fatal("task recovery", stored, e)
	}
	get := proto.Clone(req.Command).(*wire.MatchingCommand)
	get.Kind = wire.MatchingCommand_GET_QUEUE
	r, e := s.Execute(ctx, matchingReq("matching-get", get))
	if e != nil || len(r.Queues) != 1 || r.Queues[0].RangeId != f.Range {
		t.Fatal(r, e)
	}
	if _, e = s.Execute(ctx, matchingReq(req.OperationId, get)); status.Code(e) != codes.InvalidArgument {
		t.Fatal("changed identity", e)
	}

	competitor := owner(t, engine(t, objects, path, false))
	defer closeOwner(t, competitor)
	deleteCommand := proto.Clone(req.Command).(*wire.MatchingCommand)
	deleteCommand.Kind = wire.MatchingCommand_DELETE_QUEUE
	if _, e = s.Execute(ctx, matchingReq("matching-fenced-delete", deleteCommand)); status.Code(e) != codes.Unavailable || !o.Quarantined() {
		t.Fatal("fenced delete must quarantine", e)
	}
}

func TestGoOwnerMatchingBytePages(t *testing.T) {
	f := matchingCase(t)
	o := owner(t, engine(t, objects(t), cfg(t).Prefix+"-matching-bytes", false))
	defer closeOwner(t, o)
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	server := grpc.NewServer()
	wire.RegisterMatchingPersistenceServer(server, &MatchingServer{Owner: o})
	go server.Serve(listener)
	defer server.Stop()
	s, e := adapter.NewMatchingStore(listener.Addr().String(), "p")
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	blob := &commonpb.DataBlob{Data: bytes.Repeat([]byte{0xff}, f.BlobBytes), EncodingType: enumspb.ENCODING_TYPE_PROTO3}
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("large-%d", i)
		if e = s.CreateTaskQueue(ctx, &p.InternalCreateTaskQueueRequest{NamespaceID: f.Namespace, TaskQueue: name, RangeID: f.Range, TaskQueueInfo: blob}); e != nil {
			t.Fatal(e)
		}
		if _, e = s.CreateTasks(ctx, &p.InternalCreateTasksRequest{NamespaceID: f.Namespace, TaskQueue: "large-0", RangeID: f.Range, Tasks: []*p.InternalCreateTask{{TaskId: int64(i), Task: blob}}}); e != nil {
			t.Fatal(e)
		}
	}
	token := []byte(nil)
	count := 0
	for page := 0; page < 4; page++ {
		r, e := s.ListTaskQueue(ctx, &p.ListTaskQueueRequest{PageSize: 1000, PageToken: token})
		if e != nil {
			t.Fatal(e)
		}
		if len(r.Items) > 2 {
			t.Fatal("queue byte cap absent")
		}
		for _, v := range r.Items {
			if !proto.Equal(v.TaskQueue, blob) {
				t.Fatal("queue blob changed")
			}
			count++
		}
		token = r.NextPageToken
		if len(token) == 0 {
			break
		}
	}
	if count != 3 {
		t.Fatal("queue pagination skipped", count)
	}
	token = nil
	count = 0
	for page := 0; page < 4; page++ {
		r, e := s.GetTasks(ctx, &p.GetTasksRequest{NamespaceID: f.Namespace, TaskQueue: "large-0", ExclusiveMaxTaskID: 3, PageSize: 1000, NextPageToken: token})
		if e != nil {
			t.Fatal(e)
		}
		if len(r.Tasks) > 2 {
			t.Fatal("task byte cap absent")
		}
		for _, v := range r.Tasks {
			if !proto.Equal(v, blob) {
				t.Fatal("task blob changed")
			}
			count++
		}
		token = r.NextPageToken
		if len(token) == 0 {
			break
		}
	}
	if count != 3 {
		t.Fatal("task pagination skipped", count)
	}
}
