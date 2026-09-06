package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

type queueDropProxy struct {
	wire.UnimplementedQueuePersistenceServer
	backend wire.QueuePersistenceClient
	mu      sync.Mutex
	seen    *wire.QueueRequest
	dropped bool
	retry   bool
}

func (s *queueDropProxy) Execute(ctx context.Context, q *wire.QueueRequest) (*wire.QueueResult, error) {
	r, e := s.backend.Execute(ctx, q)
	if e != nil {
		return nil, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if q.Command.Kind == wire.QueueCommand_ENQUEUE_DLQ {
		if !s.dropped {
			s.dropped = true
			s.seen = proto.Clone(q).(*wire.QueueRequest)
			return nil, status.Error(codes.Unavailable, "declared lost completed DLQ response")
		}
		if q.OperationId == s.seen.OperationId {
			s.retry = proto.Equal(q, s.seen)
		}
	}
	return r, nil
}
func queueFixture(t *testing.T) namespaceCase {
	t.Helper()
	raw, e := os.ReadFile("../../../proof/queue/case.json")
	if e != nil {
		t.Fatal(e)
	}
	var c namespaceCase
	if e = json.Unmarshal(raw, &c); e != nil || c.SchemaVersion != 1 || c.Backend != "memory" || c.Fault != "drop_first_completed_dlq_enqueue_response" || c.NearLimitDataBytes != 1048576 || c.ResponseBudgetBytes != 3145728 {
		t.Fatal("invalid queue fixture", e)
	}
	return c
}
func TestQueueRPC(t *testing.T) {
	c := queueFixture(t)
	address := startNamespaceNode(t, c)
	conn, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	proxy := &queueDropProxy{backend: wire.NewQueuePersistenceClient(conn)}
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	srv := grpc.NewServer()
	wire.RegisterQueuePersistenceServer(srv, proxy)
	go srv.Serve(l)
	defer srv.Stop()
	q, e := NewQueue(l.Addr().String(), c.Partition, p.NamespaceReplicationQueueType)
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.TestTimeoutSeconds)*time.Second)
	defer cancel()
	initial := &commonpb.DataBlob{Data: c.InitialData, EncodingType: enumspb.EncodingType(c.Encoding)}
	updated := &commonpb.DataBlob{Data: c.UpdatedData, EncodingType: enumspb.EncodingType(c.Encoding)}
	if e = q.Init(ctx, initial); e != nil {
		t.Fatal(e)
	}
	if e = q.Init(ctx, updated); e != nil {
		t.Fatal(e)
	}
	m, e := q.GetAckLevels(ctx)
	if e != nil || m.Version != 0 || !proto.Equal(m.Blob, initial) {
		t.Fatal(m, e)
	}
	for version := int64(0); version < 2; version++ {
		if e = q.UpdateAckLevel(ctx, &p.InternalQueueMetadata{Blob: updated, Version: version}); e != nil {
			t.Fatal(e)
		}
		if e = q.UpdateDLQAckLevel(ctx, &p.InternalQueueMetadata{Blob: updated, Version: version}); e != nil {
			t.Fatal("DLQ version not honored", e)
		}
	}
	var unavailable *serviceerror.Unavailable
	if e = q.UpdateAckLevel(ctx, &p.InternalQueueMetadata{Blob: initial, Version: 0}); !errors.As(e, &unavailable) {
		t.Fatal("stale ACK accepted", e)
	}
	if e = q.UpdateDLQAckLevel(ctx, &p.InternalQueueMetadata{Blob: initial, Version: 0}); !errors.As(e, &unavailable) {
		t.Fatal("stale DLQ ACK accepted", e)
	}
	m, e = q.GetDLQAckLevels(ctx)
	if e != nil || m.Version != 2 || !proto.Equal(m.Blob, updated) {
		t.Fatal("stale write changed data", m, e)
	}
	for i := 0; i < 3; i++ {
		if e = q.EnqueueMessage(ctx, initial); e != nil {
			t.Fatal(e)
		}
		id, e := q.EnqueueMessageToDLQ(ctx, updated)
		if e != nil || id != int64(i) {
			t.Fatal("wrong inserted DLQ ID or duplicate retry", id, e)
		}
	}
	proxy.mu.Lock()
	observed := proxy.dropped && proxy.retry
	proxy.mu.Unlock()
	if !observed {
		t.Fatal("declared response loss/retry not observed")
	}
	messages, e := q.ReadMessages(ctx, -1, 10)
	if e != nil || len(messages) != 3 || messages[0].ID != 0 || messages[0].QueueType != 1 || !bytes.Equal(messages[0].Data, initial.Data) || messages[0].Encoding != enumspb.EncodingType(c.Encoding).String() {
		t.Fatal(messages, e)
	}
	dlq, token, e := q.ReadMessagesFromDLQ(ctx, -1, 2, 2, nil)
	if e != nil || len(dlq) != 2 || len(token) != 8 || dlq[0].QueueType != -1 || dlq[0].ID != 0 {
		t.Fatal(dlq, token, e)
	}
	dlq, token, e = q.ReadMessagesFromDLQ(ctx, -1, 2, 2, token)
	if e != nil || len(dlq) != 1 || dlq[0].ID != 2 || len(token) != 0 {
		t.Fatal(dlq, token, e)
	}
	var internal *serviceerror.Internal
	if _, _, e = q.ReadMessagesFromDLQ(ctx, -1, 2, 2, []byte{1}); !errors.As(e, &internal) {
		t.Fatal(e)
	}
	if e = q.DeleteMessagesBefore(ctx, 2); e != nil {
		t.Fatal(e)
	}
	messages, e = q.ReadMessages(ctx, -1, 10)
	if e != nil || len(messages) != 1 || messages[0].ID != 2 {
		t.Fatal("delete-before boundary", messages, e)
	}
	if e = q.RangeDeleteMessagesFromDLQ(ctx, 0, 1); e != nil {
		t.Fatal(e)
	}
	dlq, _, e = q.ReadMessagesFromDLQ(ctx, -1, 9, 10, nil)
	if e != nil || len(dlq) != 2 || dlq[0].ID != 0 || dlq[1].ID != 2 {
		t.Fatal("(first,last] delete boundary", dlq, e)
	}
	if e = q.DeleteMessageFromDLQ(ctx, 0); e != nil {
		t.Fatal(e)
	}
	if e = q.DeleteMessageFromDLQ(ctx, 2); e != nil {
		t.Fatal(e)
	}
	id, e := q.EnqueueMessageToDLQ(ctx, initial)
	if e != nil || id != 3 {
		t.Fatal("delete-all reused ID", id, e)
	}
	if e = q.DeleteMessagesBefore(ctx, 100); e != nil {
		t.Fatal(e)
	}
	if e = q.EnqueueMessage(ctx, updated); e != nil {
		t.Fatal(e)
	}
	messages, e = q.ReadMessages(ctx, -1, 10)
	if e != nil || len(messages) != 1 || messages[0].ID != 3 {
		t.Fatal(messages, e)
	}
}
func TestQueueByteBoundedPagination(t *testing.T) {
	c := queueFixture(t)
	address := startNamespaceNode(t, c)
	q, e := NewQueue(address, c.Partition, 1)
	if e != nil {
		t.Fatal(e)
	}
	defer q.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.TestTimeoutSeconds)*time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		_, e = q.EnqueueMessageToDLQ(ctx, &commonpb.DataBlob{Data: bytes.Repeat([]byte{byte(i + 1)}, c.NearLimitDataBytes), EncodingType: enumspb.ENCODING_TYPE_PROTO3})
		if e != nil {
			t.Fatal(e)
		}
	}
	rows, token, e := q.ReadMessagesFromDLQ(ctx, -1, 10, 1000, nil)
	if e != nil || len(rows) != 2 || len(token) != 8 {
		t.Fatal(len(rows), e)
	}
	rows, token, e = q.ReadMessagesFromDLQ(ctx, -1, 10, 1000, token)
	if e != nil || len(rows) != 1 || rows[0].ID != 2 || len(token) != 0 {
		t.Fatal(len(rows), e)
	}
}

type queueCancelClient struct{ code codes.Code }

func (c queueCancelClient) Execute(ctx context.Context, _ *wire.QueueRequest, _ ...grpc.CallOption) (*wire.QueueResult, error) {
	if ctx.Err() != nil {
		panic("requires live context")
	}
	return nil, status.Error(c.code, "remote cancellation")
}
func TestQueueRemoteCancellation(t *testing.T) {
	for _, tc := range []struct {
		code codes.Code
		want error
	}{{codes.Canceled, context.Canceled}, {codes.DeadlineExceeded, context.DeadlineExceeded}} {
		q := &Queue{client: queueCancelClient{tc.code}, invocationTimeout: time.Minute}
		e := q.DeleteMessagesBefore(context.Background(), 0)
		if !errors.Is(e, tc.want) {
			t.Fatal(e)
		}
	}
}
