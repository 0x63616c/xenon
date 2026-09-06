package adapter

import (
	"context"
	"crypto/sha256"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/rpctrace"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"time"
)

type Queue struct {
	operations        *operationIDs
	connection        *grpc.ClientConn
	client            wire.QueuePersistenceClient
	partition         string
	queueType         p.QueueType
	invocationTimeout time.Duration
}

var _ p.Queue = (*Queue)(nil)

func NewQueue(address, partition string, queueType p.QueueType, options ...StoreOption) (*Queue, error) {
	conn, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithChainUnaryInterceptor(rpctrace.Unary))
	if e != nil {
		return nil, e
	}
	return &Queue{operations: newOperationIDs(options...), connection: conn, client: wire.NewQueuePersistenceClient(conn), partition: partition, queueType: queueType, invocationTimeout: 30 * time.Second}, nil
}
func (q *Queue) Close() {
	if q.connection != nil {
		_ = q.connection.Close()
	}
}
func (q *Queue) invokeQueue(ctx context.Context, c *wire.QueueCommand) (traceResult *wire.QueueResult, traceErr error) {
	ctx, traceFinish, traceBeginErr := rpctrace.BeginObserved(ctx, "queue")
	if traceBeginErr != nil {
		return nil, traceBeginErr
	}
	defer func() { traceErr = traceFinish(traceErr) }()
	ctx, cancel := context.WithTimeout(ctx, q.invocationTimeout)
	defer cancel()
	c.QueueType = int32(q.queueType)
	raw, e := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if e != nil {
		return nil, e
	}
	digest := sha256.Sum256(raw)
	operation, operationErr := q.operations.next()
	if operationErr != nil {
		return nil, operationErr
	}
	req := &wire.QueueRequest{ProtocolVersion: 1, Partition: q.partition, OperationId: operation, CommandSha256: digest[:], Command: c}
	for attempt := 0; attempt < 3; attempt++ {
		r, e := q.client.Execute(ctx, req)
		if e == nil {
			if r == nil {
				return nil, serviceerror.NewInternal("nil queue result")
			}
			switch r.Error {
			case wire.QueueResult_NONE:
				return r, nil
			case wire.QueueResult_UNAVAILABLE:
				return nil, serviceerror.NewUnavailable(r.Message)
			case wire.QueueResult_INTERNAL:
				return nil, serviceerror.NewInternal(r.Message)
			default:
				return nil, serviceerror.NewInternal("unknown queue error")
			}
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		switch status.Code(e) {
		case codes.DeadlineExceeded:
			return nil, context.DeadlineExceeded
		case codes.Canceled:
			return nil, context.Canceled
		}
		if status.Code(e) != codes.Unavailable || attempt == 2 {
			return nil, serviceerror.FromStatus(status.Convert(e))
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 20 * time.Millisecond):
		}
	}
	panic("unreachable")
}
func queueBlob(kind wire.QueueCommand_Kind, b *commonpb.DataBlob) (*wire.QueueCommand, error) {
	if b == nil {
		return nil, serviceerror.NewInvalidArgument("nil queue blob")
	}
	return &wire.QueueCommand{Kind: kind, Data: b.Data, Encoding: int32(b.EncodingType)}, nil
}
func (q *Queue) blob(ctx context.Context, k wire.QueueCommand_Kind, b *commonpb.DataBlob) (*wire.QueueResult, error) {
	c, e := queueBlob(k, b)
	if e != nil {
		return nil, e
	}
	return q.invokeQueue(ctx, c)
}
func (q *Queue) Init(ctx context.Context, b *commonpb.DataBlob) error {
	_, e := q.blob(ctx, wire.QueueCommand_INIT, b)
	return e
}
func (q *Queue) EnqueueMessage(ctx context.Context, b *commonpb.DataBlob) error {
	_, e := q.blob(ctx, wire.QueueCommand_ENQUEUE, b)
	return e
}
func (q *Queue) EnqueueMessageToDLQ(ctx context.Context, b *commonpb.DataBlob) (int64, error) {
	r, e := q.blob(ctx, wire.QueueCommand_ENQUEUE_DLQ, b)
	if e != nil {
		return p.EmptyQueueMessageID, e
	}
	return r.MessageId, nil
}
func queueMessages(r *wire.QueueResult) ([]*p.QueueMessage, error) {
	out := make([]*p.QueueMessage, 0, len(r.Messages))
	for _, m := range r.Messages {
		if m == nil {
			return nil, serviceerror.NewInternal("nil queue entry")
		}
		out = append(out, &p.QueueMessage{QueueType: p.QueueType(m.QueueType), ID: m.Id, Data: m.Data, Encoding: m.Encoding})
	}
	return out, nil
}
func (q *Queue) ReadMessages(ctx context.Context, lastID int64, maxCount int) ([]*p.QueueMessage, error) {
	r, e := q.invokeQueue(ctx, &wire.QueueCommand{Kind: wire.QueueCommand_READ, FirstId: lastID, PageSize: int64(maxCount)})
	if e != nil {
		return nil, e
	}
	return queueMessages(r)
}
func (q *Queue) ReadMessagesFromDLQ(ctx context.Context, firstID, lastID int64, pageSize int, token []byte) ([]*p.QueueMessage, []byte, error) {
	r, e := q.invokeQueue(ctx, &wire.QueueCommand{Kind: wire.QueueCommand_READ_DLQ, FirstId: firstID, LastId: lastID, PageSize: int64(pageSize), NextPageToken: token})
	if e != nil {
		return nil, nil, e
	}
	m, e := queueMessages(r)
	return m, r.NextPageToken, e
}
func (q *Queue) DeleteMessagesBefore(ctx context.Context, id int64) error {
	_, e := q.invokeQueue(ctx, &wire.QueueCommand{Kind: wire.QueueCommand_DELETE_BEFORE, LastId: id})
	return e
}
func (q *Queue) DeleteMessageFromDLQ(ctx context.Context, id int64) error {
	_, e := q.invokeQueue(ctx, &wire.QueueCommand{Kind: wire.QueueCommand_DELETE_DLQ, LastId: id})
	return e
}
func (q *Queue) RangeDeleteMessagesFromDLQ(ctx context.Context, first, last int64) error {
	_, e := q.invokeQueue(ctx, &wire.QueueCommand{Kind: wire.QueueCommand_RANGE_DELETE_DLQ, FirstId: first, LastId: last})
	return e
}
func (q *Queue) updateAck(ctx context.Context, k wire.QueueCommand_Kind, m *p.InternalQueueMetadata) error {
	if m == nil {
		return serviceerror.NewInvalidArgument("nil queue metadata")
	}
	c, e := queueBlob(k, m.Blob)
	if e != nil {
		return e
	}
	c.Version = m.Version
	_, e = q.invokeQueue(ctx, c)
	return e
}
func (q *Queue) UpdateAckLevel(ctx context.Context, m *p.InternalQueueMetadata) error {
	return q.updateAck(ctx, wire.QueueCommand_UPDATE_ACK, m)
}
func (q *Queue) UpdateDLQAckLevel(ctx context.Context, m *p.InternalQueueMetadata) error {
	return q.updateAck(ctx, wire.QueueCommand_UPDATE_DLQ_ACK, m)
}
func (q *Queue) ack(ctx context.Context, k wire.QueueCommand_Kind) (*p.InternalQueueMetadata, error) {
	r, e := q.invokeQueue(ctx, &wire.QueueCommand{Kind: k})
	if e != nil {
		return nil, e
	}
	if r.Metadata == nil {
		return nil, serviceerror.NewInternal("missing queue metadata")
	}
	return &p.InternalQueueMetadata{Version: r.Metadata.Version, Blob: &commonpb.DataBlob{Data: r.Metadata.Data, EncodingType: enumspb.EncodingType(r.Metadata.Encoding)}}, nil
}
func (q *Queue) GetAckLevels(ctx context.Context) (*p.InternalQueueMetadata, error) {
	return q.ack(ctx, wire.QueueCommand_GET_ACK)
}
func (q *Queue) GetDLQAckLevels(ctx context.Context) (*p.InternalQueueMetadata, error) {
	return q.ack(ctx, wire.QueueCommand_GET_DLQ_ACK)
}
