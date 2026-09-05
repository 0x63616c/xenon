package adapter

import (
	"context"
	"crypto/sha256"
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/rpctrace"
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"time"
)

type QueueV2 struct {
	connection *grpc.ClientConn
	client     wire.QueueV2PersistenceClient
	partition  string
}

var _ p.QueueV2 = (*QueueV2)(nil)

func NewQueueV2(address, partition string) (*QueueV2, error) {
	c, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithChainUnaryInterceptor(rpctrace.Unary))
	if e != nil {
		return nil, e
	}
	return &QueueV2{connection: c, client: wire.NewQueueV2PersistenceClient(c), partition: partition}, nil
}
func (q *QueueV2) Close() {
	if q.connection != nil {
		_ = q.connection.Close()
	}
}
func qv2Error(r *wire.QueueV2Result) error {
	switch r.Error {
	case wire.QueueV2Result_NONE:
		return nil
	case wire.QueueV2Result_NOT_FOUND:
		return serviceerror.NewNotFound(r.Message)
	case wire.QueueV2Result_ALREADY_EXISTS:
		return fmt.Errorf("%w: %s", p.ErrQueueAlreadyExists, r.Message)
	case wire.QueueV2Result_INVALID_READ_TOKEN:
		return fmt.Errorf("%w: %s", p.ErrInvalidReadQueueMessagesNextPageToken, r.Message)
	case wire.QueueV2Result_INVALID_LIST_TOKEN:
		return fmt.Errorf("%w: %s", p.ErrInvalidListQueuesNextPageToken, r.Message)
	case wire.QueueV2Result_NONPOSITIVE_READ_SIZE:
		return p.ErrNonPositiveReadQueueMessagesPageSize
	case wire.QueueV2Result_NONPOSITIVE_LIST_SIZE:
		return p.ErrNonPositiveListQueuesPageSize
	case wire.QueueV2Result_NEGATIVE_OFFSET:
		return p.ErrNegativeListQueuesOffset
	case wire.QueueV2Result_INVALID_DELETE_ID:
		return fmt.Errorf("%w: %s", p.ErrInvalidQueueRangeDeleteMaxMessageID, r.Message)
	case wire.QueueV2Result_UNKNOWN_ENCODING:
		return serialization.NewUnknownEncodingTypeError(r.Message)
	case wire.QueueV2Result_INVALID_ARGUMENT:
		return serviceerror.NewInvalidArgument(r.Message)
	default:
		return serviceerror.NewInternal("unknown QueueV2 result")
	}
}
func (q *QueueV2) invoke(ctx context.Context, c *wire.QueueV2Command) (traceResult *wire.QueueV2Result, traceErr error) {
	ctx, traceFinish, traceBeginErr := rpctrace.BeginObserved(ctx, "queuev2")
	if traceBeginErr != nil {
		return nil, traceBeginErr
	}
	defer func() { traceErr = traceFinish(traceErr) }()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	b, e := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if e != nil {
		return nil, e
	}
	h := sha256.Sum256(b)
	req := &wire.QueueV2Request{ProtocolVersion: 1, Partition: q.partition, OperationId: uuid.NewString(), CommandSha256: h[:], Command: c}
	for i := 0; i < 3; i++ {
		r, e := q.client.Execute(ctx, req)
		if e == nil {
			if r == nil {
				return nil, serviceerror.NewInternal("nil QueueV2 result")
			}
			return r, qv2Error(r)
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		switch status.Code(e) {
		case codes.Canceled:
			return nil, context.Canceled
		case codes.DeadlineExceeded:
			return nil, context.DeadlineExceeded
		}
		if status.Code(e) != codes.Unavailable || i == 2 {
			return nil, serviceerror.FromStatus(status.Convert(e))
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(i+1) * 20 * time.Millisecond):
		}
	}
	panic("unreachable")
}
func (q *QueueV2) CreateQueue(ctx context.Context, r *p.InternalCreateQueueRequest) (*p.InternalCreateQueueResponse, error) {
	if r == nil {
		return nil, serviceerror.NewInvalidArgument("nil queue request")
	}
	_, e := q.invoke(ctx, &wire.QueueV2Command{Kind: wire.QueueV2Command_CREATE, QueueType: int64(r.QueueType), QueueName: r.QueueName})
	if e != nil {
		return nil, e
	}
	return &p.InternalCreateQueueResponse{}, nil
}
func (q *QueueV2) EnqueueMessage(ctx context.Context, r *p.InternalEnqueueMessageRequest) (*p.InternalEnqueueMessageResponse, error) {
	if r == nil {
		return nil, serviceerror.NewInvalidArgument("nil queue request")
	}
	c := &wire.QueueV2Command{Kind: wire.QueueV2Command_ENQUEUE, QueueType: int64(r.QueueType), QueueName: r.QueueName}
	if r.Blob != nil {
		c.HasBlob = true
		c.Data = r.Blob.Data
		c.Encoding = int32(r.Blob.EncodingType)
	}
	v, e := q.invoke(ctx, c)
	if e != nil {
		return nil, e
	}
	return &p.InternalEnqueueMessageResponse{Metadata: p.MessageMetadata{ID: v.MessageId}}, nil
}
func (q *QueueV2) ReadMessages(ctx context.Context, r *p.InternalReadMessagesRequest) (*p.InternalReadMessagesResponse, error) {
	if r == nil {
		return nil, serviceerror.NewInvalidArgument("nil queue request")
	}
	v, e := q.invoke(ctx, &wire.QueueV2Command{Kind: wire.QueueV2Command_READ, QueueType: int64(r.QueueType), QueueName: r.QueueName, PageSize: int64(r.PageSize), NextPageToken: r.NextPageToken})
	if e != nil {
		return nil, e
	}
	out := &p.InternalReadMessagesResponse{NextPageToken: v.NextPageToken}
	for _, m := range v.Messages {
		if m == nil {
			return nil, serviceerror.NewInternal("nil queue message")
		}
		out.Messages = append(out.Messages, p.QueueV2Message{MetaData: p.MessageMetadata{ID: m.Id}, Data: &commonpb.DataBlob{Data: m.Data, EncodingType: enumspb.EncodingType(m.Encoding)}})
	}
	return out, nil
}
func (q *QueueV2) RangeDeleteMessages(ctx context.Context, r *p.InternalRangeDeleteMessagesRequest) (*p.InternalRangeDeleteMessagesResponse, error) {
	if r == nil {
		return nil, serviceerror.NewInvalidArgument("nil queue request")
	}
	v, e := q.invoke(ctx, &wire.QueueV2Command{Kind: wire.QueueV2Command_DELETE_RANGE, QueueType: int64(r.QueueType), QueueName: r.QueueName, InclusiveMaxId: r.InclusiveMaxMessageMetadata.ID})
	if e != nil {
		return nil, e
	}
	return &p.InternalRangeDeleteMessagesResponse{MessagesDeleted: v.Deleted}, nil
}
func (q *QueueV2) ListQueues(ctx context.Context, r *p.InternalListQueuesRequest) (*p.InternalListQueuesResponse, error) {
	if r == nil {
		return nil, serviceerror.NewInvalidArgument("nil queue request")
	}
	v, e := q.invoke(ctx, &wire.QueueV2Command{Kind: wire.QueueV2Command_LIST, QueueType: int64(r.QueueType), PageSize: int64(r.PageSize), NextPageToken: r.NextPageToken})
	if e != nil {
		return nil, e
	}
	out := &p.InternalListQueuesResponse{NextPageToken: v.NextPageToken}
	for _, i := range v.Queues {
		if i == nil {
			return nil, serviceerror.NewInternal("nil queue info")
		}
		out.Queues = append(out.Queues, p.QueueInfo{QueueName: i.Name, MessageCount: i.Count, LastMessageID: i.LastId})
	}
	return out, nil
}
