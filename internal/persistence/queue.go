package persistence

import (
	"context"
	"encoding/binary"
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"math"
)

func ValidateQueueCommand(c *wire.QueueCommand) error {
	if c == nil || c.Kind < wire.QueueCommand_INIT || c.Kind > wire.QueueCommand_GET_DLQ_ACK || c.QueueType <= 0 || len(c.Data) > 1024*1024 {
		return status.Error(codes.InvalidArgument, "invalid queue command")
	}
	if (c.Kind == wire.QueueCommand_READ || c.Kind == wire.QueueCommand_READ_DLQ) && (c.PageSize < 1 || c.PageSize > 1000) {
		return status.Error(codes.InvalidArgument, "queue page size must be 1..1000")
	}
	return nil
}

// ApplyQueue preserves queue/DLQ allocation, range deletion and acknowledgement
// version semantics. Its caller owns admission, replay and native durability.
func ApplyQueue(ctx context.Context, tx ClusterTransaction, c *wire.QueueCommand) (*wire.StoredOutcome, error) {
	if tx == nil {
		return nil, status.Error(codes.InvalidArgument, "nil queue transaction")
	}
	if err := ValidateQueueCommand(c); err != nil {
		return nil, err
	}
	r, err := applyQueue(ctx, tx, c)
	if err != nil {
		return nil, err
	}
	return &wire.StoredOutcome{Result: &wire.StoredOutcome_QueueResult{QueueResult: r}}, nil
}
func queuePrefix(t int32) string { return fmt.Sprintf("v1/queue/%d/", t) }
func queueEntryKey(t int32, id int64) string {
	return fmt.Sprintf("%sm/%016x", queuePrefix(t), uint64(id))
}
func queueMetadata(ctx context.Context, tx ClusterTransaction, t int32) (*wire.QueueMetadataRecord, error) {
	raw, e := tx.Get(ctx, []byte(queuePrefix(t)+"meta"))
	if e != nil || raw == nil {
		return nil, e
	}
	r := new(wire.QueueMetadataRecord)
	if e = proto.Unmarshal(raw, r); e != nil {
		return nil, clusterEncodingError(e)
	}
	return r, nil
}
func putQueueProto(tx ClusterTransaction, k string, m proto.Message) error {
	raw, e := proto.Marshal(m)
	if e != nil {
		return clusterEncodingError(e)
	}
	return tx.Put([]byte(k), raw)
}
func queueLogical(message string) *wire.QueueResult {
	return &wire.QueueResult{Error: wire.QueueResult_UNAVAILABLE, Message: message}
}
func scanQueue(ctx context.Context, tx ClusterTransaction, t int32, visit func(*wire.QueueEntry) (bool, error)) error {
	return scanCluster(ctx, tx, queuePrefix(t)+"m/", func(key, value []byte) (bool, error) {
		m := new(wire.QueueEntry)
		if err := proto.Unmarshal(value, m); err != nil {
			return false, clusterEncodingError(err)
		}
		if m.Id < 0 || m.QueueType != t || queuePrefix(t)+"m/"+string(key) != queueEntryKey(t, m.Id) {
			return false, status.Error(codes.Unavailable, "corrupt queue entry identity")
		}
		return visit(m)
	})
}
func applyQueue(ctx context.Context, tx ClusterTransaction, c *wire.QueueCommand) (*wire.QueueResult, error) {
	t := c.QueueType
	if c.Kind >= wire.QueueCommand_ENQUEUE_DLQ {
		t = -t
	}
	r := new(wire.QueueResult)
	switch c.Kind {
	case wire.QueueCommand_INIT:
		for _, kind := range []int32{t, -t} {
			m, e := queueMetadata(ctx, tx, kind)
			if e != nil {
				return nil, e
			}
			if m == nil {
				if e = putQueueProto(tx, queuePrefix(kind)+"meta", &wire.QueueMetadataRecord{Data: c.Data, Encoding: c.Encoding}); e != nil {
					return nil, e
				}
			}
		}
	case wire.QueueCommand_ENQUEUE, wire.QueueCommand_ENQUEUE_DLQ:
		// Delegated #30 correction: allocation survives deletion and returns the actual inserted ID.
		raw, e := tx.Get(ctx, []byte(queuePrefix(t)+"highwater"))
		if e != nil {
			return nil, e
		}
		last := int64(-1)
		if raw != nil {
			if len(raw) != 8 {
				return nil, status.Error(codes.Unavailable, "corrupt queue highwater")
			}
			last = int64(binary.BigEndian.Uint64(raw))
			if last < 0 {
				return nil, status.Error(codes.Unavailable, "negative queue highwater")
			}
		}
		if last == math.MaxInt64 {
			return nil, status.Error(codes.ResourceExhausted, "queue message IDs exhausted")
		}
		id := last + 1
		if e = putQueueProto(tx, queueEntryKey(t, id), &wire.QueueEntry{QueueType: t, Id: id, Data: c.Data, Encoding: enumspb.EncodingType(c.Encoding).String()}); e != nil {
			return nil, e
		}
		next := make([]byte, 8)
		binary.BigEndian.PutUint64(next, uint64(id))
		if e = tx.Put([]byte(queuePrefix(t)+"highwater"), next); e != nil {
			return nil, e
		}
		r.MessageId = id
	case wire.QueueCommand_UPDATE_ACK, wire.QueueCommand_UPDATE_DLQ_ACK:
		m, e := queueMetadata(ctx, tx, t)
		if e != nil {
			return nil, e
		}
		if m == nil || m.Version != c.Version {
			return queueLogical("queue metadata version mismatch"), nil
		}
		if c.Version == math.MaxInt64 {
			return nil, status.Error(codes.ResourceExhausted, "queue metadata version exhausted")
		}
		if e = putQueueProto(tx, queuePrefix(t)+"meta", &wire.QueueMetadataRecord{Data: c.Data, Encoding: c.Encoding, Version: c.Version + 1}); e != nil {
			return nil, e
		}
	case wire.QueueCommand_GET_ACK, wire.QueueCommand_GET_DLQ_ACK:
		m, e := queueMetadata(ctx, tx, t)
		if e != nil {
			return nil, e
		}
		if m == nil {
			return queueLogical("queue metadata not initialized"), nil
		}
		r.Metadata = m
	case wire.QueueCommand_READ, wire.QueueCommand_READ_DLQ:
		first, last := c.FirstId, c.LastId
		if c.Kind == wire.QueueCommand_READ {
			last = math.MaxInt64
		}
		if c.Kind == wire.QueueCommand_READ_DLQ && len(c.NextPageToken) != 0 {
			if len(c.NextPageToken) != 8 {
				return &wire.QueueResult{Error: wire.QueueResult_INTERNAL, Message: "invalid next page token"}, nil
			}
			first = int64(binary.LittleEndian.Uint64(c.NextPageToken))
		}
		truncated := false
		e := scanQueue(ctx, tx, t, func(m *wire.QueueEntry) (bool, error) {
			if m.Id <= first || m.Id > last {
				return false, nil
			}
			r.Messages = append(r.Messages, m)
			if c.Kind == wire.QueueCommand_READ_DLQ {
				r.NextPageToken = make([]byte, 8)
				binary.LittleEndian.PutUint64(r.NextPageToken, uint64(m.Id))
			}
			if proto.Size(r) > 3*1024*1024 {
				r.Messages = r.Messages[:len(r.Messages)-1]
				if len(r.Messages) == 0 {
					return true, status.Error(codes.ResourceExhausted, "queue record exceeds response budget")
				}
				truncated = true
				return true, nil
			}
			if int64(len(r.Messages)) == c.PageSize {
				truncated = true
				return true, nil
			}
			return false, nil
		})
		if e != nil {
			return nil, e
		}
		if truncated && c.Kind == wire.QueueCommand_READ_DLQ {
			binary.LittleEndian.PutUint64(r.NextPageToken, uint64(r.Messages[len(r.Messages)-1].Id))
		} else {
			r.NextPageToken = nil
		}
	case wire.QueueCommand_DELETE_BEFORE, wire.QueueCommand_DELETE_DLQ, wire.QueueCommand_RANGE_DELETE_DLQ:
		e := scanQueue(ctx, tx, t, func(m *wire.QueueEntry) (bool, error) {
			remove := false
			switch c.Kind {
			case wire.QueueCommand_DELETE_BEFORE:
				remove = m.Id < c.LastId
			case wire.QueueCommand_DELETE_DLQ:
				remove = m.Id == c.LastId
			case wire.QueueCommand_RANGE_DELETE_DLQ:
				remove = m.Id > c.FirstId && m.Id <= c.LastId
			}
			if remove {
				return false, tx.Delete([]byte(queueEntryKey(t, m.Id)))
			}
			return false, nil
		})
		if e != nil {
			return nil, e
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "unknown queue mutation")
	}
	return r, nil
}
