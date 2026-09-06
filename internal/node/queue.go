package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"math"
	native "slatedb.io/slatedb-go/uniffi"
)

type QueueServer struct {
	wire.UnimplementedQueuePersistenceServer
	Owner *Owner
}

func (s *QueueServer) Execute(ctx context.Context, request *wire.QueueRequest) (*wire.QueueResult, error) {
	if request == nil || request.ProtocolVersion != 1 || request.Partition != s.Owner.config.Partition || !operationID.MatchString(request.OperationId) || request.Command == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid queue envelope")
	}
	request = proto.Clone(request).(*wire.QueueRequest)
	c := request.Command
	if c.Kind < wire.QueueCommand_INIT || c.Kind > wire.QueueCommand_GET_DLQ_ACK || c.QueueType <= 0 || len(c.Data) > 1024*1024 {
		return nil, status.Error(codes.InvalidArgument, "invalid queue command")
	}
	if (c.Kind == wire.QueueCommand_READ || c.Kind == wire.QueueCommand_READ_DLQ) && (c.PageSize < 1 || c.PageSize > 1000) {
		return nil, status.Error(codes.InvalidArgument, "queue page size must be 1..1000")
	}
	raw, e := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if e != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid command")
	}
	digest := sha256.Sum256(raw)
	if !bytes.Equal(digest[:], request.CommandSha256) {
		return nil, status.Error(codes.InvalidArgument, "command digest mismatch")
	}
	encoded, e := s.Owner.Run(ctx, func(*native.Db) ([]byte, error) {
		outcome, e := s.Owner.journal(request.OperationId, digest[:], queueFamily, func(tx *native.DbTransaction) (*wire.StoredOutcome, error) {
			r, e := applyQueue(tx, c)
			if e != nil {
				return nil, e
			}
			return &wire.StoredOutcome{Result: &wire.StoredOutcome_QueueResult{QueueResult: r}}, nil
		})
		if e != nil {
			return nil, e
		}
		return proto.Marshal(outcome.GetQueueResult())
	})
	if e != nil {
		return nil, e
	}
	r := new(wire.QueueResult)
	if e = proto.Unmarshal(encoded, r); e != nil {
		return nil, backend(e)
	}
	return r, nil
}
func queuePrefix(t int32) string { return fmt.Sprintf("v1/queue/%d/", t) }
func queueEntryKey(t int32, id int64) string {
	return fmt.Sprintf("%sm/%016x", queuePrefix(t), uint64(id))
}
func queueMetadata(tx *native.DbTransaction, t int32) (*wire.QueueMetadataRecord, error) {
	raw, e := get(tx, queuePrefix(t)+"meta")
	if e != nil || raw == nil {
		return nil, e
	}
	r := new(wire.QueueMetadataRecord)
	if e = proto.Unmarshal(raw, r); e != nil {
		return nil, backend(e)
	}
	return r, nil
}
func putQueueProto(tx *native.DbTransaction, k string, m proto.Message) error {
	raw, e := proto.Marshal(m)
	if e != nil {
		return backend(e)
	}
	return put(tx, k, raw)
}
func queueLogical(message string) *wire.QueueResult {
	return &wire.QueueResult{Error: wire.QueueResult_UNAVAILABLE, Message: message}
}
func scanQueue(tx *native.DbTransaction, t int32, visit func(*wire.QueueEntry) (bool, error)) error {
	iter, e := tx.ScanPrefix([]byte(queuePrefix(t)+"m/"), native.KeyRange{})
	if e != nil {
		return backend(e)
	}
	defer iter.Destroy()
	for {
		entry, e := iter.Next()
		if e != nil {
			return backend(e)
		}
		if entry == nil {
			return nil
		}
		m := new(wire.QueueEntry)
		if e = proto.Unmarshal(entry.Value, m); e != nil {
			return backend(e)
		}
		if m.Id < 0 || m.QueueType != t || string(entry.Key) != queueEntryKey(t, m.Id) {
			return status.Error(codes.Unavailable, "corrupt queue entry identity")
		}
		stop, e := visit(m)
		if e != nil || stop {
			return e
		}
	}
}
func applyQueue(tx *native.DbTransaction, c *wire.QueueCommand) (*wire.QueueResult, error) {
	t := c.QueueType
	if c.Kind >= wire.QueueCommand_ENQUEUE_DLQ {
		t = -t
	}
	r := new(wire.QueueResult)
	switch c.Kind {
	case wire.QueueCommand_INIT:
		for _, kind := range []int32{t, -t} {
			m, e := queueMetadata(tx, kind)
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
		raw, e := get(tx, queuePrefix(t)+"highwater")
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
		if e = put(tx, queuePrefix(t)+"highwater", next); e != nil {
			return nil, e
		}
		r.MessageId = id
	case wire.QueueCommand_UPDATE_ACK, wire.QueueCommand_UPDATE_DLQ_ACK:
		m, e := queueMetadata(tx, t)
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
		m, e := queueMetadata(tx, t)
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
		e := scanQueue(tx, t, func(m *wire.QueueEntry) (bool, error) {
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
		e := scanQueue(tx, t, func(m *wire.QueueEntry) (bool, error) {
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
				return false, backend(tx.Delete([]byte(queueEntryKey(t, m.Id))))
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
