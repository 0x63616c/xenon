package persistence

import (
	"context"
	"encoding/hex"
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	p "go.temporal.io/server/common/persistence"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"math"
	"unicode/utf8"
)

func ValidateQueueV2Command(c *wire.QueueV2Command) error {
	if c == nil || c.Kind < wire.QueueV2Command_CREATE || c.Kind > wire.QueueV2Command_LIST || len(c.QueueName) > 1024 || !utf8.ValidString(c.QueueName) || len(c.Data) > 1024*1024 {
		return status.Error(codes.InvalidArgument, "invalid QueueV2 command")
	}
	return nil
}

// ApplyQueueV2 stages queue metadata and message changes in the caller's atomic
// transaction. The caller owns replay accounting, admission and durability.
func ApplyQueueV2(ctx context.Context, tx ClusterTransaction, c *wire.QueueV2Command) (*wire.StoredOutcome, error) {
	if tx == nil {
		return nil, status.Error(codes.InvalidArgument, "nil QueueV2 transaction")
	}
	if err := ValidateQueueV2Command(c); err != nil {
		return nil, err
	}
	r, err := applyQueueV2(ctx, tx, c)
	if err != nil {
		return nil, err
	}
	return &wire.StoredOutcome{Result: &wire.StoredOutcome_Queuev2Result{Queuev2Result: r}}, nil
}
func qv2Type(t int64) string { return fmt.Sprintf("v1/queuev2/%d/", t) }
func qv2StateKey(t int64, n string) string {
	return qv2Type(t) + "meta/" + hex.EncodeToString([]byte(n))
}
func qv2Messages(t int64, n string) string {
	return qv2Type(t) + "messages/" + hex.EncodeToString([]byte(n)) + "/"
}
func qv2MessageKey(t int64, n string, id int64) string {
	return fmt.Sprintf("%s%016x", qv2Messages(t, n), id)
}
func qv2Logical(code wire.QueueV2Result_Error, msg string) *wire.QueueV2Result {
	return &wire.QueueV2Result{Error: code, Message: msg}
}
func validQueueV2State(s *wire.QueueV2State) bool {
	return s.MinId >= 0 && s.LastId >= -1 && s.LastId < math.MaxInt64 && s.MinId <= s.LastId+1
}
func applyQueueV2(ctx context.Context, tx ClusterTransaction, c *wire.QueueV2Command) (*wire.QueueV2Result, error) {
	if c.Kind == wire.QueueV2Command_READ && c.PageSize <= 0 {
		return qv2Logical(wire.QueueV2Result_NONPOSITIVE_READ_SIZE, "non-positive read size"), nil
	}
	if c.Kind == wire.QueueV2Command_LIST && c.PageSize <= 0 {
		return qv2Logical(wire.QueueV2Result_NONPOSITIVE_LIST_SIZE, "non-positive list size"), nil
	}
	if c.Kind == wire.QueueV2Command_DELETE_RANGE && c.InclusiveMaxId < 0 {
		return qv2Logical(wire.QueueV2Result_INVALID_DELETE_ID, fmt.Sprintf("id is %d but must be >= 0", c.InclusiveMaxId)), nil
	}
	r := new(wire.QueueV2Result)
	if c.Kind == wire.QueueV2Command_LIST {
		offset, e := p.GetOffsetForListQueues(c.NextPageToken)
		if e != nil {
			return qv2Logical(wire.QueueV2Result_INVALID_LIST_TOKEN, e.Error()), nil
		}
		if offset < 0 {
			return qv2Logical(wire.QueueV2Result_NEGATIVE_OFFSET, "negative list offset"), nil
		}
		index := int64(0)
		e = scanCluster(ctx, tx, qv2Type(c.QueueType)+"meta/", func(key, value []byte) (bool, error) {
			if index < offset {
				index++
				return false, nil
			}
			s := new(wire.QueueV2State)
			if e := proto.Unmarshal(value, s); e != nil {
				return false, clusterEncodingError(e)
			}
			if !validQueueV2State(s) || string(key) != hex.EncodeToString([]byte(s.Name)) {
				return false, status.Error(codes.Unavailable, "corrupt QueueV2 metadata")
			}
			info := &wire.QueueV2Info{Name: s.Name, Count: s.LastId - s.MinId + 1, LastId: s.LastId}
			r.Queues = append(r.Queues, info)
			r.NextPageToken = p.GetNextPageTokenForListQueues(offset + int64(len(r.Queues)))
			if proto.Size(r) > 3*1024*1024 {
				r.Queues = r.Queues[:len(r.Queues)-1]
				if len(r.Queues) == 0 {
					return true, status.Error(codes.ResourceExhausted, "QueueV2 metadata exceeds page budget")
				}
				r.NextPageToken = p.GetNextPageTokenForListQueues(offset + int64(len(r.Queues)))
				return true, nil
			}
			return int64(len(r.Queues)) >= c.PageSize, nil
		})
		return r, e
	}
	s := new(wire.QueueV2State)
	exists, e := loadCluster(ctx, tx, qv2StateKey(c.QueueType, c.QueueName), s)
	if e != nil {
		return nil, e
	}
	if c.Kind == wire.QueueV2Command_CREATE {
		if exists {
			return qv2Logical(wire.QueueV2Result_ALREADY_EXISTS, fmt.Sprintf("queue type %d and name %s", c.QueueType, c.QueueName)), nil
		}
		s = &wire.QueueV2State{Name: c.QueueName, LastId: -1}
		return r, saveCluster(tx, qv2StateKey(c.QueueType, c.QueueName), s)
	}
	if !exists {
		return qv2Logical(wire.QueueV2Result_NOT_FOUND, fmt.Sprintf("queue not found: type = %d and name = %s", c.QueueType, c.QueueName)), nil
	}
	if !validQueueV2State(s) || s.Name != c.QueueName {
		return nil, status.Error(codes.Unavailable, "corrupt QueueV2 metadata")
	}
	switch c.Kind {
	case wire.QueueV2Command_ENQUEUE:
		if !c.HasBlob {
			return qv2Logical(wire.QueueV2Result_INVALID_ARGUMENT, "nil QueueV2 blob"), nil
		}
		if s.LastId >= math.MaxInt64-1 {
			return nil, status.Error(codes.ResourceExhausted, "QueueV2 ID space exhausted")
		}
		s.LastId++
		r.MessageId = s.LastId
		if e = saveCluster(tx, qv2MessageKey(c.QueueType, c.QueueName, s.LastId), &wire.QueueV2Entry{Id: s.LastId, Data: c.Data, Encoding: c.Encoding}); e != nil {
			return nil, e
		}
		e = saveCluster(tx, qv2StateKey(c.QueueType, c.QueueName), s)
	case wire.QueueV2Command_READ:
		// Decode the pinned token format, checking terminal ID before addition.
		minID := s.MinId
		if len(c.NextPageToken) > 0 {
			var token persistencespb.ReadQueueMessagesNextPageToken
			if e = token.Unmarshal(c.NextPageToken[1:]); e != nil {
				return qv2Logical(wire.QueueV2Result_INVALID_READ_TOKEN, e.Error()), nil
			}
			if token.LastReadMessageId == math.MaxInt64 {
				return r, nil
			}
			minID = token.LastReadMessageId + 1
		}
		minID = max(minID, s.MinId)
		e = scanCluster(ctx, tx, qv2Messages(c.QueueType, c.QueueName), func(key, value []byte) (bool, error) {
			m := new(wire.QueueV2Entry)
			if e := proto.Unmarshal(value, m); e != nil {
				return false, clusterEncodingError(e)
			}
			if m.Id < 0 || string(key) != fmt.Sprintf("%016x", m.Id) {
				return false, status.Error(codes.Unavailable, "corrupt QueueV2 entry")
			}
			if m.Id < minID {
				return false, nil
			}
			if _, ok := enumspb.EncodingType_name[m.Encoding]; !ok {
				r = qv2Logical(wire.QueueV2Result_UNKNOWN_ENCODING, enumspb.EncodingType(m.Encoding).String())
				return true, nil
			}
			r.Messages = append(r.Messages, m)
			r.NextPageToken = p.GetNextPageTokenForReadMessages([]p.QueueV2Message{{MetaData: p.MessageMetadata{ID: m.Id}}})
			if proto.Size(r) > 3*1024*1024 {
				r.Messages = r.Messages[:len(r.Messages)-1]
				if len(r.Messages) == 0 {
					return true, status.Error(codes.ResourceExhausted, "QueueV2 message exceeds page budget")
				}
				r.NextPageToken = p.GetNextPageTokenForReadMessages([]p.QueueV2Message{{MetaData: p.MessageMetadata{ID: r.Messages[len(r.Messages)-1].Id}}})
				return true, nil
			}
			return int64(len(r.Messages)) >= c.PageSize, nil
		})
		return r, e
	case wire.QueueV2Command_DELETE_RANGE:
		if s.LastId < 0 {
			return r, nil
		}
		dr, ok := p.GetDeleteRange(p.DeleteRequest{LastIDToDeleteInclusive: c.InclusiveMaxId, ExistingMessageRange: p.InclusiveMessageRange{MinMessageID: s.MinId, MaxMessageID: s.LastId}})
		if !ok {
			return r, nil
		}
		e = scanCluster(ctx, tx, qv2Messages(c.QueueType, c.QueueName), func(key, value []byte) (bool, error) {
			m := new(wire.QueueV2Entry)
			if e := proto.Unmarshal(value, m); e != nil {
				return false, clusterEncodingError(e)
			}
			if m.Id >= dr.MinMessageID && m.Id <= dr.MaxMessageID {
				return false, tx.Delete([]byte(qv2MessageKey(c.QueueType, c.QueueName, m.Id)))
			}
			return false, nil
		})
		if e != nil {
			return nil, e
		}
		s.MinId = dr.NewMinMessageID
		r.Deleted = dr.MessagesToDelete
		e = saveCluster(tx, qv2StateKey(c.QueueType, c.QueueName), s)
	}
	return r, e
}
