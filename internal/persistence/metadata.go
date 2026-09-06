package persistence

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func ValidateMetadataCommand(c *wire.MetadataCommand) error {
	if c == nil {
		return status.Error(codes.InvalidArgument, "invalid namespace command")
	}
	if c.Kind < wire.MetadataCommand_CREATE || c.Kind > wire.MetadataCommand_GET_METADATA || len(c.Data) > 1024*1024 || len(c.Name) > 1024 || len(c.PreviousName) > 1024 {
		return status.Error(codes.InvalidArgument, "invalid namespace command")
	}
	switch c.Kind {
	case wire.MetadataCommand_CREATE, wire.MetadataCommand_UPDATE, wire.MetadataCommand_RENAME, wire.MetadataCommand_DELETE:
		if len(c.Id) != 16 {
			return status.Error(codes.InvalidArgument, "namespace ID must be UUID bytes")
		}
	case wire.MetadataCommand_GET:
		if !((len(c.Id) == 16 && c.Name == "") || (len(c.Id) == 0 && c.Name != "")) {
			return status.Error(codes.InvalidArgument, "exactly one namespace ID or name required")
		}
	case wire.MetadataCommand_LIST:
		if c.PageSize < 1 || c.PageSize > 1000 || (len(c.NextPageToken) != 0 && len(c.NextPageToken) != 16) {
			return status.Error(codes.InvalidArgument, "invalid namespace page size/token")
		}
	}
	return nil
}

// ApplyNamespace preserves the existing ID/name indexes and notification-version
// protocol in one opaque transaction. The caller owns serialization, durable
// replay, accounting and AwaitDurable; PreviousName remains intentionally ignored.
func ApplyNamespace(ctx context.Context, tx ClusterTransaction, c *wire.MetadataCommand) (*wire.StoredOutcome, error) {
	if tx == nil {
		return nil, status.Error(codes.InvalidArgument, "nil namespace transaction")
	}
	if err := ValidateMetadataCommand(c); err != nil {
		return nil, err
	}
	result, err := applyNamespace(ctx, tx, c)
	if err != nil {
		return nil, err
	}
	return &wire.StoredOutcome{Result: &wire.StoredOutcome_MetadataResult{MetadataResult: result}}, nil
}

func namespaceIDKey(id []byte) string     { return "v1/namespace/id/" + string(id) }
func namespaceNameKey(name string) string { return "v1/namespace/name/" + name }
func namespaceRecord(ctx context.Context, tx ClusterTransaction, id []byte) (*wire.NamespaceRecord, error) {
	data, err := tx.Get(ctx, []byte(namespaceIDKey(id)))
	if err != nil || data == nil {
		return nil, err
	}
	record := &wire.NamespaceRecord{}
	if err = proto.Unmarshal(data, record); err != nil {
		return nil, clusterEncodingError(err)
	}
	return record, nil
}
func namespaceLogical(code wire.MetadataResult_Error, message string) *wire.MetadataResult {
	return &wire.MetadataResult{Error: code, Message: message}
}

func applyNamespace(ctx context.Context, tx ClusterTransaction, c *wire.MetadataCommand) (*wire.MetadataResult, error) {
	raw, err := tx.Get(ctx, []byte("v1/namespace/notification"))
	if err != nil {
		return nil, err
	}
	version := int64(1)
	if raw != nil {
		if len(raw) != 8 {
			return nil, status.Error(codes.Unavailable, "corrupt namespace notification version")
		}
		version = int64(binary.BigEndian.Uint64(raw))
	}
	result := &wire.MetadataResult{}
	switch c.Kind {
	case wire.MetadataCommand_GET_METADATA:
		result.NotificationVersion = version
	case wire.MetadataCommand_CREATE, wire.MetadataCommand_UPDATE, wire.MetadataCommand_RENAME:
		old, err := namespaceRecord(ctx, tx, c.Id)
		if err != nil {
			return nil, err
		}
		other, err := tx.Get(ctx, []byte(namespaceNameKey(c.Name)))
		if err != nil {
			return nil, err
		}
		if c.Kind == wire.MetadataCommand_CREATE {
			if old != nil || other != nil {
				return namespaceLogical(wire.MetadataResult_ALREADY_EXISTS, "name: "+c.Name), nil
			}
		} else {
			// Pinned SQL Rename uses ID and notification version, not PreviousName.
			if version != c.NotificationVersion {
				return namespaceLogical(wire.MetadataResult_CONDITION_FAILED, fmt.Sprintf("conditional update error: expect: %d, actual: %d", c.NotificationVersion, version)), nil
			}
			if old == nil {
				return namespaceLogical(wire.MetadataResult_UNAVAILABLE, "0 rows updated instead of one"), nil
			}
			if other != nil && !bytes.Equal(other, c.Id) {
				return namespaceLogical(wire.MetadataResult_UNAVAILABLE, "namespace rename violates unique name"), nil
			}
		}
		if version == math.MaxInt64 {
			return nil, status.Error(codes.ResourceExhausted, "namespace version exhausted")
		}
		item := &wire.NamespaceRecord{Id: c.Id, Name: c.Name, Data: c.Data, Encoding: c.Encoding, IsGlobal: c.IsGlobal, NotificationVersion: version}
		data, err := proto.Marshal(item)
		if err != nil {
			return nil, clusterEncodingError(err)
		}
		if old != nil && old.Name != c.Name {
			if err = tx.Delete([]byte(namespaceNameKey(old.Name))); err != nil {
				return nil, err
			}
		}
		if err = tx.Put([]byte(namespaceIDKey(c.Id)), data); err != nil {
			return nil, err
		}
		if err = tx.Put([]byte(namespaceNameKey(c.Name)), c.Id); err != nil {
			return nil, err
		}
		next := make([]byte, 8)
		binary.BigEndian.PutUint64(next, uint64(version+1))
		if err = tx.Put([]byte("v1/namespace/notification"), next); err != nil {
			return nil, err
		}
	case wire.MetadataCommand_GET, wire.MetadataCommand_DELETE, wire.MetadataCommand_DELETE_BY_NAME:
		id := c.Id
		if len(id) == 0 || c.Kind == wire.MetadataCommand_DELETE_BY_NAME {
			id, err = tx.Get(ctx, []byte(namespaceNameKey(c.Name)))
			if err != nil {
				return nil, err
			}
		}
		var item *wire.NamespaceRecord
		if id != nil {
			item, err = namespaceRecord(ctx, tx, id)
			if err != nil {
				return nil, err
			}
		}
		if c.Kind == wire.MetadataCommand_GET {
			if item == nil {
				name := c.Name
				if name == "" {
					u, _ := uuid.FromBytes(c.Id)
					name = u.String()
				}
				return namespaceLogical(wire.MetadataResult_NOT_FOUND, name), nil
			}
			result.Namespaces = []*wire.NamespaceRecord{item}
		} else if item != nil {
			if err = tx.Delete([]byte(namespaceIDKey(id))); err != nil {
				return nil, err
			}
			if err = tx.Delete([]byte(namespaceNameKey(item.Name))); err != nil {
				return nil, err
			}
		}
	case wire.MetadataCommand_LIST:
		truncated := false
		err := scanCluster(ctx, tx, "v1/namespace/id/", func(_ []byte, value []byte) (bool, error) {
			item := &wire.NamespaceRecord{}
			if err := proto.Unmarshal(value, item); err != nil {
				return false, clusterEncodingError(err)
			}
			if len(c.NextPageToken) > 0 && bytes.Compare(item.Id, c.NextPageToken) <= 0 {
				return false, nil
			}
			result.Namespaces = append(result.Namespaces, item)
			result.NextPageToken = item.Id
			if proto.Size(result) > 3*1024*1024 {
				result.Namespaces = result.Namespaces[:len(result.Namespaces)-1]
				if len(result.Namespaces) == 0 {
					return false, status.Error(codes.ResourceExhausted, "namespace record exceeds response budget")
				}
				truncated = true
				return true, nil
			}
			if len(result.Namespaces) == int(c.PageSize) {
				truncated = true
				return true, nil
			}
			return false, nil
		})
		if err != nil {
			return nil, err
		}
		if truncated {
			result.NextPageToken = result.Namespaces[len(result.Namespaces)-1].Id
		} else {
			result.NextPageToken = nil
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "unknown namespace operation")
	}
	return result, nil
}
