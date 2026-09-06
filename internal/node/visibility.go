package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	qmodel "github.com/0x63616c/xenon/internal/query"
	vmodel "github.com/0x63616c/xenon/internal/visibility"
	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	native "slatedb.io/slatedb-go/uniffi"
	"sort"
	"strings"
)

type VisibilityServer struct {
	wire.UnimplementedVisibilityPersistenceServer
	Owner *Owner
}

func (s *VisibilityServer) Execute(ctx context.Context, q *wire.VisibilityRequest) (*wire.VisibilityResult, error) {
	if q == nil || q.ProtocolVersion != 1 || q.Partition != s.Owner.config.Partition || !operationID.MatchString(q.OperationId) || q.Command == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid history-task envelope")
	}
	q = proto.Clone(q).(*wire.VisibilityRequest)
	c := q.Command
	if e := validateVisibility(c); e != nil {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	if c.Kind <= wire.VisibilityCommand_GET {
		n, r := c.NamespaceId, c.RunId
		if c.Document != nil {
			n, r = c.Document.NamespaceId, c.Document.RunId
		}
		partition, e := vmodel.Partition(n, r)
		if e != nil || partition != s.Owner.config.Partition {
			return nil, status.Error(codes.InvalidArgument, "visibility document routed to wrong partition")
		}
	}

	b, e := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if e != nil {
		return nil, status.Error(codes.InvalidArgument, e.Error())
	}
	d := sha256.Sum256(b)
	if !bytes.Equal(d[:], q.CommandSha256) {
		return nil, status.Error(codes.InvalidArgument, "digest mismatch")
	}
	raw, e := s.Owner.Run(ctx, func(*native.Db) ([]byte, error) {
		out, e := s.Owner.journal(q.OperationId, d[:], visibilityFamily, func(tx *native.DbTransaction) (*wire.StoredOutcome, error) {
			r, e := applyVisibility(tx, c)
			if e != nil {
				return nil, e
			}
			return &wire.StoredOutcome{Result: &wire.StoredOutcome_VisibilityResult{VisibilityResult: r}}, nil
		})
		if e != nil {
			return nil, e
		}
		return proto.Marshal(out.GetVisibilityResult())
	})
	if e != nil {
		return nil, e
	}
	r := new(wire.VisibilityResult)
	if e = proto.Unmarshal(raw, r); e != nil {
		return nil, backend(e)
	}
	return r, nil
}

func validateVisibility(c *wire.VisibilityCommand) error {
	if c.Kind < wire.VisibilityCommand_START || c.Kind > wire.VisibilityCommand_GET_SCHEMA {
		return fmt.Errorf("invalid visibility operation")
	}
	if len(c.NextPageToken) > 4096 {
		return fmt.Errorf("oversized visibility cursor")
	}
	if c.Kind == wire.VisibilityCommand_LIST && (c.PageSize < 1 || c.PageSize > 1000) {
		return fmt.Errorf("visibility page size must be 1..1000")
	}
	if c.Kind <= wire.VisibilityCommand_UPSERT {
		d := c.Document
		if d == nil || d.Tombstone {
			return fmt.Errorf("invalid visibility document")
		}
		n, e := vmodel.CanonicalUUID(d.NamespaceId)
		if e != nil || n != d.NamespaceId {
			return fmt.Errorf("invalid namespace UUID")
		}
		r, e := vmodel.CanonicalUUID(d.RunId)
		if e != nil || r != d.RunId {
			return fmt.Errorf("invalid run UUID")
		}
		if d.StartTime == nil || d.ExecutionTime == nil {
			return fmt.Errorf("missing visibility timestamp")
		}
		for _, t := range []*wire.VisibilityTime{d.StartTime, d.ExecutionTime, d.CloseTime} {
			if t != nil && (t.Nanos < 0 || t.Nanos >= 1e9 || t.Nanos%1000 != 0) {
				return fmt.Errorf("invalid visibility timestamp")
			}
		}
		if c.Kind == wire.VisibilityCommand_CLOSE && d.CloseTime == nil {
			return fmt.Errorf("missing close timestamp")
		}
		for name, a := range d.Attributes {
			if len(name) > 256 {
				return fmt.Errorf("attribute name too long")
			}
			if e := vmodel.ValidateAttribute(a); e != nil {
				return fmt.Errorf("%s: %w", name, e)
			}
		}
		if proto.Size(d) > 2*1024*1024 {
			return fmt.Errorf("visibility document exceeds 2 MiB")
		}
	}
	if c.Kind == wire.VisibilityCommand_DELETE || c.Kind == wire.VisibilityCommand_GET {
		n, e := vmodel.CanonicalUUID(c.NamespaceId)
		if e != nil || n != c.NamespaceId {
			return fmt.Errorf("invalid namespace UUID")
		}
		r, e := vmodel.CanonicalUUID(c.RunId)
		if e != nil || r != c.RunId {
			return fmt.Errorf("invalid run UUID")
		}
	}
	if c.Kind == wire.VisibilityCommand_LIST || c.Kind == wire.VisibilityCommand_COUNT {
		if c.Query == nil || c.Query.FormatVersion != 1 || c.Query.PartitionFormat != 1 {
			return fmt.Errorf("invalid visibility query version")
		}
		if e := vmodel.ValidateExpression(c.Query.Predicate, 0); e != nil {
			return e
		}
		for _, column := range c.Query.GroupBy {
			if column == nil || column.ValueType < 1 || column.ValueType > 7 {
				return fmt.Errorf("invalid group column")
			}
		}
		n, e := vmodel.CanonicalUUID(c.Query.NamespaceId)
		if e != nil || n != c.Query.NamespaceId {
			return fmt.Errorf("invalid query namespace")
		}
	}
	return nil
}
func visibilityKey(n, r string) string      { return "v1/visibility/doc/" + n + "/" + r }
func visibilityOrderPrefix(n string) string { return "v1/visibility/order/" + n + "/" }
func visibilityOrder(d *wire.VisibilityDocument) string {
	return visibilityOrderPrefix(d.NamespaceId) + vmodel.SortKey(d)
}
func visibilityIndices(d *wire.VisibilityDocument) ([]string, error) {
	keys := []string{visibilityOrder(d)}
	for name, a := range d.Attributes {
		if a == nil || a.Missing {
			continue
		}
		for _, v := range a.Values {
			value := v
			if a.ValueType == int32(enumspb.INDEXED_VALUE_TYPE_DOUBLE) {
				normalized, e := vmodel.NormalizedScalar(&wire.VisibilityAttribute{ValueType: a.ValueType, Values: []*wire.QueryValue{v}})
				if e != nil {
					return nil, backend(e)
				}
				number, ok := normalized.(float64)
				if !ok {
					return nil, status.Error(codes.Unavailable, "invalid stored numeric index")
				}
				value = &wire.QueryValue{Scalar: &wire.QueryValue_DoubleValue{DoubleValue: number}}
			}
			raw, e := proto.MarshalOptions{Deterministic: true}.Marshal(value)
			if e != nil {
				return nil, backend(e)
			}
			keys = append(keys, fmt.Sprintf("v1/visibility/type/%s/%s/%d/%s/%s", d.NamespaceId, hex.EncodeToString([]byte(name)), a.ValueType, fmt.Sprintf("%x", sha256.Sum256(raw)), d.RunId))
		}
	}
	return keys, nil
}
func loadVisibility(tx *native.DbTransaction, n, r string) (*wire.VisibilityDocument, error) {
	raw, e := get(tx, visibilityKey(n, r))
	if e != nil || raw == nil {
		return nil, e
	}
	d := new(wire.VisibilityDocument)
	if e = proto.Unmarshal(raw, d); e != nil {
		return nil, backend(e)
	}
	if d.NamespaceId != n || d.RunId != r {
		return nil, status.Error(codes.Unavailable, "corrupt visibility identity")
	}
	return d, nil
}
func applyVisibility(tx *native.DbTransaction, c *wire.VisibilityCommand) (*wire.VisibilityResult, error) {
	result := new(wire.VisibilityResult)
	switch c.Kind {
	case wire.VisibilityCommand_START, wire.VisibilityCommand_CLOSE, wire.VisibilityCommand_UPSERT, wire.VisibilityCommand_DELETE:
		n, r := c.NamespaceId, c.RunId
		if c.Document != nil {
			n, r = c.Document.NamespaceId, c.Document.RunId
		}
		old, e := loadVisibility(tx, n, r)
		if e != nil {
			return nil, e
		}
		if c.Kind != wire.VisibilityCommand_DELETE && old != nil && (old.Tombstone || c.Kind == wire.VisibilityCommand_START || old.TaskId >= c.Document.TaskId) {
			return result, nil
		}
		if old != nil && !old.Tombstone {
			indices, e := visibilityIndices(old)
			if e != nil {
				return nil, e
			}
			for _, k := range indices {
				if e = tx.Delete([]byte(k)); e != nil {
					return nil, backend(e)
				}
			}
		}
		next := c.Document
		if c.Kind == wire.VisibilityCommand_DELETE {
			next = &wire.VisibilityDocument{NamespaceId: n, RunId: r, Tombstone: true}
		}
		if e = putHistory(tx, visibilityKey(n, r), next); e != nil {
			return nil, e
		}
		if !next.Tombstone {
			indices, e := visibilityIndices(next)
			if e != nil {
				return nil, e
			}
			for _, k := range indices {
				if e = put(tx, k, []byte(visibilityKey(n, r))); e != nil {
					return nil, e
				}
			}
		}
		return result, nil
	case wire.VisibilityCommand_GET:
		d, e := loadVisibility(tx, c.NamespaceId, c.RunId)
		if e != nil {
			return nil, e
		}
		if d == nil || d.Tombstone {
			return &wire.VisibilityResult{Error: wire.VisibilityResult_NOT_FOUND, Message: "visibility execution not found"}, nil
		}
		result.Documents = []*wire.VisibilityDocument{d}
		return result, nil
	case wire.VisibilityCommand_ADD_ATTRIBUTES, wire.VisibilityCommand_GET_SCHEMA:
		raw, e := get(tx, "v1/visibility/schema")
		if e != nil {
			return nil, e
		}
		if raw != nil {
			if e = proto.Unmarshal(raw, result); e != nil {
				return nil, backend(e)
			}
		}
		if proto.Size(result) > vmodel.ResponseBudget {
			return &wire.VisibilityResult{Error: wire.VisibilityResult_RESOURCE_EXHAUSTED, Message: "visibility schema exceeds response budget"}, nil
		}
		if c.Kind == wire.VisibilityCommand_GET_SCHEMA {
			return result, nil
		}
		if result.SearchAttributeTypes == nil {
			result.SearchAttributeTypes = map[string]int32{}
		}
		for name, typ := range c.SearchAttributeTypes {
			if name == "" || len(name) > 256 || typ < 1 || typ > 7 {
				return &wire.VisibilityResult{Error: wire.VisibilityResult_INVALID_ARGUMENT, Message: "invalid search attribute definition"}, nil
			}
			if old, ok := result.SearchAttributeTypes[name]; ok && old != typ {
				return &wire.VisibilityResult{Error: wire.VisibilityResult_INVALID_ARGUMENT, Message: "search attribute type is immutable"}, nil
			}
		}
		changed := false
		for name, typ := range c.SearchAttributeTypes {
			if _, ok := result.SearchAttributeTypes[name]; !ok {
				result.SearchAttributeTypes[name] = typ
				changed = true
			}
		}
		if changed {
			result.SchemaVersion++
			if proto.Size(result) > vmodel.ResponseBudget {
				return &wire.VisibilityResult{Error: wire.VisibilityResult_RESOURCE_EXHAUSTED, Message: "visibility schema exceeds response budget"}, nil
			}
			if e = putHistory(tx, "v1/visibility/schema", result); e != nil {
				return nil, e
			}
		}
		return result, nil
	case wire.VisibilityCommand_LIST, wire.VisibilityCommand_COUNT:
		return queryVisibility(tx, c)
	}
	return nil, status.Error(codes.InvalidArgument, "unknown visibility command")
}

type visibilityCursor struct {
	Version int
	Binding []byte
	Last    string
}

func queryVisibility(tx *native.DbTransaction, c *wire.VisibilityCommand) (*wire.VisibilityResult, error) {
	result := new(wire.VisibilityResult)
	digest, e := qmodel.Digest(c.Query)
	if e != nil {
		return nil, e
	}
	after := ""
	if len(c.NextPageToken) > 0 {
		var token visibilityCursor
		if json.Unmarshal(c.NextPageToken, &token) != nil || token.Version != 1 || token.Last == "" || !bytes.Equal(token.Binding, digest[:]) || len(token.Last) > 256 {
			return &wire.VisibilityResult{Error: wire.VisibilityResult_INVALID_ARGUMENT, Message: "visibility cursor does not match query"}, nil
		}
		after = token.Last
	}
	groups := map[string]*wire.VisibilityGroup{}
	prefix := visibilityOrderPrefix(c.Query.NamespaceId)
	last := ""
	e = visibilityScan(tx, prefix, after, func(k, v []byte) (bool, error) {
		key := strings.TrimPrefix(string(k), prefix)
		if key <= after {
			return false, nil
		}
		raw, e := get(tx, string(v))
		if e != nil {
			return false, e
		}
		if raw == nil {
			return false, status.Error(codes.Unavailable, "dangling visibility index")
		}
		d := new(wire.VisibilityDocument)
		if e = proto.Unmarshal(raw, d); e != nil {
			return false, backend(e)
		}
		if d.Tombstone || d.NamespaceId != c.Query.NamespaceId || visibilityOrder(d) != string(k) {
			return false, status.Error(codes.Unavailable, "corrupt visibility index")
		}
		match, e := vmodel.Match(d, c.Query.Predicate)
		if e != nil {
			return false, status.Error(codes.InvalidArgument, e.Error())
		}
		if !match {
			return false, nil
		}
		if c.Kind == wire.VisibilityCommand_COUNT {
			result.Count++
			if len(c.Query.GroupBy) > 0 {
				g := &wire.VisibilityGroup{}
				for _, col := range c.Query.GroupBy {
					a := vmodel.Field(d, col.Field)
					if a == nil {
						a = &wire.VisibilityAttribute{ValueType: col.ValueType, Missing: true}
					}
					g.Values = append(g.Values, a)
				}
				b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(g)
				key := string(b)
				if old := groups[key]; old != nil {
					old.Count++
				} else {
					g.Count = 1
					groups[key] = g
				}
			}
			return false, nil
		}
		result.Documents = append(result.Documents, d)
		result.NextPageToken, _ = json.Marshal(visibilityCursor{1, digest[:], key})
		if proto.Size(result) > vmodel.ResponseBudget {
			result.Documents = result.Documents[:len(result.Documents)-1]
			if last == "" {
				return false, status.Error(codes.ResourceExhausted, "visibility row exceeds response budget")
			}
			result.NextPageToken, _ = json.Marshal(visibilityCursor{1, digest[:], last})
			return true, nil
		}
		last = key
		return len(result.Documents) >= int(c.PageSize), nil
	})
	if e != nil {
		return nil, e
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		result.Groups = append(result.Groups, groups[k])
	}
	if proto.Size(result) > vmodel.ResponseBudget {
		return nil, status.Error(codes.ResourceExhausted, "visibility result exceeds response budget")
	}
	return result, nil
}

func visibilityScan(tx *native.DbTransaction, prefix, after string, visit func([]byte, []byte) (bool, error)) error {
	bounds := native.KeyRange{}
	if after != "" {
		// Prefix scans interpret bounds relative to the prefix (pinned SlateDB ops.rs).
		start := []byte(after)
		bounds.Start = &start
		bounds.StartInclusive = false
	}
	order := native.IterationOrderAscending
	it, e := tx.ScanPrefixWithOptions([]byte(prefix), bounds, native.ScanOptions{DurabilityFilter: native.DurabilityLevelRemote, ReadAheadBytes: 1 << 20, CacheBlocks: true, MaxFetchTasks: 1, Order: &order})
	if e != nil {
		return backend(e)
	}
	defer it.Destroy()
	for {
		row, e := it.Next()
		if e != nil {
			return backend(e)
		}
		if row == nil {
			return nil
		}
		stop, e := visit(row.Key, row.Value)
		if e != nil || stop {
			return e
		}
	}
}
