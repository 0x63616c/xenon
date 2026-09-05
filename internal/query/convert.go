// Package query translates the pinned Temporal query grammar into Xenon's typed
// protobuf contract. It does not evaluate predicates or supply a visibility store.
package query

import (
	"crypto/sha256"
	"fmt"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/google/uuid"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	upstream "go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/protobuf/proto"
)

type Config struct {
	NamespaceID   string
	NamespaceName namespace.Name
	Types         searchattribute.NameTypeMap
	Mapper        searchattribute.Mapper
	ChasmMapper   *chasm.VisibilitySearchAttributesMapper
	ArchetypeID   chasm.ArchetypeID
	SchemaVersion uint64
}

// Compile retains upstream validation and its ordinary/CHASM division predicate.
// Namespace binding is explicit in the outer message and must be enforced by
// every storage operation; a division predicate is not an isolation boundary.
func Compile(text string, cfg Config) (*wire.VisibilityQuery, error) {
	var id uuid.UUID
	var err error
	canonicalNamespace := ""
	if cfg.NamespaceID != "" {
		id, err = uuid.Parse(cfg.NamespaceID)
		canonicalNamespace = id.String()
	}
	if err != nil {
		return nil, fmt.Errorf("namespace ID: %w", err)
	}
	// The pinned converter dereferences its mapper for custom attributes.
	// Identity mapping is safe when aliases are absent; type-map validation still applies.
	if cfg.Mapper == nil {
		cfg.Mapper = identityMapper{}
	}
	c := upstream.NewQueryConverter[*wire.QueryExpr](converter{}, cfg.NamespaceName, cfg.Types, cfg.Mapper).
		WithChasmMapper(cfg.ChasmMapper).WithArchetypeID(cfg.ArchetypeID)
	p, err := c.Convert(text)
	if err != nil {
		return nil, err
	}
	if len(p.OrderBy) > 0 {
		return nil, upstream.NewConverterError("ORDER BY is unsupported by the selected SQL visibility policy")
	}
	result := &wire.VisibilityQuery{FormatVersion: 1, NamespaceId: canonicalNamespace, Predicate: p.QueryExpr, SchemaVersion: cfg.SchemaVersion, PartitionFormat: 1}
	for _, col := range p.GroupBy {
		result.GroupBy = append(result.GroupBy, column(col))
	}
	return result, nil
}

// Digest binds pagination to the complete typed expression and schema context.
func Digest(q *wire.VisibilityQuery) ([32]byte, error) {
	b, err := (proto.MarshalOptions{Deterministic: true}).Marshal(q)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(b), nil
}

type converter struct{}

var _ upstream.StoreQueryConverter[*wire.QueryExpr] = converter{}

func (converter) GetDatetimeFormat() string                                 { return "2006-01-02 15:04:05.999999" }
func (converter) BuildParenExpr(e *wire.QueryExpr) (*wire.QueryExpr, error) { return e, nil }
func (converter) BuildNotExpr(e *wire.QueryExpr) (*wire.QueryExpr, error) {
	return &wire.QueryExpr{Operator: "not", Children: []*wire.QueryExpr{e}}, nil
}
func (converter) BuildAndExpr(es ...*wire.QueryExpr) (*wire.QueryExpr, error) {
	return logical("and", es), nil
}
func (converter) BuildOrExpr(es ...*wire.QueryExpr) (*wire.QueryExpr, error) {
	return logical("or", es), nil
}
func logical(op string, es []*wire.QueryExpr) *wire.QueryExpr {
	var children []*wire.QueryExpr
	for _, e := range es {
		if e != nil {
			children = append(children, e)
		}
	}
	if len(children) == 0 {
		return nil
	}
	if len(children) == 1 {
		return children[0]
	}
	return &wire.QueryExpr{Operator: op, Children: children}
}
func column(c *upstream.SAColumn) *wire.QueryColumn {
	return &wire.QueryColumn{Field: c.FieldName, Alias: c.Alias, ValueType: int32(c.ValueType)}
}
func scalar(v any) (*wire.QueryValue, error) {
	switch x := v.(type) {
	case string:
		return &wire.QueryValue{Scalar: &wire.QueryValue_StringValue{StringValue: x}}, nil
	case int64:
		return &wire.QueryValue{Scalar: &wire.QueryValue_IntValue{IntValue: x}}, nil
	case float64:
		return &wire.QueryValue{Scalar: &wire.QueryValue_DoubleValue{DoubleValue: x}}, nil
	case bool:
		return &wire.QueryValue{Scalar: &wire.QueryValue_BoolValue{BoolValue: x}}, nil
	default:
		return nil, fmt.Errorf("unsupported pinned query scalar %T", v)
	}
}
func expression(op string, c *upstream.SAColumn, vs ...any) (*wire.QueryExpr, error) {
	e := &wire.QueryExpr{Operator: op, Column: column(c)}
	for _, v := range vs {
		values, ok := v.([]any)
		if !ok {
			values = []any{v}
		}
		for _, item := range values {
			s, err := scalar(item)
			if err != nil {
				return nil, err
			}
			e.Values = append(e.Values, s)
		}
	}
	return e, nil
}
func (converter) ConvertComparisonExpr(op string, c *upstream.SAColumn, v any) (*wire.QueryExpr, error) {
	return expression(op, c, v)
}
func (converter) ConvertKeywordComparisonExpr(op string, c *upstream.SAColumn, v any) (*wire.QueryExpr, error) {
	return expression(op, c, v)
}
func (converter) ConvertKeywordListComparisonExpr(op string, c *upstream.SAColumn, v any) (*wire.QueryExpr, error) {
	return expression(op, c, v)
}
func (converter) ConvertTextComparisonExpr(op string, c *upstream.SAColumn, v any) (*wire.QueryExpr, error) {
	text, ok := v.(string)
	if !ok || len(upstream.TokenizeTextQueryString(text)) == 0 {
		return nil, upstream.NewConverterError("Text comparison requires at least one token")
	}
	return expression(op, c, v)
}
func (converter) ConvertRangeExpr(op string, c *upstream.SAColumn, a, b any) (*wire.QueryExpr, error) {
	return expression(op, c, a, b)
}
func (converter) ConvertIsExpr(op string, c *upstream.SAColumn) (*wire.QueryExpr, error) {
	return expression(op, c)
}

type identityMapper struct{}

func (identityMapper) GetAlias(name, _ string) (string, error)     { return name, nil }
func (identityMapper) GetFieldName(name, _ string) (string, error) { return name, nil }
