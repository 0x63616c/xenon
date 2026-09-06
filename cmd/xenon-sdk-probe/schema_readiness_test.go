package main

import (
	"context"
	"errors"
	"github.com/0x63616c/xenon/internal/query"
	"go.temporal.io/server/common/searchattribute"
	"reflect"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
)

type schemaStub struct {
	attributes        map[string]enumspb.IndexedValueType
	listErr, countErr error
	lists, counts     int
	ctx               context.Context
	query             string
	cancel            context.CancelFunc
}

func (s *schemaStub) ListSearchAttributes(ctx context.Context, q *operatorservice.ListSearchAttributesRequest, _ ...grpc.CallOption) (*operatorservice.ListSearchAttributesResponse, error) {
	s.lists++
	s.ctx = ctx
	if q.Namespace != "proof" {
		return nil, errors.New("wrong namespace")
	}
	return &operatorservice.ListSearchAttributesResponse{CustomAttributes: s.attributes}, s.listErr
}
func (s *schemaStub) CountWorkflowExecutions(ctx context.Context, q *workflowservice.CountWorkflowExecutionsRequest, _ ...grpc.CallOption) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	s.counts++
	s.query = q.Query
	if ctx != s.ctx || q.Namespace != "proof" {
		return nil, errors.New("context or namespace changed")
	}
	if s.cancel != nil {
		s.cancel()
	}
	return &workflowservice.CountWorkflowExecutionsResponse{Count: 0}, s.countErr
}
func TestSchemaReadinessRequiresExactWorkloadTypes(t *testing.T) {
	want := map[string]enumspb.IndexedValueType{"XenonProof": enumspb.INDEXED_VALUE_TYPE_KEYWORD, "OmesExecutionID": enumspb.INDEXED_VALUE_TYPE_KEYWORD, "KS_Keyword": enumspb.INDEXED_VALUE_TYPE_KEYWORD, "KS_Int": enumspb.INDEXED_VALUE_TYPE_INT}
	if !reflect.DeepEqual(requiredWorkloadSchema(), want) {
		t.Fatal("counted workload schema changed")
	}
	for name := range want {
		for _, bad := range []enumspb.IndexedValueType{enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED, enumspb.INDEXED_VALUE_TYPE_BOOL} {
			s := &schemaStub{attributes: requiredWorkloadSchema()}
			s.attributes[name] = bad
			if got, err := schemaReadiness(context.Background(), s, s, "proof"); err == nil || got != nil || s.counts != 0 {
				t.Fatal("incomplete/wrong schema admitted", name, got, err)
			}
		}
	}
}
func TestSchemaReadinessPendingPropagationUsesActualValidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	s := &schemaStub{attributes: map[string]enumspb.IndexedValueType{}}
	if _, err := schemaReadiness(ctx, s, s, "proof"); err == nil {
		t.Fatal("pending schema accepted")
	}
	s.attributes = requiredWorkloadSchema()
	s.countErr = errors.New("cached alias unavailable")
	if _, err := schemaReadiness(ctx, s, s, "proof"); !errors.Is(err, s.countErr) {
		t.Fatal("operator list alone admitted schema", err)
	}
	s.countErr = nil
	got, err := schemaReadiness(ctx, s, s, "proof")
	if err != nil || !got.Ready || s.lists != 3 || s.counts != 2 {
		t.Fatal(got, err)
	}
	const query = "XenonProof = 'xenon-schema-readiness' AND OmesExecutionID = 'xenon-schema-readiness' AND KS_Keyword = 'xenon-schema-readiness' AND KS_Int = 0"
	if s.query != query {
		t.Fatal("validation omitted required typed field", s.query)
	}
	if after, _ := s.ctx.Deadline(); !after.Equal(deadline) {
		t.Fatal("setup deadline changed")
	}
}
func TestSchemaReadinessNeverAcceptsCanceledObservation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &schemaStub{attributes: requiredWorkloadSchema(), cancel: cancel}
	if _, err := schemaReadiness(ctx, s, s, "proof"); !errors.Is(err, context.Canceled) {
		t.Fatal("late success accepted", err)
	}
	s = &schemaStub{}
	if _, err := schemaReadiness(ctx, s, s, "proof"); !errors.Is(err, context.Canceled) || s.lists != 0 {
		t.Fatal(err)
	}
}

func TestSchemaValidationQueryUsesProductionCompiler(t *testing.T) {
	attributes := requiredWorkloadSchema()
	cfg := query.Config{NamespaceID: "10000000-0000-0000-0000-000000000001", Types: searchattribute.NewNameTypeMap(attributes), SchemaVersion: 1}
	if _, err := query.Compile(schemaValidationQuery, cfg); err != nil {
		t.Fatal(err)
	}
	for name := range attributes {
		incomplete := requiredWorkloadSchema()
		delete(incomplete, name)
		cfg.Types = searchattribute.NewNameTypeMap(incomplete)
		if _, err := query.Compile(schemaValidationQuery, cfg); err == nil {
			t.Fatal("query did not validate field", name)
		}
	}
}
