package main

import (
	"context"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
)

// This is the schema used by the counted SDK and Omes workload, not an optional
// list learned from whatever one frontend currently advertises.
func requiredWorkloadSchema() map[string]enumspb.IndexedValueType {
	return map[string]enumspb.IndexedValueType{"XenonProof": enumspb.INDEXED_VALUE_TYPE_KEYWORD, "OmesExecutionID": enumspb.INDEXED_VALUE_TYPE_KEYWORD, "KS_Keyword": enumspb.INDEXED_VALUE_TYPE_KEYWORD, "KS_Int": enumspb.INDEXED_VALUE_TYPE_INT}
}

const schemaValidationQuery = "XenonProof = 'xenon-schema-readiness' AND OmesExecutionID = 'xenon-schema-readiness' AND KS_Keyword = 'xenon-schema-readiness' AND KS_Int = 0"

type schemaOperator interface {
	ListSearchAttributes(context.Context, *operatorservice.ListSearchAttributesRequest, ...grpc.CallOption) (*operatorservice.ListSearchAttributesResponse, error)
}
type schemaWorkflow interface {
	CountWorkflowExecutions(context.Context, *workflowservice.CountWorkflowExecutionsRequest, ...grpc.CallOption) (*workflowservice.CountWorkflowExecutionsResponse, error)
}
type schemaObservation struct {
	Ready      bool                                `json:"ready"`
	Namespace  string                              `json:"namespace"`
	Attributes map[string]enumspb.IndexedValueType `json:"attributes"`
	Query      string                              `json:"query"`
	Count      int64                               `json:"count"`
}

// Each call observes one explicitly addressed frontend. The operator read forces
// its cluster schema cache refresh. Count then exercises the actual visibility
// compiler's provider and namespace mapper, without creating workflow state.
// Separate history/worker caches and future availability are not proven here.
func schemaReadiness(ctx context.Context, op schemaOperator, wf schemaWorkflow, namespace string) (*schemaObservation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	schema, err := op.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{Namespace: namespace})
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if schema == nil {
		return nil, fmt.Errorf("schema readiness: missing operator response")
	}
	for name, want := range requiredWorkloadSchema() {
		if got := schema.CustomAttributes[name]; got != want {
			return nil, fmt.Errorf("schema readiness pending: %s has type %s, requires %s", name, got, want)
		}
	}
	count, err := wf.CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{Namespace: namespace, Query: schemaValidationQuery})
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if count == nil || count.Count < 0 {
		return nil, fmt.Errorf("schema readiness: invalid count response")
	}
	return &schemaObservation{true, namespace, schema.CustomAttributes, schemaValidationQuery, count.Count}, nil
}
