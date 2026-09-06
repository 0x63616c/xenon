package adapter

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	qmodel "github.com/0x63616c/xenon/internal/query"
	"github.com/0x63616c/xenon/internal/rpctrace"
	vmodel "github.com/0x63616c/xenon/internal/visibility"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"sort"
	"sync"
	"time"
)

type VisibilityStore struct {
	operations             *operationIDs
	connection             *grpc.ClientConn
	client                 wire.VisibilityPersistenceClient
	index, schemaPartition string
	provider               searchattribute.Provider
	mappers                searchattribute.MapperProvider
	chasm                  *chasm.Registry
}

var _ store.VisibilityStore = (*VisibilityStore)(nil)

func NewVisibilityStore(address, index, schemaPartition string, provider searchattribute.Provider, mappers searchattribute.MapperProvider, registry *chasm.Registry, options ...StoreOption) (*VisibilityStore, error) {
	if address == "" || index == "" || schemaPartition == "" || provider == nil {
		return nil, fmt.Errorf("visibility address, index, schema partition and type provider are required")
	}
	c, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithChainUnaryInterceptor(rpctrace.Unary))
	if e != nil {
		return nil, e
	}
	return &VisibilityStore{newOperationIDs(options...), c, wire.NewVisibilityPersistenceClient(c), index, schemaPartition, provider, mappers, registry}, nil
}
func (s *VisibilityStore) Close() {
	if s.connection != nil {
		_ = s.connection.Close()
	}
}
func (s *VisibilityStore) GetName() string      { return "xenon" }
func (s *VisibilityStore) GetIndexName() string { return s.index }
func (s *VisibilityStore) invokeVisibility(ctx context.Context, partition string, c *wire.VisibilityCommand) (traceResult *wire.VisibilityResult, traceErr error) {
	ctx, traceFinish, traceBeginErr := rpctrace.BeginObserved(ctx, "visibility")
	if traceBeginErr != nil {
		return nil, traceBeginErr
	}
	defer func() { traceErr = traceFinish(traceErr) }()
	if _, bounded := ctx.Deadline(); !bounded {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	raw, e := proto.MarshalOptions{Deterministic: true}.Marshal(c)
	if e != nil {
		return nil, e
	}
	digest := sha256.Sum256(raw)
	operation, operationErr := s.operations.next()
	if operationErr != nil {
		return nil, operationErr
	}
	q := &wire.VisibilityRequest{ProtocolVersion: 1, Partition: partition, OperationId: operation, CommandSha256: digest[:], Command: c}
	r, e := retryOperation(ctx, func(ctx context.Context) (*wire.VisibilityResult, error) { return s.client.Execute(ctx, q) })
	if e != nil {
		return nil, e
	}

	if r == nil {
		return nil, serviceerror.NewInternal("nil visibility response")
	}
	return r, visibilityError(r)

}
func visibilityError(r *wire.VisibilityResult) error {
	switch r.Error {
	case wire.VisibilityResult_NONE:
		return nil
	case wire.VisibilityResult_INVALID_ARGUMENT:
		return serviceerror.NewInvalidArgument(r.Message)
	case wire.VisibilityResult_NOT_FOUND:
		return serviceerror.NewNotFound(r.Message)
	case wire.VisibilityResult_UNAVAILABLE:
		return serviceerror.NewUnavailable(r.Message)
	case wire.VisibilityResult_RESOURCE_EXHAUSTED:
		return serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, r.Message)
	default:
		return serviceerror.NewInternal(r.Message)
	}
}
func (s *VisibilityStore) ValidateCustomSearchAttributes(attrs map[string]any) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	types, _, e := s.visibilityTypes(ctx)
	if e != nil {
		return nil, e
	}
	out := map[string]any{}
	var invalid error
	for name, v := range attrs {
		typ, e := types.GetType(name)
		if e != nil {
			invalid = serviceerror.NewInvalidArgument(e.Error())
			continue
		}
		if _, e = vmodel.Value(typ, v); e != nil {
			invalid = serviceerror.NewInvalidArgument(name + ": " + e.Error())
			continue
		}
		out[name] = v
	}
	return out, invalid
}
func (s *VisibilityStore) document(ctx context.Context, b *store.InternalVisibilityRequestBase) (*wire.VisibilityDocument, error) {
	if b == nil {
		return nil, serviceerror.NewInvalidArgument("nil visibility request")
	}
	n, e := vmodel.CanonicalUUID(b.NamespaceID)
	if e != nil {
		return nil, serviceerror.NewInvalidArgument(e.Error())
	}
	r, e := vmodel.CanonicalUUID(b.RunID)
	if e != nil {
		return nil, serviceerror.NewInvalidArgument(e.Error())
	}
	d := &wire.VisibilityDocument{NamespaceId: n, RunId: r, WorkflowId: b.WorkflowID, WorkflowType: b.WorkflowTypeName, StartTime: vmodel.Time(b.StartTime), ExecutionTime: vmodel.Time(b.ExecutionTime), Status: int32(b.Status), TaskId: b.TaskID, ShardId: b.ShardID, TaskQueue: b.TaskQueue, ParentWorkflowId: b.ParentWorkflowID, ParentRunId: b.ParentRunID, RootWorkflowId: b.RootWorkflowID, RootRunId: b.RootRunID, Attributes: map[string]*wire.VisibilityAttribute{}}
	if b.Memo != nil {
		d.Memo = &wire.HistoryBlob{Data: b.Memo.Data, Encoding: int32(b.Memo.EncodingType)}
	}
	types, _, e := s.visibilityTypes(ctx)
	if e != nil {
		return nil, e
	}
	attrs, e := searchattribute.Decode(b.SearchAttributes, &types, false)
	if e != nil {
		return nil, e
	}
	var retained *commonpb.SearchAttributes
	if b.SearchAttributes != nil {
		retained = proto.Clone(b.SearchAttributes).(*commonpb.SearchAttributes)
	}
	for name, v := range attrs {
		if v == nil {
			if retained != nil {
				delete(retained.IndexedFields, name)
			}
			continue
		}
		typ, e := types.GetType(name)
		if e != nil {
			typ = sadefs.GetMetadataType(b.SearchAttributes.GetIndexedFields()[name])
		}
		a, e := vmodel.Value(typ, v)
		if e != nil {
			return nil, serviceerror.NewInvalidArgument(name + ": " + e.Error())
		}
		d.Attributes[name] = a
	}
	if retained != nil {
		d.SearchAttributes, e = proto.Marshal(retained)
		if e != nil {
			return nil, e
		}
	}
	return d, nil
}
func (s *VisibilityStore) writeVisibility(ctx context.Context, kind wire.VisibilityCommand_Kind, d *wire.VisibilityDocument) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	partition, e := vmodel.Partition(d.NamespaceId, d.RunId)
	if e != nil {
		return serviceerror.NewInvalidArgument(e.Error())
	}
	_, e = s.invokeVisibility(ctx, partition, &wire.VisibilityCommand{Kind: kind, Document: d})
	return e
}
func (s *VisibilityStore) RecordWorkflowExecutionStarted(ctx context.Context, r *store.InternalRecordWorkflowExecutionStartedRequest) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if r == nil {
		return serviceerror.NewInvalidArgument("nil visibility request")
	}
	d, e := s.document(ctx, r.InternalVisibilityRequestBase)
	if e != nil {
		return e
	}
	return s.writeVisibility(ctx, wire.VisibilityCommand_START, d)
}
func (s *VisibilityStore) RecordWorkflowExecutionClosed(ctx context.Context, r *store.InternalRecordWorkflowExecutionClosedRequest) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if r == nil {
		return serviceerror.NewInvalidArgument("nil visibility request")
	}
	d, e := s.document(ctx, r.InternalVisibilityRequestBase)
	if e != nil {
		return e
	}
	d.CloseTime = vmodel.Time(r.CloseTime)
	d.HistoryLength = r.HistoryLength
	d.HistorySizeBytes = r.HistorySizeBytes
	d.ExecutionDuration = int64(r.ExecutionDuration)
	d.StateTransitionCount = r.StateTransitionCount
	return s.writeVisibility(ctx, wire.VisibilityCommand_CLOSE, d)
}
func (s *VisibilityStore) UpsertWorkflowExecution(ctx context.Context, r *store.InternalUpsertWorkflowExecutionRequest) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if r == nil {
		return serviceerror.NewInvalidArgument("nil visibility request")
	}
	d, e := s.document(ctx, r.InternalVisibilityRequestBase)
	if e != nil {
		return e
	}
	return s.writeVisibility(ctx, wire.VisibilityCommand_UPSERT, d)
}
func (s *VisibilityStore) DeleteWorkflowExecution(ctx context.Context, r *manager.VisibilityDeleteWorkflowExecutionRequest) error {
	if r == nil {
		return serviceerror.NewInvalidArgument("nil visibility delete")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	n, e := vmodel.CanonicalUUID(r.NamespaceID.String())
	if e != nil {
		return serviceerror.NewInvalidArgument(e.Error())
	}
	run, e := vmodel.CanonicalUUID(r.RunID)
	if e != nil {
		return serviceerror.NewInvalidArgument(e.Error())
	}
	p, e := vmodel.Partition(n, run)
	if e != nil {
		return e
	}
	_, e = s.invokeVisibility(ctx, p, &wire.VisibilityCommand{Kind: wire.VisibilityCommand_DELETE, NamespaceId: n, RunId: run})
	return e
}
func visibilityInfo(d *wire.VisibilityDocument) (*store.InternalExecutionInfo, error) {
	if d == nil {
		return nil, serviceerror.NewInternal("missing visibility row")
	}
	r := &store.InternalExecutionInfo{WorkflowID: d.WorkflowId, RunID: d.RunId, TypeName: d.WorkflowType, StartTime: vmodel.GoTime(d.StartTime), ExecutionTime: vmodel.GoTime(d.ExecutionTime), CloseTime: vmodel.GoTime(d.CloseTime), Status: enumspb.WorkflowExecutionStatus(d.Status), TaskQueue: d.TaskQueue, RootWorkflowID: d.RootWorkflowId, RootRunID: d.RootRunId, ParentWorkflowID: d.GetParentWorkflowId(), ParentRunID: d.GetParentRunId(), HistoryLength: d.HistoryLength, HistorySizeBytes: d.HistorySizeBytes, ExecutionDuration: time.Duration(d.ExecutionDuration), StateTransitionCount: d.StateTransitionCount}
	if d.Memo != nil {
		r.Memo = &commonpb.DataBlob{Data: d.Memo.Data, EncodingType: enumspb.EncodingType(d.Memo.Encoding)}
	}
	if len(d.SearchAttributes) > 0 {
		r.SearchAttributes = new(commonpb.SearchAttributes)
		if e := proto.Unmarshal(d.SearchAttributes, r.SearchAttributes); e != nil {
			return nil, e
		}
	}

	if r.ExecutionTime.UnixNano() == 0 {
		r.ExecutionTime = r.StartTime
	}
	return r, nil
}
func (s *VisibilityStore) GetWorkflowExecution(ctx context.Context, r *manager.GetWorkflowExecutionRequest) (*store.InternalGetWorkflowExecutionResponse, error) {
	if r == nil {
		return nil, serviceerror.NewInvalidArgument("nil visibility get")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	n, e := vmodel.CanonicalUUID(r.NamespaceID.String())
	if e != nil {
		return nil, serviceerror.NewInvalidArgument(e.Error())
	}
	run, e := vmodel.CanonicalUUID(r.RunID)
	if e != nil {
		return nil, serviceerror.NewInvalidArgument(e.Error())
	}
	p, e := vmodel.Partition(n, run)
	if e != nil {
		return nil, e
	}
	out, e := s.invokeVisibility(ctx, p, &wire.VisibilityCommand{Kind: wire.VisibilityCommand_GET, NamespaceId: n, RunId: run})
	if e != nil {
		return nil, e
	}
	if len(out.Documents) != 1 {
		return nil, serviceerror.NewInternal("invalid visibility get response")
	}
	info, e := visibilityInfo(out.Documents[0])
	return &store.InternalGetWorkflowExecutionResponse{Execution: info}, e
}
func (s *VisibilityStore) visibilityTypes(ctx context.Context) (searchattribute.NameTypeMap, *wire.VisibilityResult, error) {
	types, e := s.provider.GetSearchAttributes(s.index, false)
	if e != nil {
		return types, nil, e
	}
	schema, e := s.invokeVisibility(ctx, s.schemaPartition, &wire.VisibilityCommand{Kind: wire.VisibilityCommand_GET_SCHEMA})
	if e != nil {
		return types, nil, e
	}
	custom := map[string]enumspb.IndexedValueType{}
	for n, t := range schema.SearchAttributeTypes {
		custom[n] = enumspb.IndexedValueType(t)
	}
	return searchattribute.MergeNameTypeMaps(types, searchattribute.NewNameTypeMap(custom)), schema, nil
}
func (s *VisibilityStore) compileVisibility(ctx context.Context, n string, name namespace.Name, text string, archetype chasm.ArchetypeID) (*wire.VisibilityQuery, error) {
	types, e := s.provider.GetSearchAttributes(s.index, false)
	if e != nil {
		return nil, e
	}
	var mapper searchattribute.Mapper
	if s.mappers != nil {
		mapper, e = s.mappers.GetMapper(name)
		if e != nil {
			return nil, e
		}
	}
	schema, e := s.invokeVisibility(ctx, s.schemaPartition, &wire.VisibilityCommand{Kind: wire.VisibilityCommand_GET_SCHEMA})
	if e != nil {
		return nil, e
	}
	customTypes := map[string]enumspb.IndexedValueType{}
	for field, typ := range schema.SearchAttributeTypes {
		customTypes[field] = enumspb.IndexedValueType(typ)
	}
	types = searchattribute.MergeNameTypeMaps(types, searchattribute.NewNameTypeMap(customTypes))
	cfg := qmodel.Config{NamespaceID: n, NamespaceName: name, Types: types, Mapper: mapper, ArchetypeID: archetype, SchemaVersion: schema.SchemaVersion}
	if archetype != 0 {
		if s.chasm == nil {
			return nil, serviceerror.NewInvalidArgument("CHASM registry missing")
		}
		component, ok := s.chasm.ComponentByID(archetype)
		if !ok {
			return nil, serviceerror.NewInvalidArgument("unknown CHASM archetype")
		}
		cfg.ChasmMapper = component.SearchAttributesMapper()
	}
	q, e := qmodel.Compile(text, cfg)
	if e != nil {
		return nil, serviceerror.NewInvalidArgument(e.Error())
	}
	contextValues := map[string]any{"types": types.All(), "schema": schema.SearchAttributeTypes}
	aliases := map[string]string{}
	if mapper != nil {
		for field := range types.Custom() {
			if alias, err := mapper.GetAlias(field, name.String()); err == nil {
				aliases[field] = alias
			}
		}
	}
	contextValues["aliases"] = aliases
	contextRaw, _ := json.Marshal(contextValues)
	contextDigest := sha256.Sum256(contextRaw)
	q.SchemaContextSha256 = contextDigest[:]

	return q, nil
}

type visibilityPageCursor struct {
	Version int
	Binding []byte
	Last    string
}

func (s *VisibilityStore) listVisibility(ctx context.Context, n string, name namespace.Name, text string, size int, token []byte, archetype chasm.ArchetypeID) (*store.InternalListExecutionsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if size < 1 || size > 1000 {
		return nil, serviceerror.NewInvalidArgument("visibility page size must be 1..1000")
	}
	q, e := s.compileVisibility(ctx, n, name, text, archetype)
	if e != nil {
		return nil, e
	}
	if len(q.GroupBy) > 0 {
		return nil, serviceerror.NewInvalidArgument("GROUP BY is supported only by Count")
	}
	digest, e := qmodel.Digest(q)
	if e != nil {
		return nil, e
	}
	if len(token) > 0 {
		var t visibilityPageCursor
		if json.Unmarshal(token, &t) != nil || t.Version != 1 || t.Last == "" || string(t.Binding) != string(digest[:]) {
			return nil, serviceerror.NewInvalidArgument("visibility cursor does not match query")
		}
	}

	type stream struct {
		rows     []*wire.VisibilityDocument
		next     []byte
		finished bool
	}
	streams := make([]stream, vmodel.PartitionCount)
	load := func(ctx context.Context, i int, token []byte) error {
		r, err := s.invokeVisibility(ctx, fmt.Sprintf("vis-v1-%d", i), &wire.VisibilityCommand{Kind: wire.VisibilityCommand_LIST, Query: q, PageSize: int64(size), NextPageToken: token})
		if err != nil {
			return err
		}
		streams[i] = stream{r.Documents, r.NextPageToken, len(r.Documents) == 0 || len(r.NextPageToken) == 0}
		return nil
	}
	if e = parallelVisibility(ctx, func(ctx context.Context, i int) error { return load(ctx, i, token) }); e != nil {
		return nil, e
	}
	result := &store.InternalListExecutionsResponse{}
	budget := new(wire.VisibilityResult)
	for len(result.Executions) < size {
		// An exhausted byte-bounded local buffer is refilled before choosing the next
		// global key. Otherwise unseen rows in that partition could be skipped forever.
		best := -1
		for i := range streams {
			if len(streams[i].rows) == 0 && !streams[i].finished {
				if e = load(ctx, i, streams[i].next); e != nil {
					return nil, e
				}
			}
			if len(streams[i].rows) > 0 && (best < 0 || vmodel.SortKey(streams[i].rows[0]) < vmodel.SortKey(streams[best].rows[0])) {
				best = i
			}
		}
		if best < 0 {
			break
		}
		d := streams[best].rows[0]
		budget.Documents = append(budget.Documents, d)
		next, _ := json.Marshal(visibilityPageCursor{1, digest[:], vmodel.SortKey(d)})
		budget.NextPageToken = next
		if proto.Size(budget) > vmodel.ResponseBudget {
			if len(result.Executions) == 0 {
				return nil, serviceerror.NewInternal("visibility document exceeds page budget")
			}
			break
		}
		info, err := visibilityInfo(d)
		if err != nil {
			return nil, err
		}
		result.Executions = append(result.Executions, info)
		result.NextPageToken = next
		streams[best].rows = streams[best].rows[1:]
	}

	return result, nil
}
func (s *VisibilityStore) countVisibility(ctx context.Context, n string, name namespace.Name, text string, archetype chasm.ArchetypeID) (*store.InternalCountExecutionsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	q, e := s.compileVisibility(ctx, n, name, text, archetype)
	if e != nil {
		return nil, e
	}
	result := new(store.InternalCountExecutionsResponse)
	groups := map[string]*wire.VisibilityGroup{}
	parts := make([]*wire.VisibilityResult, vmodel.PartitionCount)
	if e = parallelVisibility(ctx, func(ctx context.Context, i int) error {
		r, err := s.invokeVisibility(ctx, fmt.Sprintf("vis-v1-%d", i), &wire.VisibilityCommand{Kind: wire.VisibilityCommand_COUNT, Query: q})
		parts[i] = r
		return err
	}); e != nil {
		return nil, e
	}
	for _, r := range parts {
		result.Count += r.Count
		for _, g := range r.Groups {
			copy := proto.Clone(g).(*wire.VisibilityGroup)
			copy.Count = 0
			b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(copy)
			k := string(b)
			if old := groups[k]; old != nil {
				old.Count += g.Count
			} else {
				groups[k] = g
			}
		}
	}
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		g := groups[k]
		out := store.InternalAggregationGroup{Count: g.Count}
		for _, v := range g.Values {
			p, e := payload.Encode(vmodel.Scalar(v))
			if e != nil {
				return nil, e
			}
			if v != nil {
				sadefs.SetMetadataType(p, enumspb.IndexedValueType(v.ValueType))
			}
			out.GroupValues = append(out.GroupValues, p)
		}
		result.Groups = append(result.Groups, out)
	}
	return result, nil
}
func (s *VisibilityStore) ListWorkflowExecutions(ctx context.Context, r *manager.ListWorkflowExecutionsRequestV2) (*store.InternalListExecutionsResponse, error) {
	if r == nil {
		return nil, serviceerror.NewInvalidArgument("nil list request")
	}
	return s.listVisibility(ctx, r.NamespaceID.String(), r.Namespace, r.Query, r.PageSize, r.NextPageToken, 0)
}
func (s *VisibilityStore) CountWorkflowExecutions(ctx context.Context, r *manager.CountWorkflowExecutionsRequest) (*store.InternalCountExecutionsResponse, error) {
	if r == nil {
		return nil, serviceerror.NewInvalidArgument("nil count request")
	}
	return s.countVisibility(ctx, r.NamespaceID.String(), r.Namespace, r.Query, 0)
}
func (s *VisibilityStore) ListChasmExecutions(ctx context.Context, r *visibilityservice.ListChasmExecutionsRequest) (*store.InternalListExecutionsResponse, error) {
	if r == nil {
		return nil, serviceerror.NewInvalidArgument("nil CHASM list")
	}
	return s.listVisibility(ctx, r.NamespaceId, namespace.Name(r.Namespace), r.Query, int(r.PageSize), r.NextPageToken, r.ArchetypeId)
}
func (s *VisibilityStore) CountChasmExecutions(ctx context.Context, r *visibilityservice.CountChasmExecutionsRequest) (*store.InternalCountExecutionsResponse, error) {
	if r == nil {
		return nil, serviceerror.NewInvalidArgument("nil CHASM count")
	}
	return s.countVisibility(ctx, r.NamespaceId, namespace.Name(r.Namespace), r.Query, r.ArchetypeId)
}
func (s *VisibilityStore) AddSearchAttributes(ctx context.Context, r *manager.AddSearchAttributesRequest) error {
	if r == nil {
		return serviceerror.NewInvalidArgument("nil search attribute request")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	c := &wire.VisibilityCommand{Kind: wire.VisibilityCommand_ADD_ATTRIBUTES, SearchAttributeTypes: map[string]int32{}}
	providerTypes, e := s.provider.GetSearchAttributes(s.index, false)
	if e != nil {
		return e
	}
	for n, t := range r.SearchAttributes {
		if existing, err := providerTypes.GetType(n); err == nil && existing != t {
			return serviceerror.NewInvalidArgument("search attribute type is immutable")
		}
		c.SearchAttributeTypes[n] = int32(t)
	}
	_, e = s.invokeVisibility(ctx, s.schemaPartition, c)
	return e
}

// Initial partition reads are independent live reads, not a cross-partition
// snapshot. Join every worker before return and preserve the first real failure.
func parallelVisibility(ctx context.Context, call func(context.Context, int) error) error {
	// Peer errors must not cancel admitted sibling durability operations: doing
	// so can quarantine a healthy owner mid-commit. The caller deadline still bounds all reads.
	var wg sync.WaitGroup
	var once sync.Once
	var first error
	for i := 0; i < vmodel.PartitionCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := call(ctx, i); err != nil {
				once.Do(func() { first = err })
			}
		}(i)
	}
	wg.Wait()
	return first
}
