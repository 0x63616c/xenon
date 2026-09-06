package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	vmodel "github.com/0x63616c/xenon/internal/visibility"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/visibilityservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	upstream "go.temporal.io/server/common/persistence/tests"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"math"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

type visibilityProxy struct {
	wire.UnimplementedVisibilityPersistenceServer
	clients       map[string]wire.VisibilityPersistenceClient
	mu            sync.Mutex
	first         *wire.VisibilityRequest
	replayed      bool
	failPartition string
}

func (p *visibilityProxy) Execute(ctx context.Context, q *wire.VisibilityRequest) (*wire.VisibilityResult, error) {
	p.mu.Lock()
	fail := p.failPartition == q.Partition
	p.mu.Unlock()
	if fail {
		return nil, status.Error(codes.Unavailable, "declared unavailable partition")
	}
	c := p.clients[q.Partition]
	if c == nil {
		return nil, status.Error(codes.InvalidArgument, "unknown partition")
	}
	r, e := c.Execute(ctx, q)
	if e != nil {
		return nil, e
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if q.Command.Kind == wire.VisibilityCommand_START {
		if p.first == nil {
			p.first = proto.Clone(q).(*wire.VisibilityRequest)
			return nil, status.Error(codes.Unavailable, "declared lost completed visibility write")
		}
		if q.OperationId == p.first.OperationId {
			p.replayed = proto.Equal(q, p.first)
		}
	}
	return r, nil
}
func visibilityProcesses(t *testing.T) (string, *visibilityProxy) {
	t.Helper()
	raw, e := os.ReadFile("../../../proof/visibility/case.json")
	if e != nil {
		t.Fatal(e)
	}
	var fixture struct {
		Schema  int
		Backend string
		Startup int `json:"startup_seconds"`
		Fault   string
	}
	if json.Unmarshal(raw, &fixture) != nil || fixture.Schema != 1 || fixture.Backend != "memory" || fixture.Fault != "drop_first_completed_start_response" {
		t.Fatal("invalid visibility fixture")
	}
	proxy := &visibilityProxy{clients: map[string]wire.VisibilityPersistenceClient{}}
	partitions := []string{"global", "vis-v1-0", "vis-v1-1", "vis-v1-2", "vis-v1-3"}
	for _, partition := range partitions {
		address := startNamespaceNode(t, namespaceCase{Backend: fixture.Backend, Partition: partition, Prefix: "visibility/" + partition, StartupTimeoutSeconds: fixture.Startup})
		conn, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { _ = conn.Close() })
		proxy.clients[partition] = wire.NewVisibilityPersistenceClient(conn)
	}
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	server := grpc.NewServer()
	wire.RegisterVisibilityPersistenceServer(server, proxy)
	go server.Serve(l)
	t.Cleanup(server.Stop)
	return l.Addr().String(), proxy
}

// The upstream suite owns its genuine manager and all assertions. Only its
// database fixture is replaced by the declared five real Go service processes.
type visibilityTestCluster struct{ address string }

func (visibilityTestCluster) SetupTestDatabase()    {}
func (visibilityTestCluster) TearDownTestDatabase() {}
func (c visibilityTestCluster) Config() config.Persistence {
	return config.Persistence{VisibilityStore: "visibility", DataStores: map[string]config.DataStore{"visibility": {CustomDataStoreConfig: &config.CustomDatastoreConfig{Name: "xenon", IndexName: "xenon-visibility", Options: map[string]any{"address": c.address}}}}}
}

type visibilityTestFactory struct{}

func (visibilityTestFactory) NewVisibilityStore(c config.CustomDatastoreConfig, p searchattribute.Provider, m searchattribute.MapperProvider, _ namespace.Registry, r *chasm.Registry, _ resolver.ServiceResolver, _ log.Logger, _ metrics.Handler) (store.VisibilityStore, error) {
	return NewVisibilityStore(fmt.Sprint(c.Options["address"]), "xenon-visibility", "global", p, m, r)
}
func TestVisibilityUpstream(t *testing.T) {
	address, proxy := visibilityProcesses(t)
	s := new(upstream.VisibilityPersistenceSuite)
	s.TestBase = &persistencetests.TestBase{DefaultTestCluster: visibilityTestCluster{address}, Logger: log.NewTestLogger()}
	s.CustomVisibilityStoreFactory = visibilityTestFactory{}
	suite.Run(t, s)
	proxy.mu.Lock()
	defer proxy.mu.Unlock()
	if !proxy.replayed {
		t.Fatal("lost completed write did not preserve exact retry identity")
	}
}

type visibilityComponent struct{ chasm.UnimplementedComponent }

func (*visibilityComponent) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

type visibilityLibrary struct{ chasm.UnimplementedLibrary }

func (visibilityLibrary) Name() string { return "VisibilityProof" }
func (visibilityLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{chasm.NewRegistrableComponent[*visibilityComponent]("Record", chasm.WithSearchAttributes(chasm.NewSearchAttributeInt("Value", chasm.SearchAttributeFieldInt01)))}
}
func TestVisibilityRPC(t *testing.T) {
	address, proxy := visibilityProcesses(t)
	registry := chasm.NewRegistry(log.NewNoopLogger())
	if e := registry.Register(visibilityLibrary{}); e != nil {
		t.Fatal(e)
	}
	archetype, ok := registry.ComponentIDByFqn("VisibilityProof.Record")
	if !ok {
		t.Fatal("missing proof archetype")
	}
	s, e := NewVisibilityStore(address, "xenon-visibility", "global", searchattribute.NewTestEsProvider(), nil, registry)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	ns := "11111111-1111-1111-1111-111111111111"
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	var same []string
	var other string
	for i := 1; len(same) < 3 || other == ""; i++ {
		run := fmt.Sprintf("00000000-0000-0000-0000-%012x", i)
		p, _ := vmodel.Partition(ns, run)
		if p == "vis-v1-0" && len(same) < 3 {
			same = append(same, run)
		} else if p != "vis-v1-0" && other == "" {
			other = run
		}
	}
	for i, run := range append(append([]string{}, same...), other) {
		b := &store.InternalVisibilityRequestBase{NamespaceID: ns, RunID: run, WorkflowID: run, StartTime: start.Add(-time.Duration(i) * time.Hour), ExecutionTime: start, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, TaskID: 1}
		if i < 3 {
			b.Memo = &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_JSON, Data: bytes.Repeat([]byte{byte(i + 1)}, 1100000)}
		}
		if e = s.RecordWorkflowExecutionStarted(ctx, &store.InternalRecordWorkflowExecutionStartedRequest{InternalVisibilityRequestBase: b}); e != nil {
			t.Fatal(e)
		}
	}
	request := &manager.ListWorkflowExecutionsRequestV2{NamespaceID: namespace.ID(ns), PageSize: 10}
	first, e := s.ListWorkflowExecutions(ctx, request)
	if e != nil || len(first.Executions) != 2 {
		t.Fatal("byte page", first, e)
	}
	var ids []string
	for _, r := range first.Executions {
		ids = append(ids, r.RunID)
	}
	request.NextPageToken = first.NextPageToken
	for len(request.NextPageToken) > 0 {
		page, e := s.ListWorkflowExecutions(ctx, request)
		if e != nil {
			t.Fatal(e)
		}
		for _, r := range page.Executions {
			ids = append(ids, r.RunID)
		}
		request.NextPageToken = page.NextPageToken
	}
	want := append(append([]string{}, same...), other)
	if fmt.Sprint(ids) != fmt.Sprint(want) {
		t.Fatal("byte merge skipped/reordered", ids, want)
	}
	var broken map[string]any
	if e = json.Unmarshal(first.NextPageToken, &broken); e != nil {
		t.Fatal(e)
	}
	delete(broken, "Last")
	badToken, _ := json.Marshal(broken)
	request.NextPageToken = badToken
	if _, e = s.ListWorkflowExecutions(ctx, request); e == nil {
		t.Fatal("missing cursor key silently restarted")
	}
	request.NextPageToken = first.NextPageToken
	request.Query = "WorkflowType = 'changed'"
	if _, e = s.ListWorkflowExecutions(ctx, request); e == nil {
		t.Fatal("changed query token accepted")
	}
	proxy.mu.Lock()
	proxy.failPartition = "vis-v1-3"
	proxy.mu.Unlock()
	if r, e := s.CountWorkflowExecutions(ctx, &manager.CountWorkflowExecutionsRequest{NamespaceID: namespace.ID(ns)}); e == nil || r != nil {
		t.Fatal("partial successful count")
	}
	proxy.mu.Lock()
	proxy.failPartition = ""
	proxy.mu.Unlock()
	if e = s.AddSearchAttributes(ctx, &manager.AddSearchAttributesRequest{SearchAttributes: map[string]enumspb.IndexedValueType{"ExtraKeyword": enumspb.INDEXED_VALUE_TYPE_KEYWORD, "CustomKeywordListField": enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST}}); e != nil {
		t.Fatal(e)
	}
	if e = s.AddSearchAttributes(ctx, &manager.AddSearchAttributesRequest{SearchAttributes: map[string]enumspb.IndexedValueType{"ExtraKeyword": enumspb.INDEXED_VALUE_TYPE_KEYWORD, "CustomKeywordListField": enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST}}); e != nil {
		t.Fatal(e)
	}
	if e = s.AddSearchAttributes(ctx, &manager.AddSearchAttributesRequest{SearchAttributes: map[string]enumspb.IndexedValueType{"ExtraKeyword": enumspb.INDEXED_VALUE_TYPE_INT}}); e == nil {
		t.Fatal("type replacement accepted")
	}
	run := "99999999-9999-9999-9999-999999999999"
	attrs, e := searchattribute.Encode(map[string]any{"CustomIntField": int64(math.MaxInt64), "CustomBoolField": true, "CustomKeywordListField": []string{"x", "y"}, "CustomTextField": "a:1 b:2", "CustomDoubleField": 1.234565, "CustomDatetimeField": start, "ExtraKeyword": "extra"}, nil)
	if e != nil {
		t.Fatal(e)
	}
	// Explicit metadata supplies newly registered fields before provider cache refresh.
	attrs.IndexedFields["ExtraKeyword"].Metadata["type"] = []byte("Keyword")
	attrs.IndexedFields["CustomKeywordListField"].Metadata["type"] = []byte("KeywordList")
	types := searchattribute.TestEsNameTypeMap()
	for name, p := range attrs.IndexedFields {
		if typ, err := types.GetType(name); err == nil {
			p.Metadata["type"] = []byte(typ.String())
		}
	}
	b := &store.InternalVisibilityRequestBase{NamespaceID: ns, RunID: run, WorkflowID: "attributes", StartTime: start.Add(time.Hour), ExecutionTime: start, TaskID: 1, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, SearchAttributes: attrs}
	if e = s.RecordWorkflowExecutionStarted(ctx, &store.InternalRecordWorkflowExecutionStartedRequest{InternalVisibilityRequestBase: b}); e != nil {
		t.Fatal(e)
	}
	for _, query := range []string{"CustomIntField = 9223372036854775807", "CustomBoolField = true", "CustomKeywordListField IN ('y','z')", "CustomTextField = 'a<->b'", "CustomDoubleField = 1.23457", "CustomDatetimeField = '2026-01-02T03:04:05Z'", "ExtraKeyword = 'extra'"} {
		r, e := s.CountWorkflowExecutions(ctx, &manager.CountWorkflowExecutionsRequest{NamespaceID: namespace.ID(ns), Query: query})
		if e != nil || r.Count != 1 {
			t.Fatal(query, r, e)
		}
	}
	// Generated scalar columns normalize query values; returned SA JSON stays raw.
	got, e := s.GetWorkflowExecution(ctx, &manager.GetWorkflowExecutionRequest{NamespaceID: namespace.ID(ns), RunID: run})
	if e != nil {
		t.Fatal(e)
	}
	if !proto.Equal(got.Execution.SearchAttributes, attrs) {
		t.Fatal("raw attributes were rewritten")
	}
	fractions := []struct{ input, match string }{{"2026-01-02T03:04:05.0000005Z", "2026-01-02T03:04:05Z"}, {"2026-01-02T03:04:05.0000015Z", "2026-01-02T03:04:05.000002Z"}, {"2026-01-02T03:04:05.0000025Z", "2026-01-02T03:04:05.000002Z"}, {"1969-12-31T23:59:59.9999995Z", "1970-01-01T00:00:00Z"}}
	for i, f := range fractions {
		value, err := time.Parse(time.RFC3339Nano, f.input)
		if err != nil {
			t.Fatal(err)
		}
		b.TaskID = int64(i + 2)
		typed := searchattribute.TestEsNameTypeMap()
		b.SearchAttributes, err = searchattribute.Encode(map[string]any{"CustomDatetimeField": value, "CustomDoubleField": 1.234565, "CustomKeywordField": nil}, &typed)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.UpsertWorkflowExecution(ctx, &store.InternalUpsertWorkflowExecutionRequest{InternalVisibilityRequestBase: b}); err != nil {
			t.Fatal(err)
		}
		result, err := s.CountWorkflowExecutions(ctx, &manager.CountWorkflowExecutionsRequest{NamespaceID: namespace.ID(ns), Query: "CustomDatetimeField = '" + f.match + "'"})
		if err != nil || result.Count != 1 {
			t.Fatal(f, result, err)
		}
		got, err := s.GetWorkflowExecution(ctx, &manager.GetWorkflowExecutionRequest{NamespaceID: namespace.ID(ns), RunID: run})
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(got.Execution.SearchAttributes.IndexedFields["CustomDatetimeField"], b.SearchAttributes.IndexedFields["CustomDatetimeField"]) || !proto.Equal(got.Execution.SearchAttributes.IndexedFields["CustomDoubleField"], b.SearchAttributes.IndexedFields["CustomDoubleField"]) {
			t.Fatal("raw scalar response changed")
		}
		if _, exists := got.Execution.SearchAttributes.IndexedFields["CustomKeywordField"]; exists {
			t.Fatal("null removal attribute retained")
		}
	}
	// CHASM's own alias mapper and namespace-division predicate are retained.
	ca, e := searchattribute.Encode(map[string]any{"TemporalNamespaceDivision": strconv.Itoa(int(archetype)), "TemporalInt01": int64(77)}, nil)
	if e != nil {
		t.Fatal(e)
	}
	ca.IndexedFields["TemporalNamespaceDivision"].Metadata["type"] = []byte("Keyword")
	ca.IndexedFields["TemporalInt01"].Metadata["type"] = []byte("Int")
	cb := *b
	cb.RunID = "88888888-8888-8888-8888-888888888888"
	cb.SearchAttributes = ca
	if e = s.RecordWorkflowExecutionStarted(ctx, &store.InternalRecordWorkflowExecutionStartedRequest{InternalVisibilityRequestBase: &cb}); e != nil {
		t.Fatal(e)
	}
	ch, e := s.CountChasmExecutions(ctx, &visibilityservice.CountChasmExecutionsRequest{NamespaceId: ns, ArchetypeId: archetype, Query: "Value = 77"})
	if e != nil || ch.Count != 1 {
		t.Fatal(ch, e)
	}
	cl, e := s.ListChasmExecutions(ctx, &visibilityservice.ListChasmExecutionsRequest{NamespaceId: ns, ArchetypeId: archetype, PageSize: 10, Query: "Value = 77"})
	if e != nil || len(cl.Executions) != 1 || cl.Executions[0].RunID != cb.RunID {
		t.Fatal(cl, e)
	}
	if e = s.DeleteWorkflowExecution(ctx, &manager.VisibilityDeleteWorkflowExecutionRequest{NamespaceID: namespace.ID(ns), RunID: run, TaskID: 0}); e != nil {
		t.Fatal(e)
	}
	b.TaskID = math.MaxInt64
	if e = s.UpsertWorkflowExecution(ctx, &store.InternalUpsertWorkflowExecutionRequest{InternalVisibilityRequestBase: b}); e != nil {
		t.Fatal(e)
	}
	_, e = s.GetWorkflowExecution(ctx, &manager.GetWorkflowExecutionRequest{NamespaceID: namespace.ID(ns), RunID: run})
	var absent *serviceerror.NotFound
	if !errors.As(e, &absent) {
		t.Fatal("terminal delete resurrected", e)
	}
}
