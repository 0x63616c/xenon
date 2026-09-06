package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/temporal/adapter"
	vmodel "github.com/0x63616c/xenon/internal/visibility"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/grpc"
	native "slatedb.io/slatedb-go/uniffi"
)

type frozenVisibilityRouter struct {
	wire.UnimplementedVisibilityPersistenceServer
	servers map[string]*VisibilityServer
}

func (r *frozenVisibilityRouter) Execute(ctx context.Context, q *wire.VisibilityRequest) (*wire.VisibilityResult, error) {
	return r.servers[q.Partition].Execute(ctx, q)
}

func TestGoOwnerVisibilityFrozen(t *testing.T) {
	raw, e := os.ReadFile("../../proof/visibility/frozen.json")
	if e != nil {
		t.Fatal(e)
	}
	var f struct {
		Records   int
		PageSizes []int
		Flush     string
		Namespace string
	}
	if json.Unmarshal(raw, &f) != nil || f.Records != 2000 || fmt.Sprint(f.PageSizes) != "[1 7 100]" || f.Flush != "1ms" {
		t.Fatal("invalid frozen fixture")
	}
	router := &frozenVisibilityRouter{servers: map[string]*VisibilityServer{}}
	for _, partition := range []string{"global", "vis-v1-0", "vis-v1-1", "vis-v1-2", "vis-v1-3"} {
		b := native.NewDbBuilder("frozen/"+partition, objects(t))
		settings := native.SettingsDefault()
		if e = settings.Set("flush_interval", `"1ms"`); e != nil {
			t.Fatal(e)
		}
		if e = b.WithSettings(settings); e != nil {
			t.Fatal(e)
		}
		db, err := b.Build()
		b.Destroy()
		settings.Destroy()
		if err != nil {
			t.Fatal(err)
		}
		o, err := NewOwner(db, DefaultConfig(partition))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := o.Close(ctx); err != nil {
				t.Error(err)
			}
		})
		router.servers[partition] = &VisibilityServer{Owner: o}
	}
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	server := grpc.NewServer()
	wire.RegisterVisibilityPersistenceServer(server, router)
	go server.Serve(l)
	t.Cleanup(server.Stop)
	s, e := adapter.NewVisibilityStore(l.Addr().String(), "frozen", "global", searchattribute.NewTestEsProvider(), nil, chasm.NewRegistry(log.NewNoopLogger()))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	expected := map[string]bool{}
	partitionCounts := map[string]int{}
	for i := 0; i < f.Records; i++ {
		run := fmt.Sprintf("00000000-0000-0000-0000-%012x", i+1)
		expected[run] = true
		partition, err := vmodel.Partition(f.Namespace, run)
		if err != nil {
			t.Fatal(err)
		}
		partitionCounts[partition]++
		b := &store.InternalVisibilityRequestBase{NamespaceID: f.Namespace, RunID: run, WorkflowID: fmt.Sprintf("frozen-%04d", i), WorkflowTypeName: fmt.Sprintf("type-%d", i%4), StartTime: start.Add(time.Duration(i) * time.Microsecond), ExecutionTime: start, Status: enumspb.WorkflowExecutionStatus(i%4 + 1), TaskID: 1}
		// Three largest/latest rows force a global byte-bound underfilled page.
		if i >= f.Records-3 {
			b.Memo = &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_JSON, Data: bytes.Repeat([]byte{'x'}, 1100000)}
		}
		if err = s.RecordWorkflowExecutionStarted(ctx, &store.InternalRecordWorkflowExecutionStartedRequest{InternalVisibilityRequestBase: b}); err != nil {
			t.Fatalf("insert%d: %v", i, err)
		}
	}
	if len(partitionCounts) != 4 {
		t.Fatal("missing partition", partitionCounts)
	}
	for _, size := range f.PageSizes {
		q := &manager.ListWorkflowExecutionsRequestV2{NamespaceID: namespace.ID(f.Namespace), PageSize: size}
		seen := map[string]bool{}
		pages := 0
		underfilled := false
		for {
			r, err := s.ListWorkflowExecutions(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			pages++
			if len(r.Executions) < size && len(r.NextPageToken) > 0 {
				underfilled = true
			}
			for _, x := range r.Executions {
				if !expected[x.RunID] || seen[x.RunID] {
					t.Fatal("unexpected/duplicate", x.RunID)
				}
				seen[x.RunID] = true
			}
			if len(r.NextPageToken) == 0 {
				break
			}
			q.NextPageToken = r.NextPageToken
			if pages > f.Records+1 {
				t.Fatal("cursor did not progress")
			}
		}
		if len(seen) != f.Records {
			t.Fatal("omitted rows", size, len(seen))
		}
		if size > 1 && !underfilled {
			t.Fatal("byte limit not exercised", size)
		}
	}
	count, err := s.CountWorkflowExecutions(ctx, &manager.CountWorkflowExecutionsRequest{NamespaceID: namespace.ID(f.Namespace)})
	if err != nil || count.Count != 2000 {
		t.Fatal(count, err)
	}
	groups, err := s.CountWorkflowExecutions(ctx, &manager.CountWorkflowExecutionsRequest{NamespaceID: namespace.ID(f.Namespace), Query: "GROUP BY ExecutionStatus"})
	if err != nil {
		t.Fatal(err)
	}
	if len(groups.Groups) != 4 {
		t.Fatal(groups)
	}
	seenGroups := map[string]bool{}
	for _, g := range groups.Groups {
		var value string
		if len(g.GroupValues) != 1 || payload.Decode(g.GroupValues[0], &value) != nil || g.Count != 500 || seenGroups[value] {
			t.Fatal("group mismatch", g)
		}
		seenGroups[value] = true
	}
	for i := 0; i < 4; i++ {
		if !seenGroups[enumspb.WorkflowExecutionStatus(i+1).String()] {
			t.Fatal("missing group", i)
		}
	}
	t.Logf("frozen records=%d partition_counts=%v; no ownership movement executed", len(expected), partitionCounts)
}
