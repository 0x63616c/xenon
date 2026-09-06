// External component oracle: no topology changes and no workflow execution claims.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/0x63616c/xenon/internal/adapter"
	model "github.com/0x63616c/xenon/internal/visibility"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/protobuf/proto"
)

type fixture struct {
	Records   int
	PageSizes []int
	Namespace string
}
type dataset struct {
	namespace, queue string
	count, offset    int
	updated          int
	concurrent       bool
}

func (d dataset) run(i int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012x", d.offset+i+1) }
func (d dataset) record(i int, version int64) *store.InternalVisibilityRequestBase {
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	b := &store.InternalVisibilityRequestBase{NamespaceID: d.namespace, RunID: d.run(i), WorkflowID: fmt.Sprintf("%s-%04d", d.queue, i), WorkflowTypeName: fmt.Sprintf("type-%d", i%4), TaskQueue: d.queue, StartTime: start.Add(time.Duration(i) * time.Microsecond), ExecutionTime: start, Status: enumspb.WorkflowExecutionStatus(i%4 + 1), TaskID: version}
	if d.offset == 0 && i >= d.count-3 {
		memo, err := proto.Marshal(&commonpb.Memo{Fields: map[string]*commonpb.Payload{"frozen": {Metadata: map[string][]byte{"encoding": []byte("binary/plain")}, Data: bytes.Repeat([]byte{'x'}, 1100000)}}})
		if err != nil {
			panic(err)
		}
		b.Memo = &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: memo}
	}
	return b
}
func (d dataset) expected() map[string]enumspb.WorkflowExecutionStatus {
	m := map[string]enumspb.WorkflowExecutionStatus{}
	for i := 0; i < d.count; i++ {
		m[d.run(i)] = enumspb.WorkflowExecutionStatus(i%4 + 1)
	}
	return m
}
func callContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 30*time.Second)
}
func check(ctx context.Context, api workflowservice.WorkflowServiceClient, ns string, d dataset, sizes []int) (map[string]any, error) {
	return checkPages(ctx, api, ns, d, sizes, nil)
}
func checkPages(ctx context.Context, api workflowservice.WorkflowServiceClient, ns string, d dataset, sizes []int, afterFirstPage func() error) (map[string]any, error) {
	expected := d.expected()
	names := map[string]string{}
	for i := 0; i < d.count; i++ {
		name := fmt.Sprintf("%s-%04d", d.queue, i)
		if i < d.updated {
			name += "-updated"
		}
		names[d.run(i)] = name
	}
	query := "TaskQueue = '" + d.queue + "'"
	pages := map[int]int{}
	hashes := map[int]string{}
	for _, size := range sizes {
		seen := map[string]bool{}
		var token []byte
		lastToken := ""
		pageCount := 0
		for {
			call, cancel := callContext(ctx)
			r, e := api.ListWorkflowExecutions(call, &workflowservice.ListWorkflowExecutionsRequest{Namespace: ns, Query: query, PageSize: int32(size), NextPageToken: token})
			cancel()
			if e != nil {
				return nil, e
			}
			if r == nil {
				return nil, fmt.Errorf("nil list response")
			}
			pageCount++
			for _, x := range r.Executions {
				if x == nil || x.Execution == nil {
					return nil, fmt.Errorf("nil execution record")
				}
				want, ok := expected[x.Execution.RunId]
				if !ok || seen[x.Execution.RunId] || x.Status != want {
					return nil, fmt.Errorf("unexpected, duplicate or wrong-status visibility record")
				}
				nameOK := x.Execution.WorkflowId == names[x.Execution.RunId]
				if d.concurrent {
					nameOK = nameOK || x.Execution.WorkflowId == names[x.Execution.RunId]+"-updated"
				}
				if !nameOK {
					return nil, fmt.Errorf("wrong workflow ID version")
				}
				seen[x.Execution.RunId] = true
			}
			if pageCount == 1 && afterFirstPage != nil {
				if len(r.NextPageToken) == 0 {
					return nil, fmt.Errorf("interpage control requires another page")
				}
				if err := afterFirstPage(); err != nil {
					return nil, err
				}
			}
			if len(r.NextPageToken) == 0 {
				break
			}
			if string(r.NextPageToken) == lastToken || pageCount > d.count+1 {
				return nil, fmt.Errorf("pagination made no progress")
			}
			token = r.NextPageToken
			lastToken = string(token)
		}
		if len(seen) != d.count {
			return nil, fmt.Errorf("page%d count=%d want%d", size, len(seen), d.count)
		}
		ids := make([]string, 0, len(seen))
		for id := range seen {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		raw, _ := json.Marshal(ids)
		hashes[size] = fmt.Sprintf("%x", sha256.Sum256(raw))
		pages[size] = pageCount
	}
	call, cancel := callContext(ctx)
	count, e := api.CountWorkflowExecutions(call, &workflowservice.CountWorkflowExecutionsRequest{Namespace: ns, Query: query})
	cancel()
	if e != nil {
		return nil, e
	}
	if count == nil {
		return nil, fmt.Errorf("nil count response")
	}
	if count.Count != int64(d.count) {
		return nil, fmt.Errorf("count mismatch")
	}
	call, cancel = callContext(ctx)
	groups, e := api.CountWorkflowExecutions(call, &workflowservice.CountWorkflowExecutionsRequest{Namespace: ns, Query: query + " GROUP BY ExecutionStatus"})
	cancel()
	if e != nil {
		return nil, e
	}
	if groups == nil {
		return nil, fmt.Errorf("nil group response")
	}
	got := map[string]int64{}
	for _, g := range groups.Groups {
		var value string
		if g == nil || len(g.GroupValues) != 1 || payload.Decode(g.GroupValues[0], &value) != nil {
			return nil, fmt.Errorf("invalid group")
		}
		if _, ok := got[value]; ok {
			return nil, fmt.Errorf("duplicate group")
		}
		got[value] = g.Count
	}
	for i := 1; i <= 4; i++ {
		if got[enumspb.WorkflowExecutionStatus(i).String()] != int64(d.count/4) {
			return nil, fmt.Errorf("group set mismatch")
		}
	}
	if len(got) != 4 {
		return nil, fmt.Errorf("extra group")
	}
	return map[string]any{"records": d.count, "pages": pages, "set_sha256": hashes, "groups": got}, nil
}
func run() error {
	mode := flag.String("mode", "", "seed, check or mutate")
	address := flag.String("address", "127.0.0.1:17233", "Temporal endpoint")
	storage := flag.String("storage-address", "127.0.0.1:17935", "Xenon ingress")
	ns := flag.String("namespace", "xenon-ministack", "existing namespace")
	casePath := flag.String("fixture", "proof/visibility/frozen.json", "committed fixture")
	output := flag.String("output", "", "new evidence file")
	checkpoint := flag.String("checkpoint", "", "caller movement checkpoint label")
	pageSize := flag.Int("page-size", 0, "check-only traversal size: 1, 7 or 100; zero checks all")
	releaseFile := flag.String("release-file", "", "optional first-page movement barrier file")
	releaseToken := flag.String("release-token", "", "exact movement release token")
	flag.Parse()
	if (*releaseFile == "") != (*releaseToken == "") || (*releaseFile != "" && *mode != "check") {
		return fmt.Errorf("invalid movement barrier options")
	}
	if *releaseFile != "" {
		if _, err := os.Stat(*releaseFile); !os.IsNotExist(err) {
			return fmt.Errorf("movement release file must not exist")
		}
	}
	if *mode != "seed" && *mode != "check" && *mode != "mutate" {
		return fmt.Errorf("invalid mode")
	}
	if *output == "" || *checkpoint == "" {
		return fmt.Errorf("output and checkpoint required")
	}
	raw, e := os.ReadFile(*casePath)
	if e != nil {
		return e
	}
	var f fixture
	if json.Unmarshal(raw, &f) != nil || f.Records != 2000 || fmt.Sprint(f.PageSizes) != "[1 7 100]" {
		return fmt.Errorf("invalid frozen recipe")
	}
	selectedSizes, e := movementPageSizes(*mode, *pageSize)
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	c, e := client.DialContext(ctx, client.Options{HostPort: *address, Namespace: *ns})
	if e != nil {
		return e
	}
	defer c.Close()
	call, done := callContext(ctx)
	description, e := c.WorkflowService().DescribeNamespace(call, &workflowservice.DescribeNamespaceRequest{Namespace: *ns})
	done()
	if e != nil {
		return e
	}
	if description == nil || description.NamespaceInfo == nil || description.NamespaceInfo.Id == "" {
		return fmt.Errorf("missing namespace identity")
	}
	d := dataset{namespace: description.NamespaceInfo.Id, queue: "xenon-frozen-visibility", count: 2000}
	s, e := adapter.NewVisibilityStore(*storage, "xenon-visibility", "global", searchattribute.NewTestEsProvider(), nil, chasm.NewRegistry(log.NewNoopLogger()))
	if e != nil {
		return e
	}
	defer s.Close()
	report := map[string]any{"mode": *mode, "checkpoint": *checkpoint, "namespace_id": d.namespace, "fixture_sha256": fmt.Sprintf("%x", sha256.Sum256(raw)), "full_acceptance": false, "ownership_movement_executed": false, "page_sizes": selectedSizes}
	if *mode == "mutate" {
		d.queue = "xenon-mutation-visibility"
		d.offset = 1000000
		d.count = 20
	}
	if *mode == "seed" || *mode == "mutate" {
		groups := map[string][]int{}
		for i := 0; i < d.count; i++ {
			p, err := model.Partition(d.namespace, d.run(i))
			if err != nil {
				return err
			}
			groups[p] = append(groups[p], i)
		}
		if *mode == "seed" && len(groups) != 4 {
			return fmt.Errorf("recipe did not populate four partitions")
		}
		partitions, err := seedPartitions(ctx, groups, func(ctx context.Context, i int) error {
			return s.RecordWorkflowExecutionStarted(ctx, &store.InternalRecordWorkflowExecutionStartedRequest{InternalVisibilityRequestBase: d.record(i, 1)})
		})
		report["partition_counts"] = partitions
		if err != nil {
			// Preserve acknowledged progress without presenting partial seeding as success.
			report["status"] = "FAILED"
			report["error"] = err.Error()
			file, writeErr := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if writeErr != nil {
				return fmt.Errorf("%w; receipt: %v", err, writeErr)
			}
			encodeErr := json.NewEncoder(file).Encode(report)
			closeErr := file.Close()
			return fmt.Errorf("%w; receipt encode=%v close=%v", err, encodeErr, closeErr)
		}
	}
	if *mode == "mutate" {
		// Freeze page one before exactly one durable mutation, then resume cursor.
		d.concurrent = true
		events := []map[string]any{}
		for i := 0; i < d.count; i++ {
			hook := func() error {
				events = append(events, map[string]any{"round": i, "event": "first_page_received", "unix_nano": time.Now().UnixNano()})
				b := d.record(i, 2)
				b.WorkflowID += "-updated"
				if e := s.UpsertWorkflowExecution(ctx, &store.InternalUpsertWorkflowExecutionRequest{InternalVisibilityRequestBase: b}); e != nil {
					return e
				}
				events = append(events, map[string]any{"round": i, "event": "write_durably_acknowledged", "unix_nano": time.Now().UnixNano()})
				return nil
			}
			if _, e = checkPages(ctx, c.WorkflowService(), *ns, d, []int{7}, hook); e != nil {
				return e
			}
			events = append(events, map[string]any{"round": i, "event": "remaining_pages_verified", "unix_nano": time.Now().UnixNano()})
		}
		d.concurrent = false
		d.updated = 20
		report["mutation_events"] = events
		report["mutation_acknowledged_rounds"] = 20
		report["mutation_scope"] = "exactly one durable write between first and remaining public pages; no simultaneous-commit or snapshot claim"
	}
	var barrier func() error
	if *releaseFile != "" {
		used := false
		barrier = func() error {
			if used {
				return nil
			}
			used = true
			if err := json.NewEncoder(os.Stdout).Encode(map[string]any{"event": "VISIBILITY_FIRST_PAGE", "checkpoint": *checkpoint, "token": *releaseToken, "page_size": selectedSizes[0]}); err != nil {
				return err
			}
			return pageBarrier(ctx, *releaseFile, *releaseToken)
		}
	}
	verified, e := checkPages(ctx, c.WorkflowService(), *ns, d, selectedSizes, barrier)
	if e != nil {
		return e
	}
	report["verified"] = verified
	file, e := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	defer file.Close()
	return json.NewEncoder(file).Encode(report)
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
