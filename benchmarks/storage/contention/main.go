// Bounded real-backend research; not the production coordinator or a DST model.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
	regS3 "github.com/0x63616c/xenon/internal/registry/s3"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
)

type config struct {
	DisableKeepAlives bool   `json:"disable_keep_alives"`
	Schema            int    `json:"schema"`
	Endpoint          string `json:"endpoint"`
	Bucket            string `json:"bucket"`
	Partitions        []int  `json:"partition_counts"`
	Contenders        []int  `json:"contenders"`
	Updates           int    `json:"updates_per_contender"`
	Renewals          int    `json:"renewals"`
	RenewalMS         int    `json:"renewal_interval_ms"`
	RetryMS           int    `json:"retry_cap_ms"`
	Deadline          int    `json:"case_deadline_seconds"`
	Base              uint64 `json:"counter_base"`
}
type meter struct {
	base           http.RoundTripper
	mu             sync.Mutex
	Counts         map[string]int
	Sent, Received int64
}
type bodyMeter struct {
	io.ReadCloser
	m *meter
}

func (b *bodyMeter) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	b.m.mu.Lock()
	b.m.Received += int64(n)
	b.m.mu.Unlock()
	return n, e
}
func (m *meter) RoundTrip(r *http.Request) (*http.Response, error) {
	response, e := m.base.RoundTrip(r)
	m.mu.Lock()
	status := "transport_error"
	if response != nil {
		status = fmt.Sprint(response.StatusCode)
	}
	m.Counts[r.Method+" "+status]++
	if r.ContentLength > 0 {
		m.Sent += r.ContentLength
	}
	m.mu.Unlock()
	if response != nil && response.Body != nil {
		response.Body = &bodyMeter{response.Body, m}
	}
	return response, e
}
func (m *meter) snapshot() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	counts := map[string]int{}
	for k, v := range m.Counts {
		counts[k] = v
	}
	return map[string]any{"responses": counts, "declared_request_body_bytes": m.Sent, "response_body_bytes": m.Received}
}

type event struct {
	Reconciliation []string              `json:"reconciliation,omitempty"`
	Worker         int                   `json:"worker"`
	Update         int                   `json:"update"`
	Attempt        int                   `json:"attempt"`
	Start          int64                 `json:"start_ns"`
	End            int64                 `json:"end_ns"`
	Expected       registry.Version      `json:"expected"`
	Version        registry.Version      `json:"version,omitempty"`
	Transition     identity.TransitionID `json:"transition"`
	Digest         string                `json:"digest"`
	Outcome        string                `json:"outcome"`
}
type summary struct {
	Partitions, Contenders, Updates, Renewals, Attempts, Conflicts, Successes, EnvelopeBytes int
	DurationNS, MaxRenewalGapNS                                                              int64
	RenewalCompletionNS                                                                      []int64
	Traffic                                                                                  map[string]any
	FinalVerified                                                                            bool
	Error                                                                                    string `json:",omitempty"`
}

// Include wrapped SDK/transport causes; the registry intentionally keeps its
// public Error short, which is insufficient for research failure provenance.
func detail(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	if multi, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range multi.Unwrap() {
			text += " | " + detail(cause)
		}
	} else if cause := errors.Unwrap(err); cause != nil {
		text += " | " + detail(cause)
	}
	return text
}
func read(ctx context.Context, s registry.Store, key registry.Key) (registry.Record, Control, error) {
	r, e := s.Read(ctx, key)
	if e != nil {
		return r, Control{}, e
	}
	env, e := registry.Decode(key, r)
	if e != nil {
		return r, Control{}, e
	}
	var c Control
	e = json.Unmarshal(env.Body, &c)
	return r, c, e
}
func write(key registry.Key, version registry.Version, id uint64, c Control) (registry.Write, error) {
	b, e := json.Marshal(c)
	if e != nil {
		return registry.Write{}, e
	}
	return registry.NewWrite(key, version, identity.TransitionID(fmt.Sprintf("trn_%022d", id)), b)
}
func pause(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
func runCase(c config, partitions, contenders int, out string) (summary, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.Deadline)*time.Second)
	defer cancel()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableKeepAlives = c.DisableKeepAlives
	defer transport.CloseIdleConnections()
	m := &meter{base: transport, Counts: map[string]int{}}
	client := sdk.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("xenon-local", "xenon-local-test-only", ""), HTTPClient: &http.Client{Transport: m}, RetryMaxAttempts: 1}, func(o *sdk.Options) { o.BaseEndpoint = aws.String(c.Endpoint); o.UsePathStyle = true })
	s, e := regS3.New(client, c.Bucket, fmt.Sprintf("p%d-n%d", partitions, contenders))
	if e != nil {
		return summary{}, e
	}
	key := registry.Key("control")
	initial := fixture(partitions, shapeConfig{100, c.Base})
	w, e := write(key, "", 1, initial)
	if e != nil {
		return summary{}, e
	}
	record, e := s.Create(ctx, key, w)
	if e != nil {
		return summary{}, e
	}
	result := summary{Partitions: partitions, Contenders: contenders, Updates: c.Updates, Renewals: c.Renewals, EnvelopeBytes: len(record.Body)}
	start := time.Now()
	var ids atomic.Uint64
	ids.Store(1)
	var wg, reads sync.WaitGroup
	reads.Add(contenders + 1)
	gate := make(chan struct{})
	go func() { reads.Wait(); close(gate) }()
	var mu sync.Mutex
	var events []event
	completedReceipts := make(map[int]Receipt)
	var firstErr error
	fail := func(e error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = e
			cancel()
		}
		mu.Unlock()
	}
	for worker := 0; worker <= contenders; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			updates := c.Updates
			if worker == 0 {
				updates = c.Renewals
			}
			for update := 0; update < updates; update++ {
				if worker == 0 && update > 0 {
					if e := pause(ctx, time.Duration(c.RenewalMS)*time.Millisecond); e != nil {
						return
					}
				}
				for attempt := 0; ; attempt++ {
					began := time.Since(start).Nanoseconds()
					r, control, e := read(ctx, s, key)
					if update == 0 && attempt == 0 {
						reads.Done()
						select {
						case <-gate:
						case <-ctx.Done():
							return
						}
					}
					if e != nil {
						fail(fmt.Errorf("worker %d read: %w", worker, e))
						return
					}
					originalReceipt := control.Receipts[worker]
					if worker == 0 {
						control.Coordinator.RenewalSequence++
					} else {
						id := identity.PartitionID(fmt.Sprintf("prt_%022d", worker))
						p := control.Partitions[id]
						p.Ready.Generation++
						control.Partitions[id] = p
					}
					transition := ids.Add(1)
					intent := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%d", key, worker, update)))
					ownReceipt := Receipt{identity.TransitionID(fmt.Sprintf("trn_%022d", transition)), fmt.Sprintf("%x", intent)}
					control.Receipts[worker] = ownReceipt
					w, e := write(key, r.Version, transition, control)
					if e != nil {
						fail(e)
						return
					}
					updated, reconciliation, e := publish(ctx, s, key, r.Version, w, worker, originalReceipt, ownReceipt)
					ev := event{Reconciliation: reconciliation, Worker: worker, Update: update, Attempt: attempt, Start: began, End: time.Since(start).Nanoseconds(), Expected: r.Version, Version: updated.Version, Transition: w.Transition, Digest: fmt.Sprintf("%x", w.Digest), Outcome: "success"}
					var conflict *registry.Conflict
					if e != nil {
						ev.Outcome = "unexpected: " + detail(e)
						if errors.As(e, &conflict) {
							ev.Outcome = "conflict"
						}
					}
					mu.Lock()
					events = append(events, ev)
					if e == nil {
						completedReceipts[worker] = ownReceipt
					}
					mu.Unlock()
					if e == nil {
						break
					}
					if !errors.As(e, &conflict) {
						fail(fmt.Errorf("worker %d update %d: %w", worker, update, e))
						return
					}
					// Recorded deterministic stagger; real time belongs only to this backend probe.
					if e := pause(ctx, time.Duration(1+(worker+attempt)%c.RetryMS)*time.Millisecond); e != nil {
						return
					}
				}
			}
		}(worker)
	}
	wg.Wait()
	result.DurationNS = time.Since(start).Nanoseconds()
	if firstErr == nil && ctx.Err() != nil {
		firstErr = ctx.Err()
	}
	if firstErr == nil {
		_, actual, e := read(ctx, s, key)
		if e != nil {
			firstErr = e
		} else {
			want := fixture(partitions, shapeConfig{100, c.Base})
			want.Coordinator.RenewalSequence += uint64(c.Renewals)
			for worker, receipt := range completedReceipts {
				want.Receipts[worker] = receipt
			}
			for worker := 1; worker <= contenders; worker++ {
				id := identity.PartitionID(fmt.Sprintf("prt_%022d", worker))
				p := want.Partitions[id]
				p.Ready.Generation += uint64(c.Updates)
				want.Partitions[id] = p
			}
			if !reflect.DeepEqual(actual, want) {
				firstErr = errors.New("final whole-record state differs: lost update or unintended field change")
			} else {
				result.FinalVerified = true
			}
		}
	}
	sort.Slice(events, func(i, j int) bool { return events[i].End < events[j].End })
	f, e := os.Create(fmt.Sprintf("%s/p%d-n%d.jsonl", out, partitions, contenders))
	if e != nil {
		return result, e
	}
	enc := json.NewEncoder(f)
	for _, ev := range events {
		if e := enc.Encode(ev); e != nil {
			f.Close()
			return result, e
		}
		result.Attempts++
		switch ev.Outcome {
		case "success":
			result.Successes++
			if ev.Worker == 0 {
				result.RenewalCompletionNS = append(result.RenewalCompletionNS, ev.End)
			}
		case "conflict":
			result.Conflicts++
		}
	}
	if e := f.Close(); e != nil {
		return result, e
	}
	prev := int64(0)
	for _, t := range result.RenewalCompletionNS {
		if t-prev > result.MaxRenewalGapNS {
			result.MaxRenewalGapNS = t - prev
		}
		prev = t
	}
	result.Traffic = m.snapshot()
	if firstErr == nil && (result.Successes != contenders*c.Updates+c.Renewals || len(result.RenewalCompletionNS) != c.Renewals) {
		firstErr = errors.New("missing success or renewal")
	}
	if firstErr != nil {
		result.Error = detail(firstErr)
	}
	return result, firstErr
}
func run() error {
	if len(os.Args) != 3 {
		return errors.New("usage: contention config.json evidence-dir")
	}
	raw, e := os.ReadFile(os.Args[1])
	if e != nil {
		return e
	}
	var c config
	if e = json.Unmarshal(raw, &c); e != nil {
		return e
	}
	if c.Schema != 1 || c.Updates <= 0 || c.Renewals <= 0 || c.RetryMS <= 0 || c.Deadline <= 0 || len(c.Partitions) == 0 || len(c.Contenders) == 0 {
		return errors.New("invalid config")
	}
	for _, p := range c.Partitions {
		for _, n := range c.Contenders {
			if p < n || n < 1 || n > 100 {
				return errors.New("invalid topology")
			}
		}
	}
	if e = os.MkdirAll(os.Args[2], 0755); e != nil {
		return e
	}
	client := sdk.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("xenon-local", "xenon-local-test-only", ""), RetryMaxAttempts: 1}, func(o *sdk.Options) { o.BaseEndpoint = aws.String(c.Endpoint); o.UsePathStyle = true })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, e = client.CreateBucket(ctx, &sdk.CreateBucketInput{Bucket: aws.String(c.Bucket)}); e != nil {
		return e
	}
	if e := receiptCases(c, os.Args[2]); e != nil {
		return e
	}
	f, e := os.Create(os.Args[2] + "/summary.jsonl")
	if e != nil {
		return e
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, p := range c.Partitions {
		for _, n := range c.Contenders {
			result, e := runCase(c, p, n, os.Args[2])
			if err := enc.Encode(result); err != nil {
				return err
			}
			fmt.Printf("partitions=%d contenders=%d attempts=%d conflicts=%d renewal_max_gap_ms=%.1f verified=%v\n", p, n, result.Attempts, result.Conflicts, float64(result.MaxRenewalGapNS)/1e6, result.FinalVerified)
			if e != nil {
				return e
			}
		}
	}
	return nil
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
