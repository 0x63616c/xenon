// layout measures a fixed logical workload through the production native seam.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http/httptest"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/0x63616c/xenon/internal/identity"
	p "github.com/0x63616c/xenon/internal/partitions"
	engine "github.com/0x63616c/xenon/internal/partitions/slatedb"
	"github.com/0x63616c/xenon/internal/proof/s3meter"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type config struct {
	LogicalShards      int `json:"logical_shards"`
	Rounds             int `json:"rounds"`
	PayloadBytes       int `json:"payload_bytes"`
	IdleMS             int `json:"idle_ms"`
	CaseTimeoutSeconds int `json:"case_timeout_seconds"`
}
type observation struct {
	Shard        int   `json:"shard"`
	Round        int   `json:"round"`
	EndToEndNS   int64 `json:"end_to_end_ns"`
	OwnDurableNS int64 `json:"own_durable_ns"`
}

func emit(v any) {
	if err := json.NewEncoder(os.Stdout).Encode(v); err != nil {
		panic(err)
	}
}
func main() {
	if err := run(); err != nil {
		emit(map[string]any{"event": "failure", "error": err.Error()})
		os.Exit(1)
	}
}
func run() (result error) {
	cfgPath := flag.String("config", "benchmarks/storage/layout/case.json", "committed case")
	physical := flag.Int("writers", 0, "physical database count")
	flag.Parse()
	raw, err := os.ReadFile(*cfgPath)
	if err != nil {
		return err
	}
	var cfg config
	if err = json.Unmarshal(raw, &cfg); err != nil {
		return err
	}
	if cfg.LogicalShards != 16 || cfg.Rounds != 16 || cfg.PayloadBytes != 256 || cfg.IdleMS != 2000 || cfg.CaseTimeoutSeconds != 120 || (*physical != 1 && *physical != 4 && *physical != 16) {
		return errors.New("unregistered bounded workload")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.CaseTimeoutSeconds)*time.Second)
	defer cancel()
	meter, err := s3meter.New(os.Getenv("AWS_ENDPOINT"))
	if err != nil {
		return err
	}
	defer meter.Close()
	proxy := httptest.NewServer(meter)
	defer proxy.Close()
	if err = os.Setenv("AWS_ENDPOINT", proxy.URL); err != nil {
		return err
	}
	client := s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("xenon-local", "xenon-local-test-only", ""), RetryMaxAttempts: 1}, func(o *s3.Options) { o.BaseEndpoint = aws.String(proxy.URL); o.UsePathStyle = true })
	bucket := os.Getenv("XENON_LAYOUT_BUCKET")
	if bucket == "" {
		return errors.New("missing isolated bucket")
	}
	if _, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return err
	}
	e, err := engine.New("s3://" + bucket)
	if err != nil {
		return err
	}
	writers := make([]p.Writer, *physical)
	defer func() {
		for _, w := range writers {
			if w != nil {
				err := w.Close(context.Background())
				if err != nil && !errors.Is(err, p.ErrFenced) {
					result = errors.Join(result, err)
				}
			}
		}
	}()
	phase := func(name string) {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		emit(map[string]any{"event": "phase", "phase": name, "at_unix_ns": time.Now().UnixNano(), "go_heap_alloc_bytes": mem.HeapAlloc, "go_sys_bytes": mem.Sys, "meter": meter.Snapshot()})
	}
	open := func(i int, inc int) (p.Writer, error) {
		return e.Open(ctx, p.OpenRequest{Path: fmt.Sprintf("db-%02d", i), Partition: identity.PartitionID(fmt.Sprintf("prt_%022d", i)), AssignmentRevision: uint64(inc), Reservation: identity.TransitionID(fmt.Sprintf("trn_%022d", inc)), Incarnation: identity.IncarnationID(fmt.Sprintf("inc_%022d", inc)), Generation: uint64(inc)})
	}
	phase("baseline")
	select {
	case <-time.After(time.Duration(cfg.IdleMS) * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	phase("open")
	opened := time.Now()
	openNS := make([]int64, *physical)
	for i := range writers {
		start := time.Now()
		writers[i], err = open(i, 1)
		if err != nil {
			return err
		}
		openNS[i] = time.Since(start).Nanoseconds()
	}
	emit(map[string]any{"event": "open_result", "total_ns": time.Since(opened).Nanoseconds(), "per_writer_ns": openNS})
	phase("idle")
	select {
	case <-time.After(time.Duration(cfg.IdleMS) * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	phase("loaded")
	started := time.Now()
	var wg sync.WaitGroup
	results := make(chan []observation, cfg.LogicalShards)
	errs := make(chan error, cfg.LogicalShards)
	for shard := 0; shard < cfg.LogicalShards; shard++ {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			rows, err := work(ctx, writers[shard%*physical], cfg, shard)
			results <- rows
			errs <- err
		}(shard)
	}
	wg.Wait()
	close(results)
	close(errs)
	elapsed := time.Since(started)
	for e := range errs {
		if e != nil {
			return e
		}
	}
	observations := []observation{}
	for rows := range results {
		observations = append(observations, rows...)
	}
	if len(observations) != cfg.LogicalShards*cfg.Rounds {
		return errors.New("incomplete workload")
	}
	phase("loaded_done")
	emit(map[string]any{"event": "workload_result", "elapsed_ns": elapsed.Nanoseconds(), "operations": observations})
	// Quiescent native writer takeover; no registry/controller or transport delay.
	phase("takeover")
	started = time.Now()
	takeoverNS := make([]int64, *physical)
	takeoverOpenNS := make([]int64, *physical)
	for i, old := range writers {
		start := time.Now()
		next, err := open(i, 2)
		takeoverOpenNS[i] = time.Since(start).Nanoseconds()
		if err != nil {
			return err
		}
		writers[i] = next
		if err = old.Close(ctx); err != nil && !errors.Is(err, p.ErrFenced) {
			return err
		}
		if err = verify(ctx, next, cfg, i, *physical); err != nil {
			return err
		}
		takeoverNS[i] = time.Since(start).Nanoseconds()
	}
	emit(map[string]any{"event": "takeover_result", "total_ns": time.Since(started).Nanoseconds(), "per_writer_with_verification_ns": takeoverNS, "per_writer_native_open_ns": takeoverOpenNS})
	phase("close")
	for i, w := range writers {
		if err = w.Close(ctx); err != nil {
			return err
		}
		writers[i] = nil
	}
	phase("reopen")
	started = time.Now()
	reopenNS := make([]int64, *physical)
	reopenOpenNS := make([]int64, *physical)
	for i := range writers {
		start := time.Now()
		writers[i], err = open(i, 3)
		reopenOpenNS[i] = time.Since(start).Nanoseconds()
		if err != nil {
			return err
		}
		if err = verify(ctx, writers[i], cfg, i, *physical); err != nil {
			return err
		}
		reopenNS[i] = time.Since(start).Nanoseconds()
	}
	emit(map[string]any{"event": "reopen_result", "total_ns": time.Since(started).Nanoseconds(), "per_writer_with_verification_ns": reopenNS, "per_writer_native_open_ns": reopenOpenNS})
	phase("recovered_idle")
	select {
	case <-time.After(time.Duration(cfg.IdleMS) * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	phase("final_close")
	for i, w := range writers {
		if err = w.Close(ctx); err != nil {
			return err
		}
		writers[i] = nil
	}
	phase("done")
	emit(map[string]any{"event": "passed", "logical_shards": cfg.LogicalShards, "physical_writers": *physical, "operations": len(observations)})
	return nil
}
func stateKey(shard int) []byte { return []byte(fmt.Sprintf("shard/%02d/state", shard)) }
func outcomeKey(shard, round int) []byte {
	return []byte(fmt.Sprintf("shard/%02d/outcome/%02d", shard, round))
}
func state(round, size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = 'x'
	}
	copy(b, fmt.Sprintf("%04d", round))
	return b
}
func work(ctx context.Context, w p.Writer, cfg config, shard int) ([]observation, error) {
	rows := make([]observation, 0, cfg.Rounds)
	for round := 1; round <= cfg.Rounds; round++ {
		start := time.Now()
		tx, err := w.Begin(ctx)
		if err != nil {
			return rows, err
		}
		value, err := tx.Get(ctx, stateKey(shard))
		if err != nil {
			_ = tx.Abort()
			return rows, err
		}
		expected := ""
		if round > 1 {
			expected = string(state(round-1, cfg.PayloadBytes))
		}
		if string(value) != expected {
			_ = tx.Abort()
			return rows, errors.New("conditional state mismatch")
		}
		if err = tx.Put(stateKey(shard), state(round, cfg.PayloadBytes)); err == nil {
			err = tx.Put(outcomeKey(shard, round), []byte(fmt.Sprintf("shard=%d;round=%d;result=accepted", shard, round)))
		}
		if err != nil {
			_ = tx.Abort()
			return rows, err
		}
		commitStart := time.Now()
		receipt, err := tx.Commit(ctx)
		if err != nil {
			return rows, err
		}
		if err = w.AwaitDurable(ctx, receipt); err != nil {
			return rows, err
		}
		done := time.Now()
		rows = append(rows, observation{shard, round, done.Sub(start).Nanoseconds(), done.Sub(commitStart).Nanoseconds()})
	}
	return rows, nil
}
func verify(ctx context.Context, w p.Writer, cfg config, physical, total int) error {
	keys := [][]byte{}
	expected := []string{}
	for shard := 0; shard < cfg.LogicalShards; shard++ {
		if shard%total != physical {
			continue
		}
		keys = append(keys, stateKey(shard))
		expected = append(expected, string(state(cfg.Rounds, cfg.PayloadBytes)))
		for round := 1; round <= cfg.Rounds; round++ {
			keys = append(keys, outcomeKey(shard, round))
			expected = append(expected, fmt.Sprintf("shard=%d;round=%d;result=accepted", shard, round))
		}
	}
	read, err := w.ReadDurable(ctx, p.ReadRequest{Keys: keys})
	if err != nil {
		return err
	}
	if len(read.Entries) != len(expected) {
		return errors.New("recovered entry count mismatch")
	}
	for i, v := range read.Entries {
		if string(v.Key) != string(keys[i]) || string(v.Value) != expected[i] {
			return fmt.Errorf("acknowledged state/outcome recovery mismatch at %q", keys[i])
		}
	}
	return nil
}
