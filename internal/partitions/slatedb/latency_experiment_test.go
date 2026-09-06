package slatedb

import (
	"context"
	"encoding/json"
	"fmt"
	p "github.com/0x63616c/xenon/internal/partitions"
	"os"
	"path/filepath"
	native "slatedb.io/slatedb-go/uniffi"
	"sync"
	"testing"
	"time"
)

// Opt-in experiment: timings are observations, never machine-dependent assertions.
func TestNativeAdmissionLatencyExperiment(t *testing.T) {
	output := os.Getenv("XENON_LATENCY_OUTPUT")
	if output == "" {
		t.Skip("run scripts/native-admission-latency.py")
	}
	var config struct {
		Concurrency []int    `json:"concurrency"`
		Operations  int      `json:"operations"`
		Flush       []string `json:"flush_intervals"`
	}
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "test", "scenarios", "native-latency", "case.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	store, err := native.ObjectStoreResolve(os.Getenv("XENON_ENGINE_STORE"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Destroy()
	type sample struct {
		Admission, Begin, Commit, Durable, Total float64
		Error                                    string `json:",omitempty"`
	}
	type result struct {
		Concurrency int
		Flush       string
		Seconds     float64
		Samples     []sample
	}
	results := []result{}
	for _, flush := range config.Flush {
		for _, concurrency := range config.Concurrency {
			builder := native.NewDbBuilder(fmt.Sprintf("latency/%s/%d", flush, concurrency), store)
			if flush != "default" {
				settings := native.SettingsDefault()
				if err = settings.Set("flush_interval", fmt.Sprintf("%q", flush)); err != nil {
					t.Fatal(err)
				}
				err = builder.WithSettings(settings)
				settings.Destroy()
				if err != nil {
					t.Fatal(err)
				}
			}
			db, err := builder.Build()
			builder.Destroy()
			if err != nil {
				t.Fatal(err)
			}
			w := newWriter(db)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			samples := make([]sample, config.Operations)
			start := make(chan struct{})
			var wg sync.WaitGroup
			wall := time.Now()
			for worker := 0; worker < concurrency; worker++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					<-start
					for index := worker; index < config.Operations; index += concurrency {
						var s sample
						began := time.Now()
						op, e := w.BeginOperation(ctx)
						admitted := time.Now()
						s.Admission = admitted.Sub(began).Seconds()
						if e == nil {
							tx, be := op.Begin(ctx)
							s.Begin = time.Since(admitted).Seconds()
							e = be
							if e == nil {
								e = tx.Put([]byte(fmt.Sprintf("key-%03d", index)), []byte("durable-value"))
								if e == nil {
									at := time.Now()
									receipt, ce := tx.Commit(ctx)
									s.Commit = time.Since(at).Seconds()
									e = ce
									if e == nil {
										at = time.Now()
										e = w.AwaitDurable(ctx, receipt)
										s.Durable = time.Since(at).Seconds()
									}
								} else {
									_ = tx.Abort()
								}
							}
							op.Release()
						}
						s.Total = time.Since(began).Seconds()
						if e != nil {
							s.Error = e.Error()
						}
						samples[index] = s
					}
				}(worker)
			}
			close(start)
			wg.Wait()
			elapsed := time.Since(wall).Seconds()
			for _, s := range samples {
				if s.Error != "" {
					t.Error(s.Error)
				}
			}
			keys := make([][]byte, config.Operations)
			for i := range keys {
				keys[i] = []byte(fmt.Sprintf("key-%03d", i))
			}
			read, err := w.ReadDurable(ctx, p.ReadRequest{Keys: keys})
			if err != nil {
				t.Error(err)
			} else {
				if len(read.Entries) != len(keys) {
					t.Error("missing writes")
				}
				for _, entry := range read.Entries {
					if string(entry.Value) != "durable-value" {
						t.Error("incorrect durable value")
					}
				}
			}
			if err = w.Close(ctx); err != nil {
				t.Error(err)
			}
			cancel()
			results = append(results, result{concurrency, flush, elapsed, samples})
		}
	}
	data, err = json.MarshalIndent(results, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(output, data, 0600); err != nil {
		t.Fatal(err)
	}
}
