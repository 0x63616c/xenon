package s3meter

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestCASLossHTTP(t *testing.T) {
	for _, mode := range []string{"success", "signed_query", "precondition_failure", "wrong_incarnation"} {
		t.Run(mode, func(t *testing.T) {
			var mu sync.Mutex
			stored := []byte(`{}`)
			etag := `"old"`
			puts := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				uri := "/bucket/metadata/owners/686973746f72792d30.json"
				if mode == "signed_query" {
					uri += "?x-id=" + map[string]string{"GET": "GetObject", "PUT": "PutObject"}[r.Method]
				}
				if r.URL.RequestURI() != uri || r.Header.Get("Authorization") != "signed-control" {
					t.Error("request path/signature modified")
				}
				if r.Method == "PUT" {
					puts++
					raw, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
					}
					if r.Header.Get("If-Match") != etag || mode == "precondition_failure" {
						w.WriteHeader(412)
						return
					}
					stored = raw
					etag = `"new"`
				}
				w.Header().Set("ETag", etag)
				if r.Method == "GET" {
					_, _ = w.Write(stored)
				} else {
					w.WriteHeader(200)
				}
			}))
			defer upstream.Close()
			p, err := New(upstream.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			receiptPath := filepath.Join(t.TempDir(), "receipt.jsonl")
			if err = p.EnableCASLoss(receiptPath); err != nil {
				t.Fatal(err)
			}
			proxy := httptest.NewServer(p)
			defer proxy.Close()
			selector := CASSelector{Path: "/bucket/metadata/owners/686973746f72792d30.json", Partition: "history-0", Node: "c", Incarnation: "fresh"}
			if err = p.ArmCASLoss(selector); err != nil {
				t.Fatal(err)
			}
			if p.ArmCASLoss(selector) == nil {
				t.Fatal("rearmed")
			}
			client := &http.Client{Timeout: time.Second}
			record := casRecord{Partition: "history-0", Node: "c", Incarnation: "fresh", State: "opening", Transition: "transition", Generation: 2}
			if mode == "wrong_incarnation" {
				record.Incarnation = "old"
			}
			send := func(method string, data []byte, match string) (*http.Response, error) {
				r, _ := http.NewRequest(method, proxy.URL+selector.Path+func() string {
					if mode == "signed_query" {
						return "?x-id=" + map[string]string{"GET": "GetObject", "PUT": "PutObject"}[method]
					}
					return ""
				}(), bytes.NewReader(data))
				r.Header.Set("Authorization", "signed-control")
				if match != "" {
					r.Header.Set("If-Match", match)
				}
				return client.Do(r)
			}
			raw, _ := json.Marshal(record)
			response, err := send("PUT", raw, `"old"`)
			if mode != "success" && mode != "signed_query" {
				if err != nil {
					t.Fatal(err)
				}
				_ = response.Body.Close()
				receipt := p.CASLossSnapshot()
				if receipt.State == "successful_response_dropped" {
					t.Fatal("false successful cut")
				}
				return
			}
			if err == nil || response != nil {
				t.Fatal("successful S3 response reached client")
			}
			mu.Lock()
			if !bytes.Equal(raw, stored) || puts != 1 {
				t.Fatal("no exact actual write")
			}
			mu.Unlock()
			response, err = send("GET", nil, "")
			if err != nil {
				t.Fatal(err)
			}
			got, _ := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if !bytes.Equal(got, raw) {
				t.Fatal("GET altered")
			}
			record.State = "ready"
			raw, _ = json.Marshal(record)
			response, err = send("PUT", raw, `"new"`)
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			response, err = send("GET", nil, "")
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			receipt := p.CASLossSnapshot()
			if receipt.Error != "" || receipt.State != "successful_response_dropped" || !receipt.ReconciledGET || !receipt.ReadyGET || receipt.UpstreamStatus != 200 || receipt.Sequence != 6 {
				t.Fatalf("missing exact reconciliation: %+v", receipt)
			}
			if _, err := ValidateCASLossJournal(receiptPath); err != nil {
				t.Fatal(err)
			}
			if p.Snapshot().Aborted != 1 {
				t.Fatal("missing actual aborted HTTP handler")
			}
		})
	}
}
