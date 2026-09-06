package s3meter

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const casBodyLimit = 16384

// CASSelector is proof metadata, never credentials or application payloads.
type CASSelector struct {
	Path        string `json:"path"`
	Partition   string `json:"partition"`
	Node        string `json:"node"`
	Incarnation string `json:"incarnation"`
}
type CASReceipt struct {
	SuccessUnixNano      int64       `json:"success_unix_nano,omitempty"`
	AbortUnixNano        int64       `json:"abort_unix_nano,omitempty"`
	Schema               int         `json:"schema"`
	Selector             CASSelector `json:"selector"`
	State                string      `json:"state"`
	RequestSHA           string      `json:"request_sha256,omitempty"`
	IfMatch              string      `json:"if_match,omitempty"`
	ETag                 string      `json:"successful_etag,omitempty"`
	UpstreamStatus       int         `json:"upstream_status,omitempty"`
	HandlerAbortObserved bool        `json:"handler_abort_observed"`
	ReconciledGET        bool        `json:"reconciled_get"`
	ReadyGET             bool        `json:"ready_get"`
	Error                string      `json:"error,omitempty"`
	Sequence             int         `json:"sequence"`
}
type casLoss struct {
	mu       sync.Mutex
	receipt  CASReceipt
	deadline time.Time
	file     *os.File
	record   casRecord
}
type casRecord struct {
	Partition   string `json:"partition"`
	Node        string `json:"node"`
	Incarnation string `json:"incarnation"`
	State       string `json:"state"`
	Transition  string `json:"transition"`
	Generation  uint64 `json:"generation"`
}

func (p *Proxy) EnableCASLoss(path string) error {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	p.cas = &casLoss{file: f, receipt: CASReceipt{Schema: 1, State: "unarmed"}}
	return nil
}
func (p *Proxy) ArmCASLoss(s CASSelector) error {
	if p.cas == nil {
		return errors.New("CAS loss disabled")
	}
	if !strings.HasPrefix(s.Path, "/") || strings.ContainsAny(s.Path, "?#%\\\r\n") || len(s.Path) > 2048 || s.Partition == "" || s.Node == "" || s.Incarnation == "" || len(s.Node) > 256 || len(s.Incarnation) > 128 {
		return errors.New("invalid CAS selector")
	}
	suffix := "/" + hex.EncodeToString([]byte(s.Partition)) + ".json"
	if !strings.HasSuffix(s.Path, suffix) {
		return errors.New("directory path/partition mismatch")
	}
	c := p.cas
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.receipt.State != "unarmed" {
		return errors.New("CAS selector is one shot")
	}
	c.receipt.Selector = s
	c.receipt.State = "armed"
	c.deadline = time.Now().Add(120 * time.Second)
	return c.save()
}
func (c *casLoss) save() error {
	c.receipt.Sequence++
	if e := json.NewEncoder(c.file).Encode(c.receipt); e != nil {
		c.receipt.Error = "receipt_write"
		return e
	}
	if e := c.file.Sync(); e != nil {
		c.receipt.Error = "receipt_sync"
		return e
	}
	return nil
}
func (p *Proxy) CASLossSnapshot() *CASReceipt {
	if p.cas == nil {
		return nil
	}
	p.cas.mu.Lock()
	defer p.cas.mu.Unlock()
	v := p.cas.receipt
	return &v
}
func (c *casLoss) prepare(r *http.Request) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.receipt.State != "armed" || r.Method != "PUT" || r.URL.EscapedPath() != c.receipt.Selector.Path || r.URL.RawQuery != "" || r.Header.Get("If-Match") == "" {
		return false, nil
	}
	if time.Now().After(c.deadline) {
		c.receipt.State = "failed"
		c.receipt.Error = "arm_expired"
		_ = c.save()
		return false, nil
	}
	if len(r.Header.Get("If-Match")) > 256 || r.ContentLength < 0 || r.ContentLength > casBodyLimit || r.Header.Get("Content-Encoding") != "" {
		c.receipt.State = "failed"
		c.receipt.Error = "unsupported_directory_body_framing"
		_ = c.save()
		return false, nil
	}
	raw, e := io.ReadAll(io.LimitReader(r.Body, casBodyLimit+1))
	if e != nil {
		return false, e
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(raw))
	var record casRecord
	if json.Unmarshal(raw, &record) != nil {
		return false, nil
	}
	s := c.receipt.Selector
	if record.Partition != s.Partition || record.Node != s.Node || record.Incarnation != s.Incarnation || record.State != "opening" {
		return false, nil
	}
	if record.Generation == 0 || record.Transition == "" {
		return false, errors.New("invalid selected directory record")
	}
	c.record = record
	c.receipt.State = "selected"
	c.receipt.RequestSHA = fmt.Sprintf("%x", sha256.Sum256(raw))
	c.receipt.IfMatch = r.Header.Get("If-Match")
	return true, c.save()
}
func (c *casLoss) after(r *http.Request, response *http.Response, selected bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if selected {
		c.receipt.UpstreamStatus = response.StatusCode
		c.receipt.ETag = response.Header.Get("ETag")
		if response.StatusCode < 200 || response.StatusCode >= 300 || (c.receipt.ETag == "" || len(c.receipt.ETag) > 256) {
			c.receipt.Error = "selected_CAS_not_successful"
			return c.save()
		}
		c.receipt.SuccessUnixNano = time.Now().UnixNano()
		c.receipt.State = "successful_response_dropped"
		// Sync the real upstream success before closing the downstream connection.
		if e := c.save(); e != nil {
			return e
		}
		_ = response.Body.Close()
		panic(http.ErrAbortHandler)
	}
	if c.receipt.State != "successful_response_dropped" || r.Method != "GET" || r.URL.EscapedPath() != c.receipt.Selector.Path || r.URL.RawQuery != "" || response.StatusCode != 200 {
		return nil
	}
	if response.ContentLength > casBodyLimit {
		return errors.New("directory GET exceeds proof bound")
	}
	raw, e := io.ReadAll(io.LimitReader(response.Body, casBodyLimit+1))
	_ = response.Body.Close()
	response.Body = io.NopCloser(bytes.NewReader(raw))
	if e != nil || len(raw) > casBodyLimit {
		return errors.New("invalid directory GET body")
	}
	changed := false
	digest := fmt.Sprintf("%x", sha256.Sum256(raw))
	if digest == c.receipt.RequestSHA {
		changed = changed || !c.receipt.ReconciledGET
		c.receipt.ReconciledGET = true
	}
	var record casRecord
	if json.Unmarshal(raw, &record) == nil && record.Partition == c.record.Partition && record.Node == c.record.Node && record.Incarnation == c.record.Incarnation && record.Transition == c.record.Transition && record.Generation == c.record.Generation && record.State == "ready" {
		changed = changed || !c.receipt.ReadyGET
		c.receipt.ReadyGET = true
	}
	if changed {
		return c.save()
	}
	return nil
}

// ValidateCASLossJournal checks the bounded, synced event sequence independently
// of the running proxy. This proves the observed cut/reconciliation only, not
// the application's final state or dispatch on its new owner.
func ValidateCASLossJournal(path string) (CASReceipt, error) {
	file, err := os.Open(path)
	if err != nil {
		return CASReceipt{}, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 65537))
	if err != nil {
		return CASReceipt{}, err
	}
	if len(raw) > 65536 || len(raw) == 0 || raw[len(raw)-1] != '\n' {
		return CASReceipt{}, errors.New("invalid CAS journal framing")
	}
	lines := bytes.Split(bytes.TrimSuffix(raw, []byte{'\n'}), []byte{'\n'})
	if len(lines) < 5 || len(lines) > 6 {
		return CASReceipt{}, errors.New("invalid CAS journal length")
	}
	var previous CASReceipt
	for i, line := range lines {
		var r CASReceipt
		d := json.NewDecoder(bytes.NewReader(line))
		d.DisallowUnknownFields()
		if err = d.Decode(&r); err != nil {
			return r, err
		}
		canonical, _ := json.Marshal(r)
		if !bytes.Equal(canonical, line) || d.Decode(&struct{}{}) != io.EOF || r.Schema != 1 || r.Sequence != i+1 || r.Error != "" {
			return r, errors.New("invalid CAS receipt")
		}
		if i > 0 && (r.Selector != previous.Selector || previous.ReconciledGET && !r.ReconciledGET || previous.ReadyGET && !r.ReadyGET) {
			return r, errors.New("CAS receipt regression")
		}
		if i == 0 && r.State != "armed" || i == 1 && r.State != "selected" || i >= 2 && r.State != "successful_response_dropped" {
			return r, errors.New("invalid CAS state sequence")
		}
		if i >= 1 {
			if hash, e := hex.DecodeString(r.RequestSHA); e != nil || len(hash) != 32 || r.IfMatch == "" {
				return r, errors.New("missing CAS request identity")
			}
		}
		if i >= 2 && (r.UpstreamStatus < 200 || r.UpstreamStatus >= 300 || r.ETag == "") {
			return r, errors.New("no actual upstream success")
		}
		if i > 1 && (r.RequestSHA != previous.RequestSHA || r.IfMatch != previous.IfMatch) {
			return r, errors.New("CAS request identity changed")
		}
		if i > 2 && (r.ETag != previous.ETag || r.UpstreamStatus != previous.UpstreamStatus) {
			return r, errors.New("CAS outcome changed")
		}
		previous = r
	}
	if !previous.HandlerAbortObserved || !previous.ReconciledGET || !previous.ReadyGET {
		return previous, errors.New("CAS reconciliation/READY missing")
	}
	return previous, nil
}

func (c *casLoss) observeAbort() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.receipt.State == "successful_response_dropped" && !c.receipt.HandlerAbortObserved {
		c.receipt.AbortUnixNano = time.Now().UnixNano()
		c.receipt.HandlerAbortObserved = true
		_ = c.save()
	}
}
