// Package s3meter measures local proof traffic. It is not an application store.
package s3meter

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"sync"
	"time"
)

type Counts struct {
	Attempts            uint64 `json:"attempts"`
	Finished            uint64 `json:"finished"`
	Completed           uint64 `json:"completed"`
	Canceled            uint64 `json:"canceled"`
	Aborted             uint64 `json:"aborted"`
	TransportErrors     uint64 `json:"transport_errors"`
	RequestReadErrors   uint64 `json:"request_read_errors"`
	ResponseReadErrors  uint64 `json:"response_read_errors"`
	ResponseWriteErrors uint64 `json:"response_write_errors"`
	RequestBodyBytes    uint64 `json:"request_body_bytes_read"`
	ResponseBodyBytes   uint64 `json:"response_body_bytes_written"`
}
type Report struct {
	CASLoss *CASReceipt `json:"cas_loss,omitempty"`
	Schema  int         `json:"schema"`
	Counts
	Inflight uint64            `json:"inflight"`
	Methods  map[string]uint64 `json:"methods"`
	Status   map[int]uint64    `json:"status"`
}
type Proxy struct {
	cas       *casLoss
	mu        sync.Mutex
	counts    Counts
	methods   map[string]uint64
	status    map[int]uint64
	proxy     *httputil.ReverseProxy
	transport *http.Transport
}

// LoopbackAddress accepts numeric loopback addresses only, never DNS or wildcards.
func LoopbackAddress(address string) error {
	host, port, e := net.SplitHostPort(address)
	if e != nil {
		return e
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() || port == "" {
		return errors.New("numeric loopback address required")
	}
	n, e := strconv.Atoi(port)
	if e != nil || n < 0 || n > 65535 {
		return errors.New("numeric port out of range")
	}
	return nil
}
func New(target string) (*Proxy, error) {
	u, e := url.Parse(target)
	if e != nil {
		return nil, e
	}
	if u.Scheme != "http" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("target must be a bare loopback HTTP origin")
	}
	if e = LoopbackAddress(u.Host); e != nil {
		return nil, e
	}
	p := &Proxy{methods: map[string]uint64{}, status: map[int]uint64{}}
	// No environment proxy, redirects, DNS, or caller-controlled destination.
	p.transport = &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", u.Host)
	}, MaxIdleConns: 64, MaxIdleConnsPerHost: 64, IdleConnTimeout: 30 * time.Second, ResponseHeaderTimeout: 30 * time.Second, DisableCompression: true}
	p.proxy = &httputil.ReverseProxy{ErrorLog: log.New(io.Discard, "", 0), Director: func(r *http.Request) { r.URL.Scheme = "http"; r.URL.Host = u.Host; r.Header.Del("X-Forwarded-For") }, Transport: roundTripper{p.transport, p}, ErrorHandler: func(w http.ResponseWriter, r *http.Request, _ error) {
		s := r.Context().Value(stateKey{}).(*state)
		s.transportErrors++
		w.WriteHeader(http.StatusBadGateway)
	}}
	return p, nil
}
func (p *Proxy) Close() {
	p.transport.CloseIdleConnections()
	if p.cas != nil {
		p.cas.mu.Lock()
		defer p.cas.mu.Unlock()
		_ = p.cas.file.Close()
	}
}
func (p *Proxy) Snapshot() Report {
	p.mu.Lock()
	defer p.mu.Unlock()
	r := Report{CASLoss: p.CASLossSnapshot(), Schema: 1, Counts: p.counts, Inflight: p.counts.Attempts - p.counts.Finished, Methods: map[string]uint64{}, Status: map[int]uint64{}}
	for k, v := range p.methods {
		r.Methods[k] = v
	}
	for k, v := range p.status {
		r.Status[k] = v
	}
	return r
}
func method(m string) string {
	switch m {
	case "GET", "PUT", "HEAD", "POST", "DELETE", "OPTIONS":
		return m
	}
	return "OTHER"
}

type stateKey struct{}
type state struct {
	casSelected                                                                              bool
	requestBytes, responseBytes, requestErrors, responseErrors, writeErrors, transportErrors uint64
	status                                                                                   int
}
type body struct {
	mu     sync.Mutex
	closed bool
	io.ReadCloser
	s       *state
	request bool
}

func (b *body) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, io.EOF
	}
	n, e := b.ReadCloser.Read(p)
	if b.request {
		b.s.requestBytes += uint64(n)
		if e != nil && e != io.EOF {
			b.s.requestErrors++
		}
	} else {
		if e != nil && e != io.EOF {
			b.s.responseErrors++
		}
	}
	return n, e
}

func (b *body) Close() error {
	e := b.ReadCloser.Close()
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	return e
}

type roundTripper struct {
	base  http.RoundTripper
	proxy *Proxy
}

func (t roundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	selected := false
	if t.proxy.cas != nil {
		var err error
		selected, err = t.proxy.cas.prepare(r)
		if err != nil {
			return nil, err
		}
	}
	if selected {
		r.Context().Value(stateKey{}).(*state).casSelected = true
	}
	response, e := t.base.RoundTrip(r)
	if e == nil && t.proxy.cas != nil {
		if err := t.proxy.cas.after(r, response, selected); err != nil {
			_ = response.Body.Close()
			return nil, err
		}
	}
	if e == nil && response.Body != nil {
		response.Body = &body{ReadCloser: response.Body, s: r.Context().Value(stateKey{}).(*state)}
	}
	return response, e
}

type writer struct {
	http.ResponseWriter
	s *state
}

func (w writer) WriteHeader(code int) {
	if code >= 200 && w.s.status == 0 {
		w.s.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}
func (w writer) Write(b []byte) (int, error) {
	if w.s.status == 0 {
		w.s.status = 200
	}
	n, e := w.ResponseWriter.Write(b)
	w.s.responseBytes += uint64(n)
	if e != nil {
		w.s.writeErrors++
	}
	return n, e
}
func (w writer) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s := &state{}
	p.mu.Lock()
	p.counts.Attempts++
	p.methods[method(r.Method)]++
	p.mu.Unlock()
	defer func() {
		failure := recover()
		if failure == http.ErrAbortHandler && s.casSelected && p.cas != nil {
			p.cas.observeAbort()
		}
		p.mu.Lock()
		c := &p.counts
		c.Finished++
		c.RequestBodyBytes += s.requestBytes
		c.ResponseBodyBytes += s.responseBytes
		c.RequestReadErrors += s.requestErrors
		c.ResponseReadErrors += s.responseErrors
		c.ResponseWriteErrors += s.writeErrors
		c.TransportErrors += s.transportErrors
		if r.Context().Err() != nil {
			c.Canceled++
		}
		if failure != nil {
			c.Aborted++
		}
		if failure == nil && r.Context().Err() == nil && s.requestErrors+s.responseErrors+s.writeErrors+s.transportErrors == 0 {
			c.Completed++
		}
		if s.status != 0 {
			p.status[s.status]++
		}
		p.mu.Unlock()
		if failure != nil {
			panic(failure)
		}
	}()
	r = r.WithContext(context.WithValue(r.Context(), stateKey{}, s))
	if r.Body != nil {
		r.Body = &body{ReadCloser: r.Body, s: s, request: true}
		defer r.Body.Close()
	}
	p.proxy.ServeHTTP(writer{w, s}, r)
}
