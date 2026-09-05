// Package recorder is a proof-only metadata journal, never application storage.
package recorder

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
)

type Event struct {
	HandshakeNS  int64  `json:"handshake_ns,omitempty"`
	Measurement  string `json:"measurement,omitempty"`
	ParentID     string `json:"parent_id,omitempty"`
	ResultStatus string `json:"result_status,omitempty"`
	Kind         string `json:"kind"`
	Phase        string `json:"phase,omitempty"`
	Producer     string `json:"producer,omitempty"`
	ID           string `json:"id,omitempty"`
	Sequence     uint64 `json:"sequence,omitempty"`
	Family       string `json:"family,omitempty"`
	Method       string `json:"method,omitempty"`
	Partition    string `json:"partition,omitempty"`
	OperationID  string `json:"operation_id,omitempty"`
	Status       string `json:"status,omitempty"`
	DurationNS   int64  `json:"duration_ns,omitempty"`
}
type State struct {
	Limit         int
	Events        int
	Registrations map[string]Event
	Terminals     map[string]Event
	Phases        map[string]string
	Closed        map[string]bool
	Dead          map[string]bool
	Sequence      map[string]uint64
}

func NewState(limit int) *State {
	return &State{Limit: limit, Registrations: map[string]Event{}, Terminals: map[string]Event{}, Phases: map[string]string{}, Closed: map[string]bool{}, Dead: map[string]bool{}, Sequence: map[string]uint64{}}
}

var identifier = regexp.MustCompile(`^[A-Za-z0-9_.:/-]{1,128}$`)

func key(e Event) string { return pair(e.Producer, e.ID) }

func validStatus(v string) bool {
	switch v {
	case "OK", "Canceled", "Unknown", "InvalidArgument", "DeadlineExceeded", "NotFound", "AlreadyExists", "PermissionDenied", "ResourceExhausted", "FailedPrecondition", "Aborted", "OutOfRange", "Unimplemented", "Internal", "Unavailable", "DataLoss", "Unauthenticated":
		return true
	}
	return false
}
func pair(a, b string) string { raw, _ := json.Marshal([]string{a, b}); return string(raw) }

// Apply returns false for an exact idempotent replay, true for a new event.
func (s *State) Apply(e Event) (bool, error) {
	for _, v := range []string{e.Phase, e.Producer, e.ID, e.Family, e.Method, e.Partition, e.OperationID, e.ParentID, e.ResultStatus} {
		if v != "" && !identifier.MatchString(v) {
			return false, fmt.Errorf("invalid metadata identifier")
		}
	}
	if e.Kind == "register" {
		if old, ok := s.Registrations[key(e)]; ok {
			if old == e {
				return false, nil
			}
			return false, fmt.Errorf("conflicting registration")
		}
	}
	if e.Kind == "terminal" {
		if old, ok := s.Terminals[key(e)]; ok {
			if old == e {
				return false, nil
			}
			return false, fmt.Errorf("conflicting terminal")
		}
	}
	if e.Phase != "" && s.Closed[e.Phase] {
		return false, fmt.Errorf("phase closed")
	}
	if s.Events >= s.Limit*4 {
		return false, fmt.Errorf("event capacity exhausted")
	}
	// Each record has a narrow schema; arbitrary metadata cannot be smuggled on barriers.
	clean := e
	switch e.Kind {
	case "open":
		clean.Phase = ""
		clean.Status = ""
	case "close":
		clean.Phase = ""
	case "death":
		clean.Producer = ""
	case "register":
		clean.Phase = ""
		clean.Producer = ""
		clean.ID = ""
		clean.Sequence = 0
		clean.Family = ""
		clean.Method = ""
		clean.Partition = ""
		clean.OperationID = ""
		clean.Measurement = ""
		clean.ParentID = ""
	case "terminal":
		clean.Phase = ""
		clean.Producer = ""
		clean.ID = ""
		clean.Sequence = 0
		clean.Status = ""
		clean.DurationNS = 0
		clean.ResultStatus = ""
		clean.HandshakeNS = 0
	}
	clean.Kind = ""
	if clean != (Event{}) {
		return false, fmt.Errorf("unexpected event fields")
	}
	switch e.Kind {
	case "open":
		if e.Phase == "" || (e.Status != "steady" && e.Status != "fault") || s.Phases[e.Phase] != "" {
			return false, fmt.Errorf("invalid phase open")
		}
		s.Phases[e.Phase] = e.Status
	case "register":
		if e.Measurement != "rpc_invocation" && e.Measurement != "execute_attempt" {
			return false, fmt.Errorf("invalid measurement kind")
		}
		if e.Measurement == "rpc_invocation" && e.ParentID != "" {
			return false, fmt.Errorf("invocation cannot have parent")
		}
		if e.Measurement == "execute_attempt" {
			parent, ok := s.Registrations[pair(e.Producer, e.ParentID)]
			_, done := s.Terminals[pair(e.Producer, e.ParentID)]
			if !ok || done || parent.Measurement != "rpc_invocation" || parent.Phase != e.Phase || parent.Family != e.Family {
				return false, fmt.Errorf("invalid attempt parent")
			}
		}
		if e.ID == "" || e.Producer == "" || e.Family == "" || s.Phases[e.Phase] == "" || s.Closed[e.Phase] || s.Dead[e.Producer] || e.Sequence != s.Sequence[e.Producer]+1 || len(s.Registrations) >= s.Limit || e.DurationNS != 0 || e.Status != "" {
			return false, fmt.Errorf("registration rejected: phase, sequence or capacity")
		}
		s.Registrations[key(e)] = e
		s.Sequence[e.Producer] = e.Sequence
	case "terminal":
		registration, ok := s.Registrations[key(e)]
		if !ok || e.Phase != registration.Phase || s.Closed[e.Phase] || e.Sequence != registration.Sequence {
			return false, fmt.Errorf("terminal lacks matching open registration")
		}
		if (e.Status != "completed" && e.Status != "admission_failed") || (e.Status == "completed" && e.DurationNS <= 0) || (e.Status == "admission_failed" && e.DurationNS != 0) {
			return false, fmt.Errorf("invalid terminal observation")
		}
		if e.HandshakeNS < 0 || e.HandshakeNS > e.DurationNS {
			return false, fmt.Errorf("invalid handshake duration")
		}
		if e.Status == "completed" && !validStatus(e.ResultStatus) {
			return false, fmt.Errorf("invalid RPC result status")
		}
		if e.Status == "admission_failed" && e.ResultStatus != "" {
			return false, fmt.Errorf("admission failure has RPC result")
		}
		s.Terminals[key(e)] = e
	case "death":
		if e.Producer == "" || s.Dead[e.Producer] {
			return false, fmt.Errorf("invalid producer death")
		}
		s.Dead[e.Producer] = true
	case "close":
		if s.Phases[e.Phase] == "" || s.Closed[e.Phase] {
			return false, fmt.Errorf("invalid phase close")
		}
		for k, r := range s.Registrations {
			if r.Phase == e.Phase {
				if _, ok := s.Terminals[k]; !ok && (s.Phases[e.Phase] == "steady" || !s.Dead[r.Producer]) {
					return false, fmt.Errorf("phase has unresolved live or steady registrations")
				}
			}
		}
		s.Closed[e.Phase] = true
	default:
		return false, fmt.Errorf("unknown event kind")
	}
	s.Events++
	return true, nil
}

type Summary struct {
	CensusComplete bool               `json:"census_complete"`
	Registered     int                `json:"registered"`
	Completed      int                `json:"completed"`
	NotAdmitted    int                `json:"not_admitted"`
	Unobserved     []string           `json:"terminal_unobserved"`
	SteadyEligible map[string]bool    `json:"steady_eligible"`
	Durations      map[string][]int64 `json:"completed_durations_ns"`
}

func (s *State) Summary() (Summary, error) {
	r := Summary{Registered: len(s.Registrations), Unobserved: []string{}, SteadyEligible: map[string]bool{}, Durations: map[string][]int64{}}
	for phase := range s.Phases {
		if !s.Closed[phase] {
			return r, fmt.Errorf("phase not closed")
		}
	}
	for k, registration := range s.Registrations {
		t, ok := s.Terminals[k]
		group := pair(registration.Measurement, pair(registration.Phase, registration.Family))
		if !ok {
			r.Unobserved = append(r.Unobserved, k)
		} else if t.Status == "admission_failed" {
			r.NotAdmitted++
		} else {
			r.Completed++
			r.Durations[group] = append(r.Durations[group], t.DurationNS)
		}
		if s.Phases[registration.Phase] == "steady" {
			r.SteadyEligible[group] = len(r.Durations[group]) >= 100
		}
	}
	for group := range r.SteadyEligible {
		r.SteadyEligible[group] = len(r.Durations[group]) >= 100
	}
	for group := range r.Durations {
		sort.Slice(r.Durations[group], func(i, j int) bool { return r.Durations[group][i] < r.Durations[group][j] })
	}
	sort.Strings(r.Unobserved)
	r.CensusComplete = true
	return r, nil
}

type Recorder struct {
	admission chan struct{}
	mu        sync.Mutex
	state     *State
	file      *os.File
	failed    atomic.Bool
	closed    bool
}

func New(path string, limit int) (*Recorder, error) {
	if limit < 1 || limit > 1000000 {
		return nil, fmt.Errorf("invalid capacity")
	}
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return nil, e
	}
	r := &Recorder{state: NewState(limit), file: f, admission: make(chan struct{}, 4)}
	_, e = fmt.Fprintf(f, "{\"kind\":\"header\",\"limit\":%d}\n", limit)
	if e == nil {
		e = f.Sync()
	}
	if e != nil {
		f.Close()
		return nil, e
	}
	return r, nil
}
func (r *Recorder) Accept(e Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failed.Load() || r.closed {
		return fmt.Errorf("recorder unavailable")
	}
	fresh, err := r.state.Apply(e)
	if err != nil {
		return err
	}
	if !fresh {
		return nil
	}
	if err = json.NewEncoder(r.file).Encode(e); err == nil {
		err = r.file.Sync()
	}
	if err != nil {
		r.failed.Store(true)
	}
	return err
}
func (r *Recorder) Close() (retErr error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("already closed")
	}
	r.closed = true
	defer func() {
		if err := r.file.Close(); retErr == nil {
			retErr = err
		}
	}()
	if r.failed.Load() {
		return fmt.Errorf("recorder failed")
	}
	if _, e := r.state.Summary(); e != nil {
		return e
	}
	if _, e := r.file.WriteString("{\"kind\":\"footer\"}\n"); e != nil {
		return e
	}
	return r.file.Sync()
}
func (r *Recorder) reject(w http.ResponseWriter, message string, code int) {
	r.failed.Store(true)
	http.Error(w, message, code)
}
func (r *Recorder) ServeHTTP(w http.ResponseWriter, q *http.Request) {
	if q.Method != "POST" || q.URL.Path != "/event" {
		r.reject(w, "unknown recorder route", 404)
		return
	}
	select {
	case r.admission <- struct{}{}:
		defer func() { <-r.admission }()
	default:
		r.reject(w, "recorder busy", 503)
		return
	}
	q.Body = http.MaxBytesReader(w, q.Body, 4096)
	decoder := json.NewDecoder(q.Body)
	decoder.DisallowUnknownFields()
	var e Event
	if err := decoder.Decode(&e); err != nil {
		r.reject(w, "invalid event", 400)
		return
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		r.reject(w, "trailing event", 400)
		return
	}
	if err := r.Accept(e); err != nil {
		r.reject(w, "event rejected", 409)
		return
	}
	w.WriteHeader(204)
}
func Validate(path string) (Summary, error) {
	f, e := os.Open(path)
	if e != nil {
		return Summary{}, e
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Size() == 0 {
		return Summary{}, fmt.Errorf("empty journal")
	}
	var last [1]byte
	if _, err = f.ReadAt(last[:], info.Size()-1); err != nil || last[0] != '\n' {
		return Summary{}, fmt.Errorf("incomplete final line")
	}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 4096)
	var state *State
	footer := false
	for scanner.Scan() {
		line := scanner.Bytes()
		if footer {
			return Summary{}, fmt.Errorf("data after footer")
		}
		var h struct {
			Kind  string `json:"kind"`
			Limit int    `json:"limit"`
		}
		if e = json.Unmarshal(line, &h); e != nil {
			return Summary{}, e
		}
		if state == nil {
			if h.Kind != "header" || h.Limit < 1 || h.Limit > 1000000 {
				return Summary{}, fmt.Errorf("invalid header")
			}
			if string(line) != fmt.Sprintf("{\"kind\":\"header\",\"limit\":%d}", h.Limit) {
				return Summary{}, fmt.Errorf("noncanonical header")
			}
			state = NewState(h.Limit)
			continue
		}
		if h.Kind == "footer" {
			if string(line) != "{\"kind\":\"footer\"}" {
				return Summary{}, fmt.Errorf("invalid footer")
			}
			footer = true
			continue
		}
		var event Event
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		if e = decoder.Decode(&event); e != nil {
			return Summary{}, e
		}
		fresh, err := state.Apply(event)
		if err != nil || !fresh {
			return Summary{}, fmt.Errorf("invalid or duplicate journal event")
		}
	}
	if e = scanner.Err(); e != nil {
		return Summary{}, e
	}
	if !footer || state == nil {
		return Summary{}, fmt.Errorf("missing final recorder footer")
	}
	return state.Summary()
}
