package recorder

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func event(t *testing.T, url string, e Event) {
	t.Helper()
	raw, _ := json.Marshal(e)
	r, err := http.Post(url+"/event", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.StatusCode != 204 {
		t.Fatal("event rejected", e, r.StatusCode)
	}
}
func registration(id string, seq uint64) Event {
	return Event{Measurement: "rpc_invocation", Kind: "register", Phase: "fault", Producer: "producer-1", ID: id, Sequence: seq, Family: "shard"}
}
func TestRecorderProducer(t *testing.T) {
	if os.Getenv("XENON_RECORDER_CHILD") == "" {
		return
	}
	event(t, os.Getenv("RECORDER_URL"), registration("killed", 2))
	fmt.Println("REGISTERED_WORK_FINISHED_END_NOT_SENT")
	time.Sleep(time.Hour)
}
func TestRecorderKillAndLostAcknowledgment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.jsonl")
	r, e := New(path, 10)
	if e != nil {
		t.Fatal(e)
	}
	server := httptest.NewServer(r)
	defer server.Close()
	event(t, server.URL, Event{Kind: "open", Phase: "fault", Status: "fault"})
	// Accepted BEGIN response is deliberately discarded, then identical retry readback.
	raw, _ := json.Marshal(registration("lost-ack", 1))
	response, e := http.Post(server.URL+"/event", "application/json", bytes.NewReader(raw))
	if e != nil {
		t.Fatal(e)
	}
	response.Body.Close()
	event(t, server.URL, registration("lost-ack", 1))
	child := exec.Command(os.Args[0], "-test.run=^TestRecorderProducer$")
	child.Env = append(os.Environ(), "XENON_RECORDER_CHILD=1", "RECORDER_URL="+server.URL)
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	out, e := child.StdoutPipe()
	if e != nil {
		t.Fatal(e)
	}
	if e = child.Start(); e != nil {
		t.Fatal(e)
	}
	waited := false
	defer func() {
		if !waited {
			_ = syscall.Kill(-child.Process.Pid, syscall.SIGKILL)
			_ = child.Wait()
		}
	}()
	ready := make(chan bool, 1)
	go func() {
		buf := make([]byte, 256)
		n, _ := out.Read(buf)
		ready <- bytes.Contains(buf[:n], []byte("END_NOT_SENT"))
	}()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("missing child barrier")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("producer startup deadline")
	}
	if e = syscall.Kill(-child.Process.Pid, syscall.SIGKILL); e != nil {
		t.Fatal(e)
	}
	_ = child.Wait()
	waited = true
	event(t, server.URL, Event{Kind: "death", Producer: "producer-1"})
	event(t, server.URL, Event{Kind: "close", Phase: "fault"})
	if e = r.Close(); e != nil {
		t.Fatal(e)
	}
	summary, e := Validate(path)
	if e != nil {
		t.Fatal(e)
	}
	if !summary.CensusComplete || summary.Registered != 2 || summary.Completed != 0 || len(summary.Unobserved) != 2 || len(summary.Durations) != 0 {
		t.Fatal(summary)
	}
}
func TestRecorderSequenceCapacityAndSteady(t *testing.T) {
	s := NewState(2)
	if _, e := s.Apply(Event{Kind: "open", Phase: "fault", Status: "steady"}); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Apply(registration("gap", 2)); e == nil {
		t.Fatal("sequence gap passed")
	}
	s.Apply(registration("one", 1))
	if _, e := s.Apply(Event{Kind: "close", Phase: "fault"}); e == nil {
		t.Fatal("unresolved steady passed")
	}
	s.Apply(registration("two", 2))
	if _, e := s.Apply(registration("three", 3)); e == nil {
		t.Fatal("capacity passed")
	}
	if _, e := s.Apply(registration("one", 1)); e != nil {
		t.Fatal("idempotent retry failed", e)
	}
	changed := registration("one", 1)
	changed.Family = "other"
	if _, e := s.Apply(changed); e == nil {
		t.Fatal("conflict passed")
	}
}
func TestRecorderRejectsMissingFooter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace")
	r, e := New(path, 3)
	if e != nil {
		t.Fatal(e)
	}
	defer r.file.Close()
	if _, e = Validate(path); e == nil {
		t.Fatal("unclosed passed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", "http://127.0.0.1:1/event", nil)
	if _, e = http.DefaultClient.Do(req); e == nil {
		t.Fatal("recorder loss unexpectedly acknowledged")
	}
}

func TestRecorderCompletedPopulationAndMalformedJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace")
	r, err := New(path, 101)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Accept(Event{Kind: "open", Phase: "steady", Status: "steady"}); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 100; i++ {
		e := registration(fmt.Sprint(i), uint64(i))
		e.Phase = "steady"
		if err = r.Accept(e); err != nil {
			t.Fatal(err)
		}
		if err = r.Accept(Event{Kind: "terminal", Phase: e.Phase, Producer: e.Producer, ID: e.ID, Sequence: e.Sequence, Status: "completed", ResultStatus: "OK", DurationNS: int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err = r.Accept(Event{Kind: "close", Phase: "steady"}); err != nil {
		t.Fatal(err)
	}
	e := registration("1", 1)
	e.Phase = "steady"
	if err = r.Accept(e); err != nil {
		t.Fatal("closed exact registration retry", err)
	}
	if err = r.Accept(Event{Kind: "terminal", Phase: "steady", Producer: e.Producer, ID: e.ID, Sequence: 1, Status: "completed", ResultStatus: "OK", DurationNS: 1}); err != nil {
		t.Fatal("closed terminal retry", err)
	}
	e.Family = "changed"
	if err = r.Accept(e); err == nil {
		t.Fatal("closed conflict accepted")
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	summary, err := Validate(path)
	if err != nil || summary.Completed != 100 || !summary.SteadyEligible[pair("rpc_invocation", pair("steady", "shard"))] {
		t.Fatal(summary, err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for name, bad := range map[string][]byte{"truncated": raw[:len(raw)-1], "footer-extra": bytes.Replace(raw, []byte(`{"kind":"footer"}`), []byte(`{"kind":"footer","unexpected":true}`), 1), "trailing": append(append([]byte{}, raw...), []byte("{}\n")...)} {
		p := filepath.Join(t.TempDir(), name)
		if err = os.WriteFile(p, bad, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err = Validate(p); err == nil {
			t.Fatal("malformed accepted", name)
		}
	}
	if pair("a/b", "c") == pair("a", "b/c") {
		t.Fatal("identity collision")
	}
}

// This child hosts the recorder independently of its controlling process group.
func TestRecorderServerProcess(t *testing.T) {
	if os.Getenv("XENON_RECORDER_SERVER") == "" {
		return
	}
	r, err := New(os.Getenv("RECORDER_JOURNAL"), 10)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(r)
	fmt.Println(server.URL)
	time.Sleep(time.Hour)
}
func TestRecorderProcessLoss(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace")
	child := exec.Command(os.Args[0], "-test.run=^TestRecorderServerProcess$")
	child.Env = append(os.Environ(), "XENON_RECORDER_SERVER=1", "RECORDER_JOURNAL="+path)
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	out, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = syscall.Kill(-child.Process.Pid, syscall.SIGKILL)
			_ = child.Wait()
		}
	}()
	ready := make(chan string, 1)
	go func() {
		reader := bufio.NewReader(out)
		line, _ := reader.ReadString('\n')
		ready <- strings.TrimSpace(line)
	}()
	var url string
	select {
	case url = <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("recorder startup deadline")
	}
	event(t, url, Event{Kind: "open", Phase: "fault", Status: "fault"})
	event(t, url, registration("registered", 1))
	if err = syscall.Kill(-child.Process.Pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	waited = true
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	raw, _ := json.Marshal(registration("never-start", 2))
	req, _ := http.NewRequestWithContext(ctx, "POST", url+"/event", bytes.NewReader(raw))
	if response, err := http.DefaultClient.Do(req); err == nil {
		response.Body.Close()
		t.Fatal("dead recorder acknowledged")
	}
	if _, err = Validate(path); err == nil {
		t.Fatal("killed recorder passed")
	}
}

func TestRecorderMeasurementLinkage(t *testing.T) {
	s := NewState(10)
	apply := func(e Event) {
		t.Helper()
		if _, err := s.Apply(e); err != nil {
			t.Fatal(err)
		}
	}
	apply(Event{Kind: "open", Phase: "fault", Status: "steady"})
	parent := registration("parent", 1)
	apply(parent)
	child := registration("child", 2)
	child.Measurement = "execute_attempt"
	child.ParentID = "missing"
	if _, err := s.Apply(child); err == nil {
		t.Fatal("missing parent accepted")
	}
	child.ParentID = "parent"
	apply(child)
	end := Event{Kind: "terminal", Phase: "fault", Producer: child.Producer, ID: child.ID, Sequence: 2, Status: "completed", ResultStatus: "secret arbitrary text", DurationNS: 10}
	if _, err := s.Apply(end); err == nil {
		t.Fatal("arbitrary result accepted")
	}
	end.ResultStatus = "Unavailable"
	apply(end)
	end.ID = parent.ID
	end.Sequence = 1
	end.DurationNS = 100
	end.ResultStatus = "OK"
	apply(end)
	apply(Event{Kind: "close", Phase: "fault"})
	summary, err := s.Summary()
	if err != nil {
		t.Fatal(err)
	}
	if len(summary.Durations) != 2 || summary.Durations[pair("execute_attempt", pair("fault", "shard"))][0] != 10 || summary.Durations[pair("rpc_invocation", pair("fault", "shard"))][0] != 100 {
		t.Fatal(summary)
	}
}

func TestRecorderHTTPProtocolFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal")
	r, err := New(path, 10)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(r)
	defer server.Close()
	response, err := http.Post(server.URL+"/event", "application/json", strings.NewReader(`{"kind":"register","secret":"rejected"}`))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 400 {
		t.Fatal(response.StatusCode)
	}
	if r.Close() == nil {
		t.Fatal("protocol error finalized successfully")
	}
	if _, err = Validate(path); err == nil {
		t.Fatal("protocol error journal passed")
	}
}
