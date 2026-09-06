//go:build linux || darwin

package filesystem

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"

	"github.com/0x63616c/xenon/internal/registry"
	"github.com/0x63616c/xenon/internal/registry/contracttest"
)

func testStore(t *testing.T, dir string) *Store {
	t.Helper()
	s, e := New(Config{Directory: dir, MaxRecordBytes: 1 << 20, Wait: func(ctx context.Context) error { runtime.Gosched(); return ctx.Err() }})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func TestContract(t *testing.T) { contracttest.Run(t, t.Context(), testStore(t, t.TempDir())) }
func TestRecoveryErrorsAndProtocol(t *testing.T) {
	for _, cut := range []string{"write-file-synced", "renamed", "directory-synced"} {
		t.Run(cut, func(t *testing.T) {
			dir := t.TempDir()
			s := testStore(t, dir)
			var trace []string
			fault := errors.New("injected I/O error")
			s.boundary = func(stage string) error {
				trace = append(trace, stage)
				if stage == cut {
					return fault
				}
				return nil
			}
			w := contracttest.Write(t, "control", "", 1, "payload")
			_, err := s.Create(t.Context(), "control", w)
			var unknown *registry.UnknownOutcome
			var unavailable *registry.Unavailable
			if cut == "write-file-synced" {
				if !errors.As(err, &unavailable) {
					t.Fatal(err)
				}
			} else if !errors.As(err, &unknown) {
				t.Fatal(err)
			}
			s.boundary = nil
			r, err := s.Read(t.Context(), "control")
			if cut == "write-file-synced" {
				var missing *registry.NotFound
				if !errors.As(err, &missing) {
					t.Fatal(err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			e, _ := registry.Decode("control", r)
			if string(e.Body) != "payload" {
				t.Fatal(e)
			}
			for _, readCut := range []string{"read-file-synced", "directory-synced"} {
				s.boundary = func(stage string) error {
					if stage == readCut {
						return fault
					}
					return nil
				}
				if _, err = s.Read(t.Context(), "control"); !errors.As(err, &unavailable) {
					t.Fatal("read exposed undurable data", err)
				}
			}
		})
	}
	// The same independent order checker must reject omitted fsync/recovery steps.
	s := testStore(t, t.TempDir())
	var events []string
	s.boundary = func(stage string) error { events = append(events, stage); return nil }
	if _, err := s.Create(t.Context(), "control", contracttest.Write(t, "control", "", 1, "x")); err != nil {
		t.Fatal(err)
	}
	check := func(got []string, want []string) bool { return strings.Join(got, ",") == strings.Join(want, ",") }
	want := []string{"condition-checked", "write-file-synced", "renamed", "directory-synced"}
	if !check(events, want) {
		t.Fatal(events)
	}
	for _, omit := range []string{"write-file-synced", "directory-synced"} {
		var mutated []string
		for _, e := range events {
			if e != omit {
				mutated = append(mutated, e)
			}
		}
		if check(mutated, want) {
			t.Fatal("missing fsync control accepted")
		}
	}
	events = nil
	if _, err := s.Read(t.Context(), "control"); err != nil {
		t.Fatal(err)
	}
	want = []string{"read-file-synced", "directory-synced"}
	if !check(events, want) || check(nil, want) {
		t.Fatal("read recovery control", events)
	}
}
func TestContainmentCorruptionCancellation(t *testing.T) {
	dir := t.TempDir()
	s := testStore(t, dir)
	key := registry.Key("control")
	w := contracttest.Write(t, key, "", 1, "x")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Create(ctx, key, w); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// Contention cancellation releases its own descriptor, never the holder's lock.
	l, err := s.lock(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(t.Context())
	s.wait = func(context.Context) error { cancel(); return ctx.Err() }
	if _, err = s.Create(ctx, key, w); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	l.Close()
	if _, err = s.Create(t.Context(), key, w); err != nil {
		t.Fatal(err)
	}
	good, err := os.ReadFile(dir + "/" + filename(key) + ".record")
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{[]byte("broken"), append(append([]byte{}, good...), byte(' ')), append([]byte(magic), make([]byte, 8)...)} {
		if err = os.WriteFile(dir+"/"+filename(key)+".record", bad, 0600); err != nil {
			t.Fatal(err)
		}
		var corrupt *registry.Corrupt
		if _, err = s.Read(t.Context(), key); !errors.As(err, &corrupt) {
			t.Fatal(err)
		}
	}
	bad := append([]byte{}, good...)
	binary.BigEndian.PutUint64(bad[8:16], 99)
	os.WriteFile(dir+"/"+filename(key)+".record", bad, 0600)
	var corrupt *registry.Corrupt
	if _, err = s.Read(t.Context(), key); !errors.As(err, &corrupt) {
		t.Fatal("generation corruption", err)
	}
	// An attacker-controlled link cannot escape the root or redirect the lock.
	outside := t.TempDir() + "/target"
	os.WriteFile(outside, []byte("sentinel"), 0600)
	for _, suffix := range []string{".record", ".lock", ".tmp"} {
		os.Remove(dir + "/" + filename(key) + suffix)
		if err = os.Symlink(outside, dir+"/"+filename(key)+suffix); err != nil {
			t.Fatal(err)
		}
		if _, err = s.Create(t.Context(), key, w); err == nil {
			t.Fatal("followed symlink", suffix)
		}
		os.Remove(dir + "/" + filename(key) + suffix)
	}
	b, _ := os.ReadFile(outside)
	if string(b) != "sentinel" {
		t.Fatal("escaped namespace")
	}
}

// TestProcessParticipant is also the executable remote-mount protocol. One JSON
// job, then a GO line; cutpoints signal stdout and wait for CONTINUE or process
// death. No schedule depends on sleeps. Environment selects only this helper.
func TestProcessParticipant(t *testing.T) {
	if os.Getenv("XENON_FS_PARTICIPANT") != "1" {
		return
	}
	if os.Getenv("XENON_FS_REMOTE") == "1" {
		fmt.Printf("PID %d\n", os.Getpid())
	}
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	var job processJob
	if err := json.Unmarshal(scanner.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	s := testStore(t, job.Directory)
	fmt.Println("READY")
	if !scanner.Scan() {
		t.Fatal("missing GO")
	}
	s.wait = func(ctx context.Context) error {
		if job.Cut == "contended" {
			fmt.Println("CUT contended")
			if !scanner.Scan() {
				return errors.New("contention interrupted")
			}
		}
		runtime.Gosched()
		return ctx.Err()
	}
	s.boundary = func(stage string) error {
		if stage == job.Cut {
			fmt.Println("CUT " + stage)
			if !scanner.Scan() {
				return errors.New("cut interrupted")
			}
		}
		return nil
	}
	var r registry.Record
	var err error
	if job.Action == "contract" {
		contracttest.Run(t, t.Context(), s)
	} else if job.Action == "read" {
		r, err = s.Read(t.Context(), "process/control")
	} else {
		w := contracttest.Write(t, "process/control", registry.Version(job.Expected), job.ID, job.Body)
		if job.Action == "create" {
			r, err = s.Create(t.Context(), "process/control", w)
		} else {
			r, err = s.Replace(t.Context(), "process/control", registry.Version(job.Expected), w)
		}
	}
	result := processResult{Record: r}
	if err != nil {
		var c *registry.Conflict
		if errors.As(err, &c) {
			result.Error = "conflict"
		} else {
			result.Error = fmt.Sprintf("%T: %v", err, err)
		}
	}
	b, _ := json.Marshal(result)
	fmt.Println("RESULT " + string(b))
}

type processJob struct {
	Directory, Action, Expected, Body, Cut string
	ID                                     int
}
type processResult struct {
	Record registry.Record
	Error  string
}
type participant struct {
	cmd    *exec.Cmd
	input  *json.Encoder
	goLine func()
	scan   *bufio.Scanner
}

func startParticipant(t *testing.T, job processJob) *participant {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestProcessParticipant$")
	cmd.Env = append(os.Environ(), "XENON_FS_PARTICIPANT=1")
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait(); in.Close() })
	p := &participant{cmd: cmd, input: json.NewEncoder(in), goLine: func() { fmt.Fprintln(in, "GO") }, scan: bufio.NewScanner(out)}
	p.input.Encode(job)
	if !p.scan.Scan() || p.scan.Text() != "READY" {
		t.Fatal("participant not ready", p.scan.Text())
	}
	return p
}
func (p *participant) result(t *testing.T) processResult {
	t.Helper()
	if !p.scan.Scan() {
		t.Fatal("missing result")
	}
	line := p.scan.Text()
	if !strings.HasPrefix(line, "RESULT ") {
		t.Fatal(line)
	}
	var r processResult
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "RESULT ")), &r); err != nil {
		t.Fatal(err)
	}
	if err := p.cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	return r
}
func TestProcessCrashRecoveryAndCAS(t *testing.T) {
	dir := t.TempDir()
	s := testStore(t, dir)
	writer := startParticipant(t, processJob{Directory: dir, Action: "create", ID: 1, Body: "durable after read", Cut: "renamed"})
	writer.goLine()
	if !writer.scan.Scan() || writer.scan.Text() != "CUT renamed" {
		t.Fatal("missing exact crash boundary")
	}
	// SIGKILL releases the real flock while deliberately skipping directory sync.
	if err := writer.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	writer.cmd.Wait()
	var recovery []string
	s.boundary = func(stage string) error { recovery = append(recovery, stage); return nil }
	r, err := s.Read(t.Context(), "process/control")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(recovery, ",") != "read-file-synced,directory-synced" {
		t.Fatal(recovery)
	}
	env, err := registry.Decode("process/control", r)
	if err != nil || string(env.Body) != "durable after read" {
		t.Fatal(env, err)
	}
	before, err := os.Stat(dir + "/" + filename("process/control") + ".lock")
	if err != nil {
		t.Fatal(err)
	}
	a := startParticipant(t, processJob{Directory: dir, Action: "replace", Expected: string(r.Version), ID: 2, Body: "a", Cut: "condition-checked"})
	b := startParticipant(t, processJob{Directory: dir, Action: "replace", Expected: string(r.Version), ID: 3, Body: "b", Cut: "contended"})
	a.goLine()
	if !a.scan.Scan() || a.scan.Text() != "CUT condition-checked" {
		t.Fatal("missing held CAS boundary")
	}
	b.goLine()
	if !b.scan.Scan() || b.scan.Text() != "CUT contended" {
		t.Fatal("unlocked replace crossed held CAS", b.scan.Text())
	}
	a.goLine()
	ra := a.result(t)
	b.goLine()
	rb := b.result(t)
	if (ra.Error == "") == (rb.Error == "") || ra.Error != "" && ra.Error != "conflict" || rb.Error != "" && rb.Error != "conflict" {
		t.Fatal(ra, rb)
	}
	after, err := os.Stat(dir + "/" + filename("process/control") + ".lock")
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("lock inode changed", err)
	}
	restart := startParticipant(t, processJob{Directory: dir, Action: "read"})
	restart.goLine()
	rr := restart.result(t)
	winner := ra
	if winner.Error != "" {
		winner = rb
	}
	if rr.Error != "" || rr.Record.Version != winner.Record.Version || string(rr.Record.Body) != string(winner.Record.Body) {
		t.Fatal("lost acknowledged winner", rr, winner)
	}
}

// Audits calls at the actual fsync syscall boundary. Source omission mutants
// retain stage callbacks, so callbacks alone cannot make these assertions pass.
func TestSyncSyscallBoundaries(t *testing.T) {
	s := testStore(t, t.TempDir())
	var calls []string
	s.fileSync = func(f *os.File) error {
		st, err := f.Stat()
		if err != nil {
			return err
		}
		kind := "file"
		if st.IsDir() {
			kind = "directory"
		}
		calls = append(calls, kind)
		return f.Sync()
	}
	if _, err := s.Create(t.Context(), "control", contracttest.Write(t, "control", "", 1, "x")); err != nil {
		t.Fatal(err)
	}
	if strings.Join(calls, ",") != "file,directory" {
		t.Fatal("write must sync file then directory", calls)
	}
	calls = nil
	if _, err := s.Read(t.Context(), "control"); err != nil {
		t.Fatal(err)
	}
	if strings.Join(calls, ",") != "file,directory" {
		t.Fatal("read must recover file then directory durability", calls)
	}
}

func TestCancellationAfterRenameAndSyncFailure(t *testing.T) {
	s := testStore(t, t.TempDir())
	ctx, cancel := context.WithCancel(t.Context())
	s.boundary = func(stage string) error {
		if stage == "renamed" {
			cancel()
		}
		return nil
	}
	w := contracttest.Write(t, "control", "", 1, "published")
	_, err := s.Create(ctx, "control", w)
	var unknown *registry.UnknownOutcome
	if !errors.As(err, &unknown) || !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	s.boundary = nil
	r, err := s.Read(t.Context(), "control")
	if err != nil {
		t.Fatal(err)
	}
	replay, err := s.Create(t.Context(), "control", w)
	if err != nil || r.Version != replay.Version {
		t.Fatal(replay, err)
	}
	for _, failAt := range []int{1, 2} {
		calls := 0
		fault := errors.New("fsync failed")
		s.fileSync = func(f *os.File) error {
			calls++
			if calls == failAt {
				return fault
			}
			return f.Sync()
		}
		var unavailable *registry.Unavailable
		if _, err = s.Read(t.Context(), "control"); !errors.As(err, &unavailable) || !errors.Is(err, fault) {
			t.Fatal("failed fsync exposed record", err)
		}
	}
}
