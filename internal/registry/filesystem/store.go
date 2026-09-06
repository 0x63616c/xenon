//go:build linux || darwin

// Package filesystem implements the registry contract on a shared directory.
// Every participant must use this protocol; qualification is mount-specific.
package filesystem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/0x63616c/xenon/internal/registry"
	"golang.org/x/sys/unix"
)

// Config requires an existing, durably provisioned, privately managed directory. Never unlink or
// replace its .lock files, including during cleanup while participants run.
// Wait is the caller's context-aware contention backoff (e.g. its injected clock).
// Operations do not spawn goroutines: the calling driver retains ownership until
// OS I/O completes. Cancellation cannot interrupt an in-flight filesystem syscall.
type Config struct {
	Directory      string
	MaxRecordBytes int64 // encoded envelope limit, excluding the 16-byte disk header
	Wait           func(context.Context) error
}
type Store struct {
	root *os.Root
	max  int64
	wait func(context.Context) error
	// boundary observes completed protocol steps; nil in production. Tests inject
	// failures/process cuts here, without substituting locks, rename or filesystem.
	boundary func(string) error
	fileSync func(*os.File) error
}

var _ registry.Store = (*Store)(nil)

func New(c Config) (*Store, error) {
	if c.Directory == "" || c.MaxRecordBytes <= 0 || c.MaxRecordBytes > math.MaxInt64-17 || c.Wait == nil {
		return nil, &registry.Invalid{Reason: "directory, positive bounded record limit and contention wait required"}
	}
	root, err := os.OpenRoot(c.Directory)
	if err != nil {
		return nil, err
	}
	return &Store{root: root, max: c.MaxRecordBytes, wait: c.Wait, fileSync: (*os.File).Sync}, nil
}

// Close requires all operations to have completed.
func (s *Store) Close() error { return s.root.Close() }
func (s *Store) step(name string) error {
	if s.boundary != nil {
		return s.boundary(name)
	}
	return nil
}
func filename(key registry.Key) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}
func (s *Store) open(name string, flag int) (*os.File, error) {
	f, err := s.root.OpenFile(name, flag|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err == nil && !st.Mode().IsRegular() {
		err = errors.New("registry path is not a regular file")
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}
func (s *Store) lock(ctx context.Context, key registry.Key) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := s.open(filename(key)+".lock", os.O_CREATE|os.O_RDWR)
	if err != nil {
		return nil, err
	}
	for {
		if err = ctx.Err(); err != nil {
			f.Close()
			return nil, err
		}
		err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return f, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EINTR) {
			f.Close()
			return nil, err
		}
		if err = s.wait(ctx); err != nil {
			f.Close()
			return nil, err
		}
	}
}
func (s *Store) syncDir() error {
	f, err := s.root.Open(".")
	if err != nil {
		return err
	}
	defer f.Close()
	if err = s.fileSync(f); err != nil {
		return err
	}
	return s.step("directory-synced")
}

// disk format: eight-byte magic followed by a big-endian nonzero generation and
// the unchanged canonical registry envelope. No deletes, wrap or counter reset.
const magic = "XENREG01"

func (s *Store) readLocked(key registry.Key) (registry.Record, uint64, error) {
	f, err := s.open(filename(key)+".record", os.O_RDWR)
	if errors.Is(err, os.ErrNotExist) {
		return registry.Record{}, 0, &registry.NotFound{Key: key}
	}
	if err != nil {
		return registry.Record{}, 0, &registry.Unavailable{Key: key, Cause: err}
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, s.max+17))
	if err != nil {
		return registry.Record{}, 0, &registry.Unavailable{Key: key, Cause: err}
	}
	corrupt := func(e error) (registry.Record, uint64, error) {
		return registry.Record{}, 0, &registry.Corrupt{Key: key, Cause: e}
	}
	if len(b) < 16 || int64(len(b)) > s.max+16 || string(b[:8]) != magic {
		return corrupt(errors.New("invalid disk header or length"))
	}
	generation := binary.BigEndian.Uint64(b[8:16])
	if generation == 0 {
		return corrupt(errors.New("zero generation"))
	}
	r := registry.Record{Body: bytes.Clone(b[16:]), Version: registry.Version(fmt.Sprintf("fs1:%d", generation))}
	env, decodeErr := registry.Decode(key, r)
	if decodeErr != nil {
		return registry.Record{}, 0, decodeErr
	}
	wantExpected := ""
	if generation > 1 {
		wantExpected = fmt.Sprintf("fs1:%d", generation-1)
	}
	if string(env.Expected) != wantExpected {
		return corrupt(errors.New("generation does not match envelope predecessor"))
	}
	// A dead writer may have renamed but not synced the directory. An exclusive
	// reader establishes durability before exposing the recovered publication.
	if err = s.fileSync(f); err == nil {
		err = s.step("read-file-synced")
	}
	if err == nil {
		err = s.syncDir()
	}
	if err != nil {
		return registry.Record{}, 0, &registry.Unavailable{Key: key, Cause: err}
	}
	return r, generation, nil
}
func (s *Store) Read(ctx context.Context, key registry.Key) (registry.Record, error) {
	if err := registry.ValidateKey(key); err != nil {
		return registry.Record{}, err
	}
	l, err := s.lock(ctx, key)
	if err != nil {
		return registry.Record{}, &registry.Unavailable{Key: key, Cause: err}
	}
	defer l.Close()
	r, _, err := s.readLocked(key)
	if err == nil && ctx.Err() != nil {
		err = &registry.Unavailable{Key: key, Cause: ctx.Err()}
	}
	if err != nil {
		return registry.Record{}, err
	}
	return r, nil
}
func (s *Store) Create(ctx context.Context, key registry.Key, w registry.Write) (registry.Record, error) {
	w.Body = bytes.Clone(w.Body)
	if err := registry.ValidateCreate(key, w); err != nil {
		return registry.Record{}, err
	}
	return s.publish(ctx, key, "", w)
}
func (s *Store) Replace(ctx context.Context, key registry.Key, expected registry.Version, w registry.Write) (registry.Record, error) {
	w.Body = bytes.Clone(w.Body)
	if err := registry.ValidateReplace(key, expected, w); err != nil {
		return registry.Record{}, err
	}
	return s.publish(ctx, key, expected, w)
}
func (s *Store) publish(ctx context.Context, key registry.Key, expected registry.Version, w registry.Write) (registry.Record, error) {
	body, err := registry.Encode(key, expected, w)
	if err != nil {
		return registry.Record{}, err
	}
	if int64(len(body)) > s.max {
		return registry.Record{}, &registry.Invalid{Key: key, Reason: "record exceeds configured limit"}
	}
	unavailable := func(e error) (registry.Record, error) {
		return registry.Record{}, &registry.Unavailable{Key: key, Cause: e}
	}
	unknown := func(e error) (registry.Record, error) {
		return registry.Record{}, &registry.UnknownOutcome{Key: key, Transition: w.Transition, Cause: e}
	}
	l, err := s.lock(ctx, key)
	if err != nil {
		return unavailable(err)
	}
	defer l.Close()
	current, generation, err := s.readLocked(key)
	var missing *registry.NotFound
	if err != nil && !errors.As(err, &missing) {
		return registry.Record{}, err
	}
	if err == nil {
		env, _ := registry.Decode(key, current)
		if env.Transition == w.Transition {
			resolution, e := registry.Reconcile(key, expected, w, current, nil)
			if e != nil {
				return registry.Record{}, e
			}
			if resolution == registry.Published {
				if ctx.Err() != nil {
					return unknown(ctx.Err())
				}
				return current, nil
			}
		}
	}
	if expected == "" && err == nil || expected != "" && (err != nil || current.Version != expected) {
		return registry.Record{}, &registry.Conflict{Key: key}
	}
	if generation == math.MaxUint64 {
		return registry.Record{}, &registry.Corrupt{Key: key, Cause: errors.New("generation exhausted")}
	}
	if err = ctx.Err(); err != nil {
		return unavailable(err)
	}
	if err = s.step("condition-checked"); err != nil {
		return unavailable(err)
	}
	b := make([]byte, 16, len(body)+16)
	copy(b, magic)
	binary.BigEndian.PutUint64(b[8:], generation+1)
	b = append(b, body...)
	name := filename(key)
	// One scratch path per key is safe under the stable lock and bounds leftovers
	// after process death. A prior incomplete scratch file is never authoritative.
	f, err := s.open(name+".tmp", os.O_CREATE|os.O_TRUNC|os.O_RDWR)
	if err != nil {
		return unavailable(err)
	}
	defer f.Close()
	defer s.root.Remove(name + ".tmp")
	if _, err = f.Write(b); err != nil {
		return unavailable(err)
	}
	if err = s.fileSync(f); err != nil {
		return unavailable(err)
	}
	if err = s.step("write-file-synced"); err != nil {
		return unavailable(err)
	}
	if err = ctx.Err(); err != nil {
		return unavailable(err)
	}
	// From rename dispatch onward publication may have happened, even on error.
	if err = s.root.Rename(name+".tmp", name+".record"); err != nil {
		return unknown(err)
	}
	if err = s.step("renamed"); err != nil {
		return unknown(err)
	}
	if err = s.syncDir(); err != nil {
		return unknown(err)
	}
	if err = ctx.Err(); err != nil {
		return unknown(err)
	}
	return registry.Record{Body: bytes.Clone(body), Version: registry.Version(fmt.Sprintf("fs1:%d", generation+1))}, nil
}
