// Package contracttest runs reusable registry assertions against an empty,
// exclusively owned backend namespace. It does not qualify a storage platform.
package contracttest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/registry"
)

// Write is a deterministic fixture constructor, never a production ID source.
func Write(t *testing.T, key registry.Key, expected registry.Version, id int, body string) registry.Write {
	t.Helper()
	w, err := registry.NewWrite(key, expected, identity.TransitionID(fmt.Sprintf("trn_%022d", id)), []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	return w
}

// Run uses fresh keys below contract/. The caller owns context budget, setup,
// namespace isolation and teardown; the same suite runs on real backend clients.
func Run(t *testing.T, ctx context.Context, s registry.Store) {
	t.Helper()
	t.Run("create_race", func(t *testing.T) {
		key := registry.Key("contract/race")
		var missing *registry.NotFound
		if _, err := s.Read(ctx, key); !errors.As(err, &missing) {
			t.Fatalf("missing: %v", err)
		}
		var wg sync.WaitGroup
		results := make(chan error, 8)
		for i := 1; i <= 8; i++ {
			w := Write(t, key, "", i, "race")
			wg.Add(1)
			go func() { defer wg.Done(); _, err := s.Create(ctx, key, w); results <- err }()
		}
		wg.Wait()
		close(results)
		success := 0
		for err := range results {
			var conflict *registry.Conflict
			if err == nil {
				success++
			} else if !errors.As(err, &conflict) {
				t.Fatal(err)
			}
		}
		if success != 1 {
			t.Fatalf("create winners=%d", success)
		}
		if r, err := s.Read(ctx, key); err != nil || r.Version == "" {
			t.Fatal(r, err)
		}
	})
	t.Run("replace_replay_no_aba", func(t *testing.T) {
		key := registry.Key("contract/replace")
		a := Write(t, key, "", 20, "same")
		first, err := s.Create(ctx, key, a)
		if err != nil {
			t.Fatal(err)
		}
		replay, err := s.Create(ctx, key, a)
		if err != nil || replay.Version != first.Version || !bytes.Equal(replay.Body, first.Body) {
			t.Fatal("create replay", err)
		}
		changed := Write(t, key, "", 20, "changed")
		var invalid *registry.Invalid
		if _, err = s.Create(ctx, key, changed); !errors.As(err, &invalid) {
			t.Fatal("changed digest", err)
		}
		b := Write(t, key, first.Version, 21, "same")
		second, err := s.Replace(ctx, key, first.Version, b)
		if err != nil || second.Version == first.Version || second.Version == "" {
			t.Fatal("replace", err)
		}
		replay, err = s.Replace(ctx, key, first.Version, b)
		if err != nil || replay.Version != second.Version {
			t.Fatal("replace replay", err)
		}
		if _, err = s.Replace(ctx, key, second.Version, Write(t, key, second.Version, 21, "same")); !errors.As(err, &invalid) {
			t.Fatal("reused transition", err)
		}
		third, err := s.Replace(ctx, key, second.Version, Write(t, key, second.Version, 22, "same"))
		if err != nil || third.Version == first.Version || third.Version == second.Version {
			t.Fatal("ABA", err)
		}
		var conflict *registry.Conflict
		if _, err = s.Replace(ctx, key, first.Version, Write(t, key, first.Version, 23, "stale")); !errors.As(err, &conflict) {
			t.Fatal("stale CAS", err)
		}
		// Old transition IDs cannot restore an old envelope: condition binds the bytes.
		fourth, err := s.Replace(ctx, key, third.Version, Write(t, key, third.Version, 20, "same"))
		if err != nil || fourth.Version == first.Version {
			t.Fatal("historical identity ABA", err)
		}
		first.Body[0] = 'x'
		b.Body[0] = 'x'
		fourth.Body[0] = 'x'
		fresh, err := s.Read(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		e, err := registry.Decode(key, fresh)
		if err != nil || string(e.Body) != "same" {
			t.Fatal("borrowed bytes", err)
		}
		fresh.Body[0] = 'x'
		again, err := s.Read(ctx, key)
		if err != nil || again.Body[0] != '{' {
			t.Fatal("borrowed read", err)
		}
	})
	t.Run("replace_race", func(t *testing.T) {
		key := registry.Key("contract/replace_race")
		first, err := s.Create(ctx, key, Write(t, key, "", 30, "start"))
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		results := make(chan error, 8)
		for i := 31; i < 39; i++ {
			w := Write(t, key, first.Version, i, "next")
			wg.Add(1)
			go func() { defer wg.Done(); _, err := s.Replace(ctx, key, first.Version, w); results <- err }()
		}
		wg.Wait()
		close(results)
		success := 0
		for err := range results {
			var conflict *registry.Conflict
			if err == nil {
				success++
			} else if !errors.As(err, &conflict) {
				t.Fatal(err)
			}
		}
		if success != 1 {
			t.Fatalf("replace winners=%d", success)
		}
	})
	t.Run("validation_cancellation", func(t *testing.T) {
		key := registry.Key("contract/validation")
		w := Write(t, key, "", 40, "body")
		var invalid *registry.Invalid
		for _, err := range []error{func() error { _, e := s.Replace(ctx, key, "", w); return e }(), func() error { _, e := s.Create(ctx, "../escape", w); return e }(), func() error { _, e := s.Create(ctx, "other", w); return e }()} {
			if !errors.As(err, &invalid) {
				t.Fatal("invalid accepted", err)
			}
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		var unavailable *registry.Unavailable
		if _, err := s.Create(canceled, key, w); !errors.As(err, &unavailable) || !errors.Is(err, context.Canceled) {
			t.Fatal("predispatch cancel", err)
		}
		var missing *registry.NotFound
		if _, err := s.Read(ctx, key); !errors.As(err, &missing) {
			t.Fatal("validation published", err)
		}
	})
}
