package persistence

import (
	"errors"
	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestReplayRetainsStoredOutcomeButRequiresDurableBarrier(t *testing.T) {
	stored := &wire.StoredOutcome{CommandSha256: []byte("digest"), Result: &wire.StoredOutcome_ShardResult{ShardResult: &wire.ShardResult{RangeId: 42}}}
	encoded, _ := proto.Marshal(stored)
	data := map[string][]byte{"v1/outcome/legacy-operation": encoded, "v1/barrier": {1}}
	failed := errors.New("durability unknown")
	commits := 0
	effects := ReplayEffects{
		Get: func(k string) ([]byte, error) { return data[k], nil },
		Put: func(k string, v []byte) error {
			if k != "v1/barrier" {
				t.Fatal("replay changed persisted application state", k)
			}
			data[k] = v
			return nil
		},
		Apply:   func() (*wire.StoredOutcome, error) { t.Fatal("replay reapplied mutation"); return nil, nil },
		Account: func(*wire.StoredOutcome, int) error { t.Fatal("replay consumed capacity"); return nil },
		Belongs: func(o *wire.StoredOutcome) bool { return o.GetShardResult() != nil },
		Commit: func(o *wire.StoredOutcome) error {
			commits++
			if o != nil {
				t.Fatal("replay became fresh write")
			}
			return failed
		},
	}
	out, err := RunReplay(effects, "legacy-operation", []byte("digest"), 0)
	if out != nil || !errors.Is(err, failed) || commits != 1 || data["v1/barrier"][0] != 0 {
		t.Fatal(out, err, commits, data)
	}
	effects.Commit = func(*wire.StoredOutcome) error { commits++; return nil }
	out, err = RunReplay(effects, "legacy-operation", []byte("digest"), 0)
	if err != nil || !proto.Equal(out, stored) || commits != 2 || data["v1/barrier"][0] != 1 {
		t.Fatal(out, err, commits)
	}
	for _, mismatch := range []bool{false, true} {
		digest := []byte("changed")
		if mismatch {
			digest = []byte("digest")
			effects.Belongs = func(*wire.StoredOutcome) bool { return false }
		}
		out, err = RunReplay(effects, "legacy-operation", digest, 0)
		if out != nil || status.Code(err) != codes.InvalidArgument || commits != 2 {
			t.Fatal("mismatch performed durability work", out, err, commits)
		}
	}
}
