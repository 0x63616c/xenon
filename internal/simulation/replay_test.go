package simulation

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/replay"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// This synchronous scenario drives production journal decisions. It does not
// simulate Temporal, native SlateDB, routing, or ownership-manager decisions.
// The fault schedule is explicit code (not dependent on a PRNG implementation).
type disk struct {
	durable     map[string][]byte
	dropOutcome bool
	commits     int
}
type transaction struct {
	disk   *disk
	staged map[string][]byte
}

func copyState(s map[string][]byte) map[string][]byte {
	r := map[string][]byte{}
	for k, v := range s {
		r[k] = bytes.Clone(v)
	}
	return r
}
func (d *disk) begin() *transaction { return &transaction{d, copyState(d.durable)} }
func (tx *transaction) effects() replay.Effects {
	return replay.Effects{
		Get: func(k string) ([]byte, error) { return bytes.Clone(tx.staged[k]), nil },
		Put: func(k string, v []byte) error { tx.staged[k] = bytes.Clone(v); return nil },
		Apply: func() (*wire.StoredOutcome, error) {
			n := byte(0)
			if b := tx.staged["value"]; len(b) > 0 {
				n = b[0]
			}
			n++
			tx.staged["value"] = []byte{n}
			return &wire.StoredOutcome{Result: &wire.StoredOutcome_ShardResult{ShardResult: &wire.ShardResult{Data: []byte{n}}}}, nil
		},
		Account: func(_ *wire.StoredOutcome, _ int) error { return nil },
		Belongs: func(o *wire.StoredOutcome) bool { return o.GetShardResult() != nil },
		Commit: func(_ *wire.StoredOutcome) error {
			tx.disk.durable = copyState(tx.staged)
			if tx.disk.dropOutcome {
				delete(tx.disk.durable, "v1/outcome/stable-operation")
			}
			tx.disk.commits++
			return nil
		},
	}
}

// check is deliberately independent of the replay implementation: it asserts a
// known arithmetic result, one retained journal record, and an actual replay
// commit. A double application violates the externally expected value of one.
func check(d *disk, result *wire.StoredOutcome) error {
	if result == nil || !bytes.Equal(result.GetShardResult().Data, []byte{1}) || !bytes.Equal(d.durable["value"], []byte{1}) {
		return fmt.Errorf("mutation was not exactly once")
	}
	count := d.durable["v1/outcome_count"]
	if len(count) != 8 || binary.BigEndian.Uint64(count) != 1 || len(d.durable["v1/outcome/stable-operation"]) == 0 {
		return fmt.Errorf("durable result/count missing")
	}
	if d.commits != 2 || len(d.durable["v1/barrier"]) != 1 {
		return fmt.Errorf("replay durability barrier missing")
	}
	return nil
}
func scenario(t *testing.T, broken bool) error {
	t.Helper()
	d := &disk{durable: map[string][]byte{}, dropOutcome: broken}
	digest := sha256.Sum256([]byte("increment"))
	// 1. Begin and durably commit production mutation + outcome.
	owner := d.begin()
	_, err := replay.Run(owner.effects(), "stable-operation", digest[:], 10)
	if err != nil {
		t.Fatal(err)
	}
	// 2. Lose response; 3. crash discards transaction and all volatile staging.
	owner.staged["uncommitted"] = []byte("discard on crash")
	owner = nil
	// 4. Replacement starts from durable state; 5. same request retries.
	replacement := d.begin()
	if replacement.staged["uncommitted"] != nil {
		t.Fatal("volatile state survived crash")
	}
	result, err := replay.Run(replacement.effects(), "stable-operation", digest[:], 10)
	if err != nil {
		t.Fatal(err)
	}
	// 6. Independent oracle checks recovered outcome and atomicity.
	if err = check(d, result); err != nil {
		return err
	}
	changed := sha256.Sum256([]byte("different command"))
	tx := d.begin()
	_, err = replay.Run(tx.effects(), "stable-operation", changed[:], 10)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal("changed digest accepted", err)
	}
	if d.commits != 2 {
		t.Fatal("rejected request committed")
	}
	return nil
}
func TestProductionReplayLostResponseCrash(t *testing.T) {
	if err := scenario(t, false); err != nil {
		t.Fatal(err)
	}
}
func TestCheckerRejectsNonAtomicOutcome(t *testing.T) {
	if err := scenario(t, true); err == nil {
		t.Fatal("checker accepted broken storage")
	}
}
