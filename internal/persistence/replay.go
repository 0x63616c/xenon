// Durable replay decisions belong to the production persistence service.
// Effects execute under the caller's single transaction and ownership admission.
package persistence

import (
	"bytes"
	"encoding/binary"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ReplayEffects are synchronous completion boundaries. Commit must make every staged
// write durable atomically before success. A nil outcome denotes replay and
// still requires the nonempty barrier commit. Errors may have uncertain effects.
type ReplayEffects struct {
	Get     func(string) ([]byte, error)
	Put     func(string, []byte) error
	Apply   func() (*wire.StoredOutcome, error)
	Account func(*wire.StoredOutcome, int) error
	Belongs func(*wire.StoredOutcome) bool
	Commit  func(*wire.StoredOutcome) error
}

func replayBackend(err error) error {
	return status.Errorf(codes.Unavailable, "storage outcome unknown: %v", err)
}

// RunReplay shares the stored ID namespace across families. Request-shape and digest
// calculation validation remain at the production RPC boundary.
func RunReplay(effects ReplayEffects, id string, digest []byte, limit uint64) (*wire.StoredOutcome, error) {
	var err error
	key := "v1/outcome/" + id
	saved, err := effects.Get(key)
	if err != nil {
		return nil, err
	}
	if saved != nil {
		outcome := &wire.StoredOutcome{}
		if err = proto.Unmarshal(saved, outcome); err != nil {
			return nil, replayBackend(err)
		}
		if !bytes.Equal(outcome.CommandSha256, digest) {
			return nil, status.Error(codes.InvalidArgument, "operation ID reused with different command")
		}
		if !effects.Belongs(outcome) {
			return nil, status.Error(codes.InvalidArgument, "operation ID belongs to another family")
		}
		old, err := effects.Get("v1/barrier")
		if err != nil {
			return nil, err
		}
		marker := byte(1)
		if bytes.Equal(old, []byte{1}) {
			marker = 0
		}
		if err = effects.Put("v1/barrier", []byte{marker}); err != nil {
			return nil, err
		}
		if err = effects.Commit(nil); err != nil {
			return nil, err
		}
		return outcome, nil
	}
	raw, err := effects.Get("v1/outcome_count")
	if err != nil {
		return nil, err
	}
	var count uint64
	if raw != nil {
		if len(raw) != 8 {
			return nil, status.Error(codes.Unavailable, "corrupt outcome count")
		}
		count = binary.BigEndian.Uint64(raw)
	}
	if count >= limit {
		return nil, status.Error(codes.ResourceExhausted, "durable outcome capacity reached; no unsafe expiry")
	}
	outcome, err := effects.Apply()
	if err != nil {
		return nil, err
	}
	outcome.CommandSha256 = digest
	data, err := proto.Marshal(outcome)
	if err != nil {
		return nil, replayBackend(err)
	}
	if err = effects.Account(outcome, len(data)); err != nil {
		return nil, err
	}
	if err = effects.Put(key, data); err != nil {
		return nil, err
	}
	counter := make([]byte, 8)
	binary.BigEndian.PutUint64(counter, count+1)
	if err = effects.Put("v1/outcome_count", counter); err != nil {
		return nil, err
	}
	if err = effects.Commit(outcome); err != nil {
		return nil, err
	}
	return outcome, nil
}
