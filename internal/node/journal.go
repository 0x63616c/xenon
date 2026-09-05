package node

import (
	"bytes"
	"encoding/binary"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	native "slatedb.io/slatedb-go/uniffi"
)

type outcomeFamily int

const (
	shardFamily outcomeFamily = iota
	metadataFamily
	clusterFamily
	queueFamily
)

func belongs(outcome *wire.StoredOutcome, family outcomeFamily) bool {
	switch family {
	case shardFamily:
		return outcome.GetShardResult() != nil
	case metadataFamily:
		return outcome.GetMetadataResult() != nil
	case clusterFamily:
		return outcome.GetClusterResult() != nil
	case queueFamily:
		return outcome.GetQueueResult() != nil
	}
	return false
}

// journal runs only inside Owner.Run. All families share capacity and IDs.
func (o *Owner) journal(id string, digest []byte, family outcomeFamily, apply func(*native.DbTransaction) (*wire.StoredOutcome, error)) (*wire.StoredOutcome, error) {
	tx, err := o.db.Begin(native.IsolationLevelSerializableSnapshot)
	if err != nil {
		return nil, backend(err)
	}
	defer tx.Destroy()
	key := "v1/outcome/" + id
	saved, err := get(tx, key)
	if err != nil {
		return nil, err
	}
	if saved != nil {
		outcome := &wire.StoredOutcome{}
		if err = proto.Unmarshal(saved, outcome); err != nil {
			return nil, backend(err)
		}
		if !bytes.Equal(outcome.CommandSha256, digest) {
			return nil, status.Error(codes.InvalidArgument, "operation ID reused with different command")
		}
		if !belongs(outcome, family) {
			return nil, status.Error(codes.InvalidArgument, "operation ID belongs to another family")
		}
		old, err := get(tx, "v1/barrier")
		if err != nil {
			return nil, err
		}
		marker := byte(1)
		if bytes.Equal(old, []byte{1}) {
			marker = 0
		}
		if err = put(tx, "v1/barrier", []byte{marker}); err != nil {
			return nil, err
		}
		if err = commit(tx); err != nil {
			return nil, err
		}
		return outcome, nil
	}
	raw, err := get(tx, "v1/outcome_count")
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
	if count >= o.config.MaxOutcomes {
		return nil, status.Error(codes.ResourceExhausted, "durable outcome capacity reached; no unsafe expiry")
	}
	outcome, err := apply(tx)
	if err != nil {
		return nil, err
	}
	outcome.CommandSha256 = digest
	data, err := proto.Marshal(outcome)
	if err != nil {
		return nil, backend(err)
	}
	if err = put(tx, key, data); err != nil {
		return nil, err
	}
	counter := make([]byte, 8)
	binary.BigEndian.PutUint64(counter, count+1)
	if err = put(tx, "v1/outcome_count", counter); err != nil {
		return nil, err
	}
	if err = commit(tx); err != nil {
		return nil, err
	}
	return outcome, nil
}
