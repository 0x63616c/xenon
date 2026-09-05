package node

import (
	"bytes"
	"context"
	"encoding/binary"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/processcut"
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
	historyFamily
	nexusFamily
	matchingFamily
	queuev2Family
	executionFamily
	historyTasksFamily
	executionTasksFamily
	visibilityFamily
)

func belongs(outcome *wire.StoredOutcome, family outcomeFamily) bool {
	switch family {
	case visibilityFamily:
		return outcome.GetVisibilityResult() != nil
	case executionFamily:
		return outcome.GetExecutionResult() != nil
	case historyTasksFamily:
		return outcome.GetHistoryTasksResult() != nil
	case executionTasksFamily:
		return outcome.GetExecutionTasksResult() != nil
	case shardFamily:
		return outcome.GetShardResult() != nil
	case metadataFamily:
		return outcome.GetMetadataResult() != nil
	case clusterFamily:
		return outcome.GetClusterResult() != nil
	case queuev2Family:
		return outcome.GetQueuev2Result() != nil
	case matchingFamily:
		return outcome.GetMatchingResult() != nil
	case nexusFamily:
		return outcome.GetNexusResult() != nil
	case historyFamily:
		return outcome.GetHistoryResult() != nil
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
	if err = accountOutcome(tx, outcome, len(data)); err != nil {
		return nil, err
	}
	if err = put(tx, key, data); err != nil {
		return nil, err
	}
	counter := make([]byte, 8)
	binary.BigEndian.PutUint64(counter, count+1)
	if err = put(tx, "v1/outcome_count", counter); err != nil {
		return nil, err
	}
	if o.cut != nil {
		familyName := ""
		success := false
		if family == shardFamily {
			familyName = "shard"
			success = outcome.GetShardResult() != nil && outcome.GetShardResult().Error == wire.ShardResult_NONE
		}
		if family == executionFamily {
			familyName = "execution"
			success = outcome.GetExecutionResult() != nil && outcome.GetExecutionResult().Error == wire.ExecutionResult_NONE
		}
		o.cutReady = success && o.cut.Matches(id, familyName, o.config.Partition)
	}
	if o.cutReady {
		err = o.commitCut(tx)
	} else {
		err = commit(tx)
	}
	if err != nil {
		return nil, err
	}
	return outcome, nil
}

// cutStage is called only under the existing owner gate. A timed-out barrier
// marks the owner terminal before any native drain or gate release.
func (o *Owner) cutStage(stage string) error {
	if e := o.cut.Stage(stage); e != nil {
		o.quarantined.Store(true)
		return backend(e)
	}
	return nil
}
func (o *Owner) commitCut(tx *native.DbTransaction) error {
	if e := o.cut.Candidate(); e != nil {
		o.quarantined.Store(true)
		return backend(e)
	}
	optional, e := tx.Commit()
	if e != nil {
		return backend(e)
	}
	if optional == nil || *optional == nil {
		return status.Error(codes.Unavailable, "missing durability handle")
	}
	handle := *optional
	defer handle.Destroy()
	pauseErr := o.cutStage(processcut.BeforeAwait)
	// Even after timeout, retain the handle and drain the native durability call.
	// Outer Owner.Run may time out meanwhile, retaining this worker and its gate.
	if e = handle.AwaitDurable(); e != nil {
		return backend(e)
	}
	if pauseErr != nil {
		return pauseErr
	}
	return o.cutStage(processcut.AfterAwait)
}

// runJournalResult returns only the outcome of one fully durable journal call.
// Its callback runs before journal Commit, and no caller-supplied code executes
// after that commit under this gate. Thus the journal's nonempty durable write
// itself is the read/fencing barrier. Arbitrary exported Run keeps its trailing
// barrier, even if its callback happens to call journal before additional reads.
func (o *Owner) runJournalResult(ctx context.Context, id string, digest []byte, family outcomeFamily, apply func(*native.DbTransaction) (*wire.StoredOutcome, error)) (*wire.StoredOutcome, error) {
	raw, err := o.run(ctx, func(*native.Db) ([]byte, error) {
		outcome, err := o.journal(id, digest, family, apply)
		if err != nil {
			return nil, err
		}
		return proto.Marshal(outcome)
	}, true)
	if err != nil {
		return nil, err
	}
	result := new(wire.StoredOutcome)
	if err = proto.Unmarshal(raw, result); err != nil {
		return nil, backend(err)
	}
	return result, nil
}
