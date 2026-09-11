package integration

import (
	"crypto/sha256"
	"testing"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/routing"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestAcknowledgedQueueRecordOracle(t *testing.T) {
	command := queueWrite(1).Command
	message := &wire.QueueEntry{QueueType: command.QueueType, Data: command.Data, Encoding: "Proto3"}
	corrupt := proto.Clone(message).(*wire.QueueEntry)
	corrupt.Data = []byte("corrupt")
	wrongID := proto.Clone(message).(*wire.QueueEntry)
	wrongID.Id = 1
	for _, tc := range []struct {
		name   string
		result *wire.QueueResult
		wantOK bool
	}{
		{"preserved", &wire.QueueResult{Messages: []*wire.QueueEntry{message}}, true},
		{"missing", &wire.QueueResult{}, false},
		{"nil response", nil, false},
		{"duplicate application", &wire.QueueResult{Messages: []*wire.QueueEntry{message, wrongID}}, false},
		{"corrupt payload", &wire.QueueResult{Messages: []*wire.QueueEntry{corrupt}}, false},
		{"wrong sequence", &wire.QueueResult{Messages: []*wire.QueueEntry{wrongID}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := checkQueueRecord(tc.result, command); (err == nil) != tc.wantOK {
				t.Fatalf("oracle result: %v", err)
			}
		})
	}
}

func TestQueueWriteRetryPreservesEnvelope(t *testing.T) {
	first, retry := queueWrite(100), queueWrite(100)
	if !proto.Equal(first, retry) {
		t.Fatal("retry changed operation identity, command, or digest")
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(first.Command)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	if string(first.CommandSha256) != string(digest[:]) {
		t.Fatal("command digest differs from wire payload")
	}
	if first.OperationId == queueWrite(101).OperationId || first.Command.QueueType == queueWrite(101).Command.QueueType {
		t.Fatal("distinct writes collide")
	}
}

func TestStaleWriteOracleRequiresTypedRejection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		wantOK bool
	}{
		{"stale owner", routing.StaleOwner(), true},
		{"accepted", nil, false},
		{"timeout", status.Error(codes.DeadlineExceeded, "timeout"), false},
		{"untyped unavailable", status.Error(codes.Unavailable, "unavailable"), false},
		{"uncertain execution", routing.UnknownOutcome(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := checkStaleOwnerRejection(tc.err); (err == nil) != tc.wantOK {
				t.Fatalf("oracle result: %v", err)
			}
		})
	}
}
