package persistence

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math"
	"testing"
)

func TestOutcomeUsagePreservesLegacyAccountingAndJSON(t *testing.T) {
	count := make([]byte, 8)
	binary.BigEndian.PutUint64(count, 3)
	tx := &testTx{data: map[string][]byte{"v1/outcome_count": count, "v1/outcome_usage": []byte(`{"shard_result":{"entries":2,"encoded_outcome_bytes":31}}`)}}
	got, err := ReadOutcomeUsage(context.Background(), tx, "history-0", 5)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(got)
	const want = `{"partition":"history-0","limit":5,"entries":3,"remaining":2,"encoded_outcome_bytes":31,"unaccounted_legacy_entries":1,"accounting_complete":false,"families":{"shard_result":{"entries":2,"encoded_outcome_bytes":31}}}`
	if string(raw) != want {
		t.Fatal(string(raw))
	}
	if len(tx.data) != 2 {
		t.Fatal("inspection staged writes", tx.data)
	}
}
func TestOutcomeUsageRejectsCorruptBoundedAccounting(t *testing.T) {
	for _, tc := range []struct {
		name  string
		count []byte
		usage string
	}{
		{"count", []byte{1}, `{}`},
		{"null", make([]byte, 8), `null`},
		{"excess", make([]byte, 8), `{"shard_result":{"entries":1}}`},
		{"overflow", binary.BigEndian.AppendUint64(nil, math.MaxUint64), `{"shard_result":{"entries":18446744073709551615},"history_result":{"entries":1}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := &testTx{data: map[string][]byte{"v1/outcome_count": tc.count, "v1/outcome_usage": []byte(tc.usage)}}
			if out, err := ReadOutcomeUsage(context.Background(), tx, "p", 5); out != nil || status.Code(err) != codes.Unavailable {
				t.Fatal(out, err)
			}
		})
	}
}
