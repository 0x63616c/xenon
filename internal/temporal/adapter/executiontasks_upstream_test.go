package adapter

import (
	"encoding/json"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/serialization"
	persistencetests "go.temporal.io/server/common/persistence/tests"
	"math/rand"
	"os"
	"testing"
)

func TestExecutionTasksUpstream(t *testing.T) {
	raw, e := os.ReadFile("../../../proof/executiontasks/case.json")
	if e != nil {
		t.Fatal(e)
	}
	var fixture struct {
		namespaceCase
		Seed int64 `json:"seed"`
	}
	if e = json.Unmarshal(raw, &fixture); e != nil || fixture.SchemaVersion != 1 {
		t.Fatal(e)
	}
	t.Setenv("GODEBUG", "randseednop=0")
	rand.Seed(fixture.Seed)
	address := startNamespaceNode(t, fixture.namespaceCase)
	store, e := NewExecutionStore(address, fixture.Partition)
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	shard, e := NewShardStore(address, fixture.Partition, "upstream")
	if e != nil {
		t.Fatal(e)
	}
	defer shard.Close()
	suite.Run(t, persistencetests.NewExecutionMutableStateTaskSuite(t, shard, store, serialization.NewSerializer(), log.NewNoopLogger()))
}
