package adapter

import (
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/rpctrace"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"time"
)

// ExecutionStore composes all pinned low-level methods over one connection.
// This interface completeness is not a claim of Temporal boot or shipping proof.
type ExecutionStore struct {
	*WorkflowStore
	*HistoryStore
	*HistoryTasksStore
	*ExecutionTasksStore
	connection *grpc.ClientConn
}

var _ p.ExecutionStore = (*ExecutionStore)(nil)

func NewExecutionStore(address, partition string) (*ExecutionStore, error) {
	c, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithChainUnaryInterceptor(rpctrace.Unary))
	if e != nil {
		return nil, e
	}
	timeout := 30 * time.Second
	return &ExecutionStore{
		WorkflowStore:       &WorkflowStore{connection: c, client: wire.NewExecutionPersistenceClient(c), partition: partition, invocationTimeout: timeout},
		HistoryStore:        &HistoryStore{connection: c, client: wire.NewHistoryPersistenceClient(c), partition: partition, invocationTimeout: timeout, HistoryBranchUtilImpl: p.NewHistoryBranchUtil(serialization.NewSerializer())},
		HistoryTasksStore:   &HistoryTasksStore{connection: c, client: wire.NewHistoryTasksPersistenceClient(c), partition: partition, invocationTimeout: timeout},
		ExecutionTasksStore: &ExecutionTasksStore{connection: c, client: wire.NewExecutionTasksPersistenceClient(c), partition: partition, invocationTimeout: timeout}, connection: c}, nil
}
func (s *ExecutionStore) GetName() string { return "xenon" }
func (s *ExecutionStore) Close() {
	if s.connection != nil {
		_ = s.connection.Close()
	}
}
