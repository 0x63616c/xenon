package adapter

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	wire "github.com/0x63616c/xenon/api/xenon/v1"
	"github.com/0x63616c/xenon/internal/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type recordedOperationSource struct {
	calls int
	err   error
}

func (s *recordedOperationSource) NewID(prefix string) (string, error) {
	s.calls++
	if s.err != nil {
		return "", s.err
	}
	return fmt.Sprintf("%s_%022d", prefix, s.calls), nil
}

type retryOperationClient struct {
	wire.ShardPersistenceClient
	seen []*wire.ShardRequest
}

func (c *retryOperationClient) Execute(_ context.Context, r *wire.ShardRequest, _ ...grpc.CallOption) (*wire.ShardResult, error) {
	c.seen = append(c.seen, proto.Clone(r).(*wire.ShardRequest))
	if len(c.seen) == 1 {
		return nil, status.Error(codes.Unavailable, "lost response")
	}
	return &wire.ShardResult{ShardId: r.Command.ShardId}, nil
}
func TestAdapterOperationIDRetainedAcrossRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &recordedOperationSource{}
		s, err := NewShardStore("passthrough:///unused", "p", "c", WithOperationIDSource(source))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		client := &retryOperationClient{}
		s.client = client
		if _, err = s.invoke(context.Background(), &wire.ShardCommand{Kind: wire.ShardCommand_GET, ShardId: 7}); err != nil {
			t.Fatal(err)
		}
		if source.calls != 1 || len(client.seen) != 2 || !proto.Equal(client.seen[0], client.seen[1]) || identity.OperationID(client.seen[0].OperationId).Validate() != nil {
			t.Fatalf("identity/digest changed across retries: source=%d requests=%v", source.calls, client.seen)
		}
	})
}
func TestAdapterOperationEntropyFailureStopsEveryFamily(t *testing.T) {
	cause := errors.New("injected entropy failure")
	for _, family := range []string{"shard", "history", "historytasks", "execution", "executiontasks", "metadata", "cluster", "queue", "queuev2", "matching", "visibility", "nexus"} {
		t.Run(family, func(t *testing.T) {
			source := &recordedOperationSource{err: cause}
			ids := newOperationIDs(WithOperationIDSource(source))
			ctx := context.Background()
			var err error
			// Nil clients make any attempted submission a test failure. These are all
			// production family invocation paths, not a substitute generator-only test.
			switch family {
			case "shard":
				_, err = (&ShardStore{operations: ids, invocationTimeout: time.Second}).invoke(ctx, &wire.ShardCommand{})
			case "history":
				_, err = (&HistoryStore{operations: ids, invocationTimeout: time.Second}).invokeHistory(ctx, &wire.HistoryCommand{})
			case "historytasks":
				_, err = (&HistoryTasksStore{operations: ids, invocationTimeout: time.Second}).invokeHistoryTasks(ctx, &wire.HistoryTasksCommand{})
			case "execution":
				_, err = (&WorkflowStore{operations: ids, invocationTimeout: time.Second}).invokeExecution(ctx, &wire.ExecutionCommand{})
			case "executiontasks":
				_, err = (&ExecutionTasksStore{operations: ids, invocationTimeout: time.Second}).invokeExecutionTasks(ctx, &wire.ExecutionTasksCommand{})
			case "metadata":
				_, err = (&MetadataStore{operations: ids, invocationTimeout: time.Second}).invokeMetadata(ctx, &wire.MetadataCommand{})
			case "cluster":
				_, err = (&ClusterStore{operations: ids, invocationTimeout: time.Second}).invokeCluster(ctx, &wire.ClusterCommand{})
			case "queue":
				_, err = (&Queue{operations: ids, invocationTimeout: time.Second}).invokeQueue(ctx, &wire.QueueCommand{})
			case "queuev2":
				_, err = (&QueueV2{operations: ids}).invoke(ctx, &wire.QueueV2Command{})
			case "matching":
				_, err = (&MatchingStore{operations: ids, invocationTimeout: time.Second}).invokeMatching(ctx, &wire.MatchingCommand{})
			case "visibility":
				_, err = (&VisibilityStore{operations: ids}).invokeVisibility(ctx, "p", &wire.VisibilityCommand{})
			case "nexus":
				_, err = (&NexusStore{operations: ids, invocationTimeout: time.Second}).invokeNexus(ctx, &wire.NexusCommand{})
			}
			if !errors.Is(err, cause) || source.calls != 1 {
				t.Fatalf("entropy failure flattened or repeated: %v calls=%d", err, source.calls)
			}
		})
	}
}
func TestAdapterDefaultOperationIDAndCompositeSource(t *testing.T) {
	var unset *operationIDs
	id, err := unset.next()
	if err != nil || identity.OperationID(id).Validate() != nil {
		t.Fatal(id, err)
	}
	source := &recordedOperationSource{}
	s, err := NewExecutionStore("passthrough:///unused", "p", WithOperationIDSource(source))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	seen := map[string]bool{}
	for _, ids := range []*operationIDs{s.WorkflowStore.operations, s.HistoryStore.operations, s.HistoryTasksStore.operations, s.ExecutionTasksStore.operations} {
		id, err := ids.next()
		if err != nil || seen[id] {
			t.Fatal("composite source not shared", id, err)
		}
		seen[id] = true
	}
	if source.calls != 4 {
		t.Fatal(source.calls)
	}
}
