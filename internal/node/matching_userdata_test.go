package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"github.com/0x63616c/xenon/internal/adapter"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	suites "go.temporal.io/server/common/persistence/tests"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"net"
	"os"
	native "slatedb.io/slatedb-go/uniffi"
	"strings"
	"testing"
	"time"
)

func matchingStore(t *testing.T) *adapter.MatchingStore { return matchingStoreMode(t, false) }
func matchingStoreMode(t *testing.T, fair bool) *adapter.MatchingStore {
	b := native.NewDbBuilder(cfg(t).Prefix+"-userdata", objects(t))
	defer b.Destroy()
	settings := native.SettingsDefault()
	defer settings.Destroy()
	// Upstream's 30s test deadline includes1024 one-row durable RPC reads.
	// A declared1ms WAL flush keeps those real durability barriers inside its budget.
	raw, err := os.ReadFile("../../proof/go-matching/userdata.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Schema int
		Flush  string `json:"flush_interval"`
	}
	if err = json.Unmarshal(raw, &fixture); err != nil || fixture.Schema != 1 || fixture.Flush != "1ms" {
		t.Fatal("invalid user-data fixture", err)
	}
	value, _ := json.Marshal(fixture.Flush)
	if e := settings.Set("flush_interval", string(value)); e != nil {
		t.Fatal(e)
	}
	if e := b.WithSettings(settings); e != nil {
		t.Fatal(e)
	}
	db, e := b.Build()
	if e != nil {
		t.Fatal(e)
	}
	o := owner(t, db)
	t.Cleanup(func() { closeOwner(t, o) })
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	server := grpc.NewServer()
	wire.RegisterMatchingPersistenceServer(server, &MatchingServer{Owner: o})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	constructor := adapter.NewMatchingStore
	if fair {
		constructor = adapter.NewFairMatchingStore
	}
	s, e := constructor(listener.Addr().String(), "p")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.Close)
	return s
}
func TestGoOwnerMatchingUserData(t *testing.T) {
	s := matchingStore(t)
	f := matchingCase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	blob := &commonpb.DataBlob{Data: []byte{0, 255, 8}, EncodingType: enumspb.ENCODING_TYPE_PROTO3}
	a, b, ac, bc := false, false, false, false
	q := &p.InternalUpdateTaskQueueUserDataRequest{NamespaceID: f.Namespace, Updates: map[string]*p.InternalSingleTaskQueueUserDataUpdate{"a": {UserData: blob, BuildIdsAdded: []string{"build"}, Applied: &a, Conflicting: &ac}, "b": {UserData: blob, BuildIdsAdded: []string{"build"}, Applied: &b, Conflicting: &bc}}}
	if e := s.UpdateTaskQueueUserData(ctx, q); e != nil || !a || !b || ac || bc {
		t.Fatal("initial applied pointers", a, b, ac, bc, e)
	}
	q.Updates["a"].Version = 1
	q.Updates["a"].BuildIdsAdded = []string{"new"}
	q.Updates["a"].BuildIdsRemoved = []string{"build"}
	q.Updates["b"].Version = 0
	var conflict *p.ConditionFailedError
	if e := s.UpdateTaskQueueUserData(ctx, q); !errors.As(e, &conflict) || a || b || ac || !bc {
		t.Fatal("conflict pointer/whole batch", a, b, ac, bc, e)
	}
	first, e := s.GetTaskQueueUserData(ctx, &p.GetTaskQueueUserDataRequest{NamespaceID: f.Namespace, TaskQueue: "a"})
	if e != nil || first.Version != 1 || !proto.Equal(first.UserData, blob) {
		t.Fatal("rollback data", first, e)
	}
	names, e := s.GetTaskQueuesByBuildId(ctx, &p.GetTaskQueuesByBuildIdRequest{NamespaceID: f.Namespace, BuildID: "build"})
	if e != nil || len(names) != 2 {
		t.Fatal("rollback index", names, e)
	}
	count, e := s.CountTaskQueuesByBuildId(ctx, &p.CountTaskQueuesByBuildIdRequest{NamespaceID: f.Namespace, BuildID: "new"})
	if e != nil || count != 0 {
		t.Fatal(count, e)
	}
	q.Updates["b"].Version = 1
	q.Updates["b"].BuildIdsAdded = nil
	bc = false
	if e = s.UpdateTaskQueueUserData(ctx, q); e != nil || !a || !b || ac || bc {
		t.Fatal("update", e)
	}
	count, e = s.CountTaskQueuesByBuildId(ctx, &p.CountTaskQueuesByBuildIdRequest{NamespaceID: f.Namespace, BuildID: "build"})
	if e != nil || count != 1 {
		t.Fatal(count, e)
	}
	page, e := s.ListTaskQueueUserDataEntries(ctx, &p.ListTaskQueueUserDataEntriesRequest{NamespaceID: f.Namespace, PageSize: 1})
	if e != nil || len(page.Entries) != 1 || page.Entries[0].TaskQueue != "a" || page.Entries[0].Version != 2 {
		t.Fatal(page, e)
	}
	next, e := s.ListTaskQueueUserDataEntries(ctx, &p.ListTaskQueueUserDataEntriesRequest{NamespaceID: f.Namespace, PageSize: 1, NextPageToken: page.NextPageToken})
	if e != nil || len(next.Entries) != 1 || next.Entries[0].TaskQueue != "b" {
		t.Fatal(next, e)
	}
	// Existing mapping collision must roll back the user's version increment.
	q.Updates = map[string]*p.InternalSingleTaskQueueUserDataUpdate{"a": {Version: 2, UserData: blob, BuildIdsAdded: []string{"new"}, Applied: &a, Conflicting: &ac}}
	var unavailable *serviceerror.Unavailable
	if e = s.UpdateTaskQueueUserData(ctx, q); !errors.As(e, &unavailable) || a || ac {
		t.Fatal("index conflict", a, ac, e)
	}
	first, e = s.GetTaskQueueUserData(ctx, &p.GetTaskQueueUserDataRequest{NamespaceID: f.Namespace, TaskQueue: "a"})
	if e != nil || first.Version != 2 {
		t.Fatal(first, e)
	}
}
func TestGoOwnerMatchingUpstreamUserData(t *testing.T) {
	suite.Run(t, suites.NewTaskQueueUserDataSuite(t, matchingStore(t), log.NewNoopLogger()))
}
func TestGoOwnerMatchingUpstreamQueues(t *testing.T) {
	suite.Run(t, suites.NewTaskQueueSuite(t, matchingStore(t), log.NewNoopLogger()))
}
func TestGoOwnerMatchingUpstreamTasks(t *testing.T) {
	suite.Run(t, suites.NewTaskQueueTaskSuite(t, matchingStore(t), log.NewNoopLogger()))
}

func TestGoOwnerMatchingUserDataRecovery(t *testing.T) {
	f := matchingCase(t)
	id := uuid.MustParse(f.Namespace)
	objects := objects(t)
	path := cfg(t).Prefix + "-userdata-recovery"
	o := owner(t, engine(t, objects, path, false))
	s := &MatchingServer{Owner: o}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := &wire.MatchingCommand{Kind: wire.MatchingCommand_UPDATE_USER_DATA, NamespaceId: id[:], Updates: []*wire.MatchingUserUpdate{{Queue: "a", Data: []byte{0, 255}, Encoding: 2, BuildIdsAdded: []string{"build"}}}}
	q := matchingReq("userdata-original", c)
	original, e := s.Execute(ctx, q)
	if e != nil || !original.Applied {
		t.Fatal(original, e)
	}
	later := proto.Clone(c).(*wire.MatchingCommand)
	later.Updates[0].Version = 1
	later.Updates[0].Data = []byte{9}
	later.Updates[0].BuildIdsAdded = nil
	later.Updates[0].BuildIdsRemoved = []string{"build"}
	if r, e := s.Execute(ctx, matchingReq("userdata-update", later)); e != nil || !r.Applied {
		t.Fatal(r, e)
	}
	closeOwner(t, o)
	o = owner(t, engine(t, objects, path, false))
	defer closeOwner(t, o)
	s = &MatchingServer{Owner: o}
	replay, e := s.Execute(ctx, q)
	if e != nil || !proto.Equal(replay, original) {
		t.Fatal("user replay", replay, e)
	}
	r, e := s.Execute(ctx, matchingReq("userdata-get", &wire.MatchingCommand{Kind: wire.MatchingCommand_GET_USER_DATA, NamespaceId: id[:], Queue: "a"}))
	if e != nil || len(r.UserData) != 1 || r.UserData[0].Version != 2 || !bytes.Equal(r.UserData[0].Data, []byte{9}) {
		t.Fatal("user recovery", r, e)
	}
	r, e = s.Execute(ctx, matchingReq("userdata-count", &wire.MatchingCommand{Kind: wire.MatchingCommand_COUNT_BY_BUILD, NamespaceId: id[:], BuildId: "build"}))
	if e != nil || r.Count != 0 {
		t.Fatal("index recovery", r, e)
	}
}
func TestGoOwnerMatchingUserDataBytePages(t *testing.T) {
	s := matchingStore(t)
	f := matchingCase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	blob := &commonpb.DataBlob{Data: bytes.Repeat([]byte{0xff}, f.BlobBytes), EncodingType: enumspb.ENCODING_TYPE_PROTO3}
	for _, name := range []string{"a", "b", "c"} {
		if e := s.UpdateTaskQueueUserData(ctx, &p.InternalUpdateTaskQueueUserDataRequest{NamespaceID: f.Namespace, Updates: map[string]*p.InternalSingleTaskQueueUserDataUpdate{name: {UserData: blob}}}); e != nil {
			t.Fatal(e)
		}
	}
	var token []byte
	var names []string
	for i := 0; i < 4; i++ {
		r, e := s.ListTaskQueueUserDataEntries(ctx, &p.ListTaskQueueUserDataEntriesRequest{NamespaceID: f.Namespace, PageSize: 1000, NextPageToken: token})
		if e != nil {
			t.Fatal(e)
		}
		if len(r.Entries) > 2 {
			t.Fatal("byte cap missing")
		}
		for _, v := range r.Entries {
			if !proto.Equal(v.Data, blob) {
				t.Fatal("blob changed")
			}
			names = append(names, v.TaskQueue)
		}
		token = r.NextPageToken
		if len(token) == 0 {
			break
		}
	}
	if strings.Join(names, ",") != "a,b,c" {
		t.Fatal("user data page skipped", names)
	}
}
