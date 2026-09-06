package persistence

import (
	"bytes"
	"context"
	"encoding/hex"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func userDataPrefix(namespace []byte) string {
	return "v1/matching/user/" + hex.EncodeToString(namespace) + "/"
}
func buildPrefix(namespace []byte, build string) string {
	return "v1/matching/build/" + hex.EncodeToString(namespace) + "/" + hex.EncodeToString([]byte(build)) + "/"
}
func applyMatchingUserData(ctx context.Context, tx ClusterTransaction, c *wire.MatchingCommand) (*wire.MatchingResult, error) {
	r := new(wire.MatchingResult)
	prefix := userDataPrefix(c.NamespaceId)
	switch c.Kind {
	case wire.MatchingCommand_GET_USER_DATA:
		v := new(wire.MatchingUserRecord)
		exists, e := loadCluster(ctx, tx, prefix+c.Queue, v)
		if e != nil {
			return nil, e
		}
		if !exists {
			return matchingLogical(wire.MatchingResult_NOT_FOUND, "task queue user data not found"), nil
		}
		r.UserData = []*wire.MatchingUserRecord{v}
		return r, nil
	case wire.MatchingCommand_UPDATE_USER_DATA:
		// The namespace batch and all build-ID indexes share one transaction. Check
		// every failure before staging: journaled logical failures must commit no data.
		for _, u := range c.Updates {
			v := new(wire.MatchingUserRecord)
			exists, e := loadCluster(ctx, tx, prefix+u.Queue, v)
			if e != nil {
				return nil, e
			}
			if (u.Version == 0 && exists) || (u.Version != 0 && (!exists || v.Version != u.Version)) {
				return &wire.MatchingResult{Error: wire.MatchingResult_CONDITION_FAILED, Message: "user data version conflict", Conflicting: []string{u.Queue}}, nil
			}
			seen := map[string]bool{}
			for _, id := range u.BuildIdsAdded {
				key := buildPrefix(c.NamespaceId, id) + u.Queue
				old, e := tx.Get(ctx, []byte(key))
				if e != nil {
					return nil, e
				}
				if seen[key] || old != nil {
					return matchingLogical(wire.MatchingResult_UNAVAILABLE, "duplicate build ID mapping"), nil
				}
				seen[key] = true
			}
		}
		for _, u := range c.Updates {
			if e := saveCluster(tx, prefix+u.Queue, &wire.MatchingUserRecord{Queue: u.Queue, Version: u.Version + 1, Data: u.Data, Encoding: u.Encoding}); e != nil {
				return nil, e
			}
			for _, id := range u.BuildIdsAdded {
				if e := tx.Put([]byte(buildPrefix(c.NamespaceId, id)+u.Queue), []byte{1}); e != nil {
					return nil, e
				}
			}
			for _, id := range u.BuildIdsRemoved {
				if e := tx.Delete([]byte(buildPrefix(c.NamespaceId, id) + u.Queue)); e != nil {
					return nil, e
				}
			}
		}
		r.Applied = true
		return r, nil
	case wire.MatchingCommand_LIST_USER_DATA:
		if len(c.Token) > 1024 {
			return nil, status.Error(codes.InvalidArgument, "invalid user-data token")
		}
		var last []byte
		truncated := false
		e := scanCluster(ctx, tx, prefix, func(key, value []byte) (bool, error) {
			if bytes.Compare(key, c.Token) <= 0 {
				return false, nil
			}
			if len(r.UserData) >= int(c.PageSize) {
				truncated = true
				return true, nil
			}
			v := new(wire.MatchingUserRecord)
			if e := proto.Unmarshal(value, v); e != nil {
				return false, clusterEncodingError(e)
			}
			r.UserData = append(r.UserData, v)
			r.Token = append([]byte(nil), key...)
			if proto.Size(r) > 3*1024*1024 {
				r.UserData = r.UserData[:len(r.UserData)-1]
				r.Token = last
				truncated = true
				return true, nil
			}
			last = r.Token
			return false, nil
		})
		if !truncated {
			r.Token = nil
		}
		return r, e
	case wire.MatchingCommand_GET_BY_BUILD, wire.MatchingCommand_COUNT_BY_BUILD:
		e := scanCluster(ctx, tx, buildPrefix(c.NamespaceId, c.BuildId), func(key, value []byte) (bool, error) {
			r.Count++
			if c.Kind == wire.MatchingCommand_GET_BY_BUILD {
				r.QueueNames = append(r.QueueNames, string(key))
				if proto.Size(r) > 3*1024*1024 {
					return true, status.Error(codes.ResourceExhausted, "complete build index exceeds response budget")
				}
			}
			return false, nil
		})
		return r, e
	}
	return nil, status.Error(codes.InvalidArgument, "unknown matching user-data operation")
}
