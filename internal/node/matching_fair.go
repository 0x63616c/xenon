package node

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	wire "github.com/0x63616c/xenon/gen/xenon/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"math"
	native "slatedb.io/slatedb-go/uniffi"
)

func fairLevel(pass, id int64) []byte {
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b, uint64(pass)^(1<<63))
	binary.BigEndian.PutUint64(b[8:], uint64(id)^(1<<63))
	return b
}
func fairQuery(c *wire.MatchingCommand) []byte {
	// Bind continuation to logical queue, subqueue and read lower bound, allowing
	// the caller to change page size without changing the requested data range.
	b := append([]byte(matchingTaskPrefix(c, c.Subqueue)), fairLevel(c.MinPass, c.MinId)...)
	h := sha256.Sum256(b)
	return h[:]
}
func applyFairTasks(tx *native.DbTransaction, c *wire.MatchingCommand) (*wire.MatchingResult, error) {
	if c.Kind == wire.MatchingCommand_GET_TASKS && (c.MinPass < 1 || c.MaxId != math.MaxInt64) {
		return nil, status.Error(codes.Internal, "invalid fair task read bounds")
	}
	if c.Kind == wire.MatchingCommand_COMPLETE_TASKS && c.MaxPass < 1 {
		return nil, status.Error(codes.Internal, "invalid fair completion pass")
	}
	prefix := matchingTaskPrefix(c, c.Subqueue)
	r := new(wire.MatchingResult)
	var after []byte
	query := fairQuery(c)
	if len(c.Token) > 0 {
		if len(c.Token) != 49 || c.Token[0] != 1 || !bytes.Equal(c.Token[1:33], query) {
			return nil, status.Error(codes.InvalidArgument, "invalid fair task cursor")
		}
		after = c.Token[33:]
	}
	min, max := fairLevel(c.MinPass, c.MinId), fairLevel(c.MaxPass, c.MaxId)
	var remove [][]byte
	truncated := false
	e := scanCluster(tx, prefix, func(key, value []byte) (bool, error) {
		if len(key) != 16 {
			return false, status.Error(codes.Unavailable, "corrupt fair task key")
		}
		if c.Kind == wire.MatchingCommand_COMPLETE_TASKS {
			if bytes.Compare(key, max) >= 0 || len(remove) >= int(c.PageSize) {
				return true, nil
			}
			remove = append(remove, append([]byte(prefix), key...))
			return false, nil
		}
		if bytes.Compare(key, min) < 0 || (after != nil && bytes.Compare(key, after) <= 0) {
			return false, nil
		}
		if len(r.Tasks) >= int(c.PageSize) {
			truncated = true
			return true, nil
		}
		v := new(wire.MatchingTask)
		if e := proto.Unmarshal(value, v); e != nil {
			return false, backend(e)
		}
		if !bytes.Equal(key, fairLevel(v.Pass, v.Id)) {
			return false, status.Error(codes.Unavailable, "corrupt fair task identity")
		}
		previous := r.Token
		r.Tasks = append(r.Tasks, v)
		r.Token = append(append([]byte{1}, query...), key...)
		if proto.Size(r) > 3*1024*1024 {
			r.Tasks = r.Tasks[:len(r.Tasks)-1]
			r.Token = previous
			truncated = true
			return true, nil
		}
		return false, nil
	})
	if e != nil {
		return nil, e
	}
	for _, key := range remove {
		if e := tx.Delete(key); e != nil {
			return nil, backend(e)
		}
	}
	if !truncated {
		r.Token = nil
	}
	r.Completed = int32(len(remove))
	return r, nil
}
