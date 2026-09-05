// Package ownership combines conditional intentions with fenced native admission.
package ownership

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/0x63616c/xenon/internal/directory"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
)

const maxTopologyBytes = 65536
const maxPartitions = 256

type Member struct {
	Address     string `json:"address"`
	Incarnation string `json:"incarnation"`
}
type Assignment struct {
	Node       string `json:"node"`
	DataPrefix string `json:"data_prefix"`
}
type Topology struct {
	Format     int                   `json:"format"`
	Revision   uint64                `json:"revision"`
	Transition string                `json:"transition"`
	Members    map[string]Member     `json:"members"`
	Partitions map[string]Assignment `json:"partitions"`
}
type TopologyStore struct {
	client         directory.S3
	bucket, prefix string
}
type TopologySnapshot struct {
	store *TopologyStore
	data  Topology
	etag  string
}

func (s TopologySnapshot) Record() Topology { return cloneTopology(s.data) }
func cloneTopology(t Topology) Topology {
	t.copyMaps()
	return t
}
func (t *Topology) copyMaps() {
	m := make(map[string]Member, len(t.Members))
	for k, v := range t.Members {
		m[k] = v
	}
	t.Members = m
	p := make(map[string]Assignment, len(t.Partitions))
	for k, v := range t.Partitions {
		p[k] = v
	}
	t.Partitions = p
}
func pathOK(s string) bool {
	if s == "" || !utf8.ValidString(s) || len(s) > 900 {
		return false
	}
	for _, p := range strings.Split(s, "/") {
		if p == "" || p == "." || p == ".." {
			return false
		}
	}
	return true
}
func overlap(a, b string) bool {
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}
func NewTopologyStore(client directory.S3, bucket, prefix string) (*TopologyStore, error) {
	if client == nil || bucket == "" || !pathOK(prefix) {
		return nil, directory.ErrInvalid
	}
	return &TopologyStore{client: client, bucket: bucket, prefix: prefix}, nil
}
func (s *TopologyStore) valid(t Topology) bool {
	if t.Format != 1 || t.Revision == 0 || len(t.Members) == 0 || len(t.Members) > 64 || len(t.Partitions) == 0 || len(t.Partitions) > maxPartitions {
		return false
	}
	if _, e := uuid.Parse(t.Transition); e != nil {
		return false
	}
	for id, m := range t.Members {
		if id == "" || len(id) > 256 || !utf8.ValidString(id) || m.Address == "" || len(m.Address) > 1024 || !utf8.ValidString(m.Address) {
			return false
		}
		if _, e := uuid.Parse(m.Incarnation); e != nil {
			return false
		}
	}
	paths := []string{s.prefix}
	for id, a := range t.Partitions {
		if id == "" || len(id) > 256 || !utf8.ValidString(id) || !pathOK(a.DataPrefix) {
			return false
		}
		if _, ok := t.Members[a.Node]; !ok {
			return false
		}
		for _, p := range paths {
			if overlap(p, a.DataPrefix) {
				return false
			}
		}
		paths = append(paths, a.DataPrefix)
	}
	return true
}
func (s *TopologyStore) Read(ctx context.Context) (TopologySnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, directory.OperationTimeout)
	defer cancel()
	out, e := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.prefix + "/topology.json")})
	if e != nil {
		var api smithy.APIError
		if errors.As(e, &api) && (api.ErrorCode() == "NoSuchKey" || api.ErrorCode() == "NotFound") {
			return TopologySnapshot{}, directory.ErrMissing
		}
		return TopologySnapshot{}, e
	}
	if out == nil || out.Body == nil {
		return TopologySnapshot{}, directory.ErrInvalid
	}
	defer out.Body.Close()
	b, e := io.ReadAll(io.LimitReader(out.Body, maxTopologyBytes+1))
	if e != nil {
		return TopologySnapshot{}, e
	}
	if len(b) > maxTopologyBytes || out.ETag == nil || *out.ETag == "" {
		return TopologySnapshot{}, directory.ErrInvalid
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var t Topology
	if dec.Decode(&t) != nil || dec.Decode(new(any)) != io.EOF || !s.valid(t) {
		return TopologySnapshot{}, directory.ErrInvalid
	}
	return TopologySnapshot{store: s, data: t, etag: *out.ETag}, nil
}

// Publish changes intention only. No engine is opened and no lease is granted.
// Existing partitions and data prefixes cannot be removed or rebound.
func (s *TopologyStore) Publish(ctx context.Context, prior *TopologySnapshot, next Topology) (TopologySnapshot, error) {
	next = cloneTopology(next)
	next.Format = 1
	next.Revision = 1
	next.Transition = uuid.NewString()
	etag := ""
	if prior != nil {
		if prior.store != s || prior.etag == "" || prior.data.Revision == math.MaxUint64 {
			return TopologySnapshot{}, directory.ErrInvalid
		}
		for id, a := range prior.data.Partitions {
			b, ok := next.Partitions[id]
			if !ok || a.DataPrefix != b.DataPrefix {
				return TopologySnapshot{}, directory.ErrInvalid
			}
		}
		next.Revision = prior.data.Revision + 1
		etag = prior.etag
	}
	b, e := json.Marshal(next)
	if e != nil || len(b) > maxTopologyBytes || !s.valid(next) {
		return TopologySnapshot{}, directory.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, directory.OperationTimeout)
	defer cancel()
	input := &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.prefix + "/topology.json"), Body: bytes.NewReader(b), ContentType: aws.String("application/json")}
	if etag == "" {
		input.IfNoneMatch = aws.String("*")
	} else {
		input.IfMatch = aws.String(etag)
	}
	out, writeErr := s.client.PutObject(ctx, input)
	if writeErr == nil && out != nil && out.ETag != nil && *out.ETag != "" {
		return TopologySnapshot{s, next, *out.ETag}, nil
	}
	observed, readErr := s.Read(ctx)
	if readErr == nil {
		got, _ := json.Marshal(observed.data)
		if bytes.Equal(got, b) {
			return observed, nil
		}
		if observed.data.Transition != next.Transition && observed.etag != etag {
			return TopologySnapshot{}, directory.ErrConflict
		}
	}
	return TopologySnapshot{}, fmt.Errorf("%w: topology put=%v read=%v", directory.ErrUnknown, writeErr, readErr)
}
