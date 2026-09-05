// Package directory stores conditional ownership intentions in S3. READY is a
// recorded transition, not proof of a live engine fence, lease, or safe handover.
package directory

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
)

const MaxRecordBytes = 4096
const OperationTimeout = 5 * time.Second

var (
	ErrMissing  = errors.New("directory entry missing")
	ErrConflict = errors.New("directory transition superseded")
	ErrUnknown  = errors.New("directory write outcome unknown")
	ErrInvalid  = errors.New("invalid directory record or transition")
	ErrConsumed = errors.New("readiness attempt already consumed")
)

type S3 interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}
type Record struct {
	Format      int    `json:"format"`
	Partition   string `json:"partition"`
	DataPrefix  string `json:"data_prefix"`
	Generation  uint64 `json:"generation"`
	Transition  string `json:"transition"`
	Node        string `json:"node"`
	Incarnation string `json:"incarnation"`
	Address     string `json:"address"`
	State       string `json:"state"`
}
type Identity struct{ Node, Incarnation, Address string }
type Directory struct {
	client                             S3
	bucket, key, partition, dataPrefix string
}

// Snapshot fields are private: callers cannot substitute a record or ETag.
type Snapshot struct {
	directory *Directory
	record    Record
	etag      string
}

func (s Snapshot) Record() Record { return s.record }

// Reservation is bound to one exact directory and is locally one-shot. A fresh
// process must reserve a fresh incarnation; this is not a persistent attempt log.
type Reservation struct{ attempt *attempt }
type attempt struct {
	snapshot Snapshot
	consumed atomic.Bool
}

func (r *Reservation) Record() Record { return r.attempt.snapshot.record }
func New(client S3, bucket, prefix, partition, dataPrefix string) (*Directory, error) {
	prefix = strings.Trim(prefix, "/")
	dataPrefix = strings.Trim(dataPrefix, "/")
	canonical := func(value string) bool {
		for _, part := range strings.Split(value, "/") {
			if part == "" || part == "." || part == ".." {
				return false
			}
		}
		return true
	}
	if !canonical(prefix) || !canonical(dataPrefix) || prefix == dataPrefix || strings.HasPrefix(prefix, dataPrefix+"/") || strings.HasPrefix(dataPrefix, prefix+"/") {
		return nil, ErrInvalid
	}
	if client == nil || bucket == "" || prefix == "" || partition == "" || dataPrefix == "" || len(partition) > 256 || len(dataPrefix) > 1024 {
		return nil, ErrInvalid
	}
	return &Directory{client: client, bucket: bucket, key: strings.TrimSuffix(prefix, "/") + "/" + hex.EncodeToString([]byte(partition)) + ".json", partition: partition, dataPrefix: dataPrefix}, nil
}
func (d *Directory) valid(r Record) bool {
	_, te := uuid.Parse(r.Transition)
	_, ie := uuid.Parse(r.Incarnation)
	return r.Format == 1 && r.Partition == d.partition && r.DataPrefix == d.dataPrefix && r.Generation > 0 && te == nil && ie == nil && r.Node != "" && len(r.Node) <= 256 && r.Address != "" && len(r.Address) <= 1024 && (r.State == "opening" || r.State == "ready")
}
func (d *Directory) Read(ctx context.Context) (Snapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, OperationTimeout)
	defer cancel()
	out, e := d.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(d.bucket), Key: aws.String(d.key)})
	if e != nil {
		var api smithy.APIError
		if errors.As(e, &api) && (api.ErrorCode() == "NoSuchKey" || api.ErrorCode() == "NotFound") {
			return Snapshot{}, ErrMissing
		}
		return Snapshot{}, e
	}
	if out == nil || out.Body == nil {
		return Snapshot{}, ErrInvalid
	}
	defer out.Body.Close()
	if out.ContentLength != nil && *out.ContentLength > MaxRecordBytes {
		return Snapshot{}, ErrInvalid
	}
	data, e := io.ReadAll(io.LimitReader(out.Body, MaxRecordBytes+1))
	if e != nil {
		return Snapshot{}, e
	}
	if len(data) > MaxRecordBytes || out.ETag == nil || *out.ETag == "" {
		return Snapshot{}, ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var r Record
	if decoder.Decode(&r) != nil || decoder.Decode(new(any)) != io.EOF || !d.valid(r) {
		return Snapshot{}, ErrInvalid
	}
	return Snapshot{directory: d, record: r, etag: *out.ETag}, nil
}

// Reserve uses nil only for create-only. Superseding an opening or ready record
// needs the exact observed snapshot. It does not open or fence any engine.
func (d *Directory) Reserve(ctx context.Context, prior *Snapshot, identity Identity) (*Reservation, error) {
	generation := uint64(1)
	etag := ""
	if prior != nil {
		if prior.directory != d || !d.valid(prior.record) || prior.etag == "" || prior.record.Generation == math.MaxUint64 {
			return nil, ErrInvalid
		}
		generation = prior.record.Generation + 1
		etag = prior.etag
	}
	record := Record{Format: 1, Partition: d.partition, DataPrefix: d.dataPrefix, Generation: generation, Transition: uuid.NewString(), Node: identity.Node, Incarnation: identity.Incarnation, Address: identity.Address, State: "opening"}
	if !d.valid(record) {
		return nil, ErrInvalid
	}
	snapshot, e := d.publish(ctx, record, etag)
	if e != nil {
		return nil, e
	}
	return &Reservation{attempt: &attempt{snapshot: snapshot}}, nil
}

// Ready consumes an opening reservation locally even on failure. An unknown
// result must be read back, never reissued as a fresh unconditional write.
func (d *Directory) Ready(ctx context.Context, reservation *Reservation) (Snapshot, error) {
	if reservation == nil || reservation.attempt == nil || reservation.attempt.snapshot.directory != d || reservation.attempt.snapshot.record.State != "opening" {
		return Snapshot{}, ErrInvalid
	}
	if !reservation.attempt.consumed.CompareAndSwap(false, true) {
		return Snapshot{}, ErrConsumed
	}
	record := reservation.attempt.snapshot.record
	record.State = "ready"
	return d.publish(ctx, record, reservation.attempt.snapshot.etag)
}
func (d *Directory) publish(ctx context.Context, record Record, etag string) (Snapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, OperationTimeout)
	defer cancel()
	data, e := json.Marshal(record)
	if e != nil || len(data) > MaxRecordBytes {
		return Snapshot{}, ErrInvalid
	}
	input := &s3.PutObjectInput{Bucket: aws.String(d.bucket), Key: aws.String(d.key), Body: bytes.NewReader(data), ContentType: aws.String("application/json")}
	if etag == "" {
		input.IfNoneMatch = aws.String("*")
	} else {
		input.IfMatch = aws.String(etag)
	}
	out, writeErr := d.client.PutObject(ctx, input)
	if writeErr == nil && out != nil && out.ETag != nil && *out.ETag != "" {
		return Snapshot{directory: d, record: record, etag: *out.ETag}, nil
	}
	// Even a precondition failure may follow a committed response lost before an
	// SDK retry. Only an exact fresh record read can reconcile that publication.
	observed, readErr := d.Read(ctx)
	if readErr == nil && observed.record == record {
		return observed, nil
	}
	if readErr == nil && observed.record.Transition != record.Transition && observed.etag != etag {
		return Snapshot{}, fmt.Errorf("%w: %v", ErrConflict, writeErr)
	}
	return Snapshot{}, fmt.Errorf("%w: put=%v read=%v", ErrUnknown, writeErr, readErr)
}
