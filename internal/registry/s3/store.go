// Package s3 implements the registry contract using conditional S3 objects.
// Its namespace must exclude deletion, lifecycle expiry and out-of-band writes.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/0x63616c/xenon/internal/registry"
	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// MaxRecordBytes bounds the encoded envelope, including its application payload.
const MaxRecordBytes = 1 << 20

type Store struct {
	client         *sdk.Client
	bucket, prefix string
}

var _ registry.Store = (*Store)(nil)

// New uses the pinned SDK directly. The client's HTTP transport and middleware
// must not replay mutations themselves; SDK retries are disabled per mutation.
// Prefix is canonical, nonempty, and has no leading/trailing slash.
func New(client *sdk.Client, bucket, prefix string) (*Store, error) {
	if client == nil || bucket == "" {
		return nil, &registry.Invalid{Reason: "client and bucket required"}
	}
	if err := registry.ValidateKey(registry.Key(prefix)); err != nil {
		return nil, err
	}
	if len(prefix)+2 > 1024 {
		return nil, &registry.Invalid{Reason: "prefix leaves no object key space"}
	}
	return &Store{client, bucket, prefix}, nil
}
func (s *Store) objectKey(key registry.Key) (string, error) {
	if err := registry.ValidateKey(key); err != nil {
		return "", err
	}
	path := s.prefix + "/" + string(key)
	if len(path) > 1024 {
		return "", &registry.Invalid{Key: key, Reason: "S3 key exceeds 1024 bytes"}
	}
	return path, nil
}
func (s *Store) Read(ctx context.Context, key registry.Key) (registry.Record, error) {
	path, err := s.objectKey(key)
	if err != nil {
		return registry.Record{}, err
	}
	if err = ctx.Err(); err != nil {
		return registry.Record{}, &registry.Unavailable{Key: key, Cause: err}
	}
	out, err := s.client.GetObject(ctx, &sdk.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(path)})
	if err != nil {
		if apiCode(err, "NoSuchKey") {
			return registry.Record{}, &registry.NotFound{Key: key}
		}
		return registry.Record{}, &registry.Unavailable{Key: key, Cause: err}
	}
	corrupt := func(cause error) (registry.Record, error) {
		return registry.Record{}, &registry.Corrupt{Key: key, Cause: cause}
	}
	if out == nil || out.Body == nil {
		return corrupt(errors.New("missing object body"))
	}
	defer out.Body.Close()
	if out.ContentLength != nil && *out.ContentLength > MaxRecordBytes {
		return corrupt(errors.New("record too large"))
	}
	data, err := io.ReadAll(io.LimitReader(out.Body, MaxRecordBytes+1))
	if err != nil {
		return registry.Record{}, &registry.Unavailable{Key: key, Cause: err}
	}
	if err = ctx.Err(); err != nil {
		return registry.Record{}, &registry.Unavailable{Key: key, Cause: err}
	}
	if len(data) > MaxRecordBytes {
		return corrupt(errors.New("record too large"))
	}
	r := registry.Record{Body: data, Version: registry.Version(aws.ToString(out.ETag))}
	if _, err = registry.Decode(key, r); err != nil {
		return registry.Record{}, err
	}
	return r, nil
}
func (s *Store) Create(ctx context.Context, key registry.Key, w registry.Write) (registry.Record, error) {
	return s.publish(ctx, key, "", w)
}
func (s *Store) Replace(ctx context.Context, key registry.Key, expected registry.Version, w registry.Write) (registry.Record, error) {
	if err := registry.ValidateReplace(key, expected, w); err != nil {
		return registry.Record{}, err
	}
	return s.publish(ctx, key, expected, w)
}
func (s *Store) publish(ctx context.Context, key registry.Key, expected registry.Version, w registry.Write) (registry.Record, error) {
	// Own bytes before validation, encoding, preflight and dispatch.
	w.Body = bytes.Clone(w.Body)
	path, err := s.objectKey(key)
	if err != nil {
		return registry.Record{}, err
	}
	data, err := registry.Encode(key, expected, w)
	if err != nil {
		return registry.Record{}, err
	}
	if len(data) > MaxRecordBytes {
		return registry.Record{}, &registry.Invalid{Key: key, Reason: "encoded record too large"}
	}
	// Read also detects a retry or immediate transition reuse before a conditional
	// replacement could overwrite its receipt. CAS still arbitrates concurrent reads.
	prior, readErr := s.Read(ctx, key)
	if readErr == nil {
		e, _ := registry.Decode(key, prior)
		if e.Transition == w.Transition {
			_, err = registry.Reconcile(key, expected, w, prior, nil)
			if err != nil {
				return registry.Record{}, err
			}
			return prior, nil
		}
		if expected == "" || prior.Version != expected {
			return registry.Record{}, &registry.Conflict{Key: key}
		}
	} else {
		var missing *registry.NotFound
		if !errors.As(readErr, &missing) {
			return registry.Record{}, readErr
		}
		if expected != "" {
			return registry.Record{}, &registry.Conflict{Key: key}
		}
	}
	if err = ctx.Err(); err != nil {
		return registry.Record{}, &registry.Unavailable{Key: key, Cause: err}
	}
	input := &sdk.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(path), Body: bytes.NewReader(data), ContentType: aws.String("application/json")}
	if expected == "" {
		input.IfNoneMatch = aws.String("*")
	} else {
		input.IfMatch = aws.String(string(expected))
	}
	out, writeErr := s.client.PutObject(ctx, input, func(o *sdk.Options) { o.Retryer = aws.NopRetryer{}; o.RetryMaxAttempts = 1 })
	// No response success is accepted beyond the caller's budget. Cancellation
	// after dispatch never claims rollback, even if the SDK returns a response.
	if err = ctx.Err(); err != nil {
		return registry.Record{}, &registry.UnknownOutcome{Key: key, Transition: w.Transition, Cause: errors.Join(writeErr, err)}
	}
	if writeErr == nil && out != nil && aws.ToString(out.ETag) != "" {
		return registry.Record{Body: data, Version: registry.Version(*out.ETag)}, nil
	}
	observed, readErr := s.Read(ctx, key)
	resolution, reconcileErr := registry.Reconcile(key, expected, w, observed, readErr)
	if resolution == registry.Published {
		return observed, nil
	}
	// With exactly one SDK mutation attempt a service 412 proves this call did
	// not publish. All transport/5xx/409/malformed responses remain ambiguous.
	if apiCode(writeErr, "PreconditionFailed") {
		return registry.Record{}, &registry.Conflict{Key: key}
	}
	return registry.Record{}, &registry.UnknownOutcome{Key: key, Transition: w.Transition, Cause: errors.Join(writeErr, reconcileErr, fmt.Errorf("publication not confirmed"))}
}
func apiCode(err error, code string) bool {
	var api smithy.APIError
	return errors.As(err, &api) && api.ErrorCode() == code
}
