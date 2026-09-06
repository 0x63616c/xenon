package ownership

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/0x63616c/xenon/internal/directory"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// ClusterManifest locks the interpretation of one cluster's S3 prefix. Version
// numbers name existing contracts; they do not assert release compatibility.
type ClusterManifest struct {
	Format        int    `json:"format"`
	Cluster       string `json:"cluster"`
	HistoryShards int32  `json:"history_shards"`
	LayoutVersion int    `json:"layout_version"`
	WireVersion   int    `json:"wire_version"`
}

func (m ClusterManifest) valid() bool {
	return m.Format == 1 && m.LayoutVersion == 1 && m.WireVersion == 1 && m.HistoryShards >= 1 && m.HistoryShards <= 16384 && len(m.Cluster) > 0 && len(m.Cluster) <= 128 && utf8.ValidString(m.Cluster) && strings.TrimSpace(m.Cluster) == m.Cluster && strings.IndexFunc(m.Cluster, unicode.IsControl) < 0
}

func readManifest(ctx context.Context, s *TopologyStore) (ClusterManifest, error) {
	var m ClusterManifest
	if err := ctx.Err(); err != nil {
		return m, err
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.prefix + "/cluster.json")})
	if err != nil {
		var api smithy.APIError
		if errors.As(err, &api) && (api.ErrorCode() == "NoSuchKey" || api.ErrorCode() == "NotFound") {
			return m, directory.ErrMissing
		}
		return m, err
	}
	if out == nil || out.Body == nil {
		return m, directory.ErrInvalid
	}
	defer out.Body.Close()
	data, err := io.ReadAll(io.LimitReader(out.Body, 4097))
	if err != nil {
		return m, err
	}
	if len(data) > 4096 {
		return m, directory.ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&m) != nil || decoder.Decode(new(any)) != io.EOF || !m.valid() {
		return m, directory.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return m, err
	}
	return m, nil
}

// EnsureCluster runs before Join. It never overwrites an existing manifest.
// An uncertain conditional create is resolved only by an exact bounded readback.
func EnsureCluster(ctx context.Context, s *TopologyStore, want ClusterManifest, bootstrap bool) error {
	if s == nil || !want.valid() {
		return directory.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, directory.OperationTimeout)
	defer cancel()
	got, err := readManifest(ctx, s)
	if err == nil {
		if got != want {
			return fmt.Errorf("%w: cluster manifest mismatch", directory.ErrConflict)
		}
		return nil
	}
	if !errors.Is(err, directory.ErrMissing) {
		return err
	}
	if !bootstrap {
		return directory.ErrMissing
	}
	data, _ := json.Marshal(want)
	_, writeErr := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.prefix + "/cluster.json"), Body: bytes.NewReader(data), ContentType: aws.String("application/json"), IfNoneMatch: aws.String("*")})
	// Read even after apparent success; only observed matching state authorizes join.
	got, readErr := readManifest(ctx, s)
	if readErr == nil {
		if got == want {
			return nil
		}
		return fmt.Errorf("%w: cluster manifest mismatch", directory.ErrConflict)
	}
	return fmt.Errorf("%w: manifest put=%v read=%v", directory.ErrUnknown, writeErr, readErr)
}
