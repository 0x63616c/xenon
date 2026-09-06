package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/0x63616c/xenon/internal/agent"
	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/identity"
	"github.com/0x63616c/xenon/internal/partitions"
	"github.com/0x63616c/xenon/internal/registry"
	registrys3 "github.com/0x63616c/xenon/internal/registry/s3"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

var (
	ErrServiceRuntimeUnavailable = errors.New("format2 service runtime assembly is not installed; legacy fallback refused")
	ErrBootstrap                 = errors.New("fresh service bootstrap refused")
	ErrLegacyPrefix              = errors.New("legacy or occupied storage prefix requires explicit offline cutover")
	ErrIncompleteInitialization  = errors.New("matching service manifest has no valid control; explicit offline recovery required")
)

const serviceControlKey registry.Key = "cluster/control"

// serviceManifest occupies the SAME key read by the legacy application's
// EnsureCluster before it starts Manager/Join. Format1 is never replaced here.
type serviceManifest struct {
	Format         int                   `json:"format"`
	Cluster        string                `json:"cluster"`
	HistoryShards  int32                 `json:"history_shards"`
	LayoutVersion  int                   `json:"layout_version"`
	WireVersion    int                   `json:"wire_version"`
	ClusterID      identity.ClusterID    `json:"cluster_id"`
	LayoutDigest   [32]byte              `json:"layout_digest"`
	Initialization identity.TransitionID `json:"initialization"`
}

func (m serviceManifest) matches(want serviceManifest) bool {
	m.Initialization = ""
	want.Initialization = ""
	return m == want
}

// Prepared is constructed only after manifest exclusion and pinned control
// readback. It contains no reusable permission to create missing live authority.
// Its accessors return owned copies; callers create all drivers from one capsule.
type Prepared struct {
	config   agent.Config
	owner    cluster.Owner
	digest   [32]byte
	store    *registrys3.Store
	snapshot cluster.Snapshot
}

func (p *Prepared) Owner() cluster.Owner     { return p.owner }
func (p *Prepared) Registry() registry.Store { return p.store }
func (p *Prepared) Control() cluster.Control { return p.snapshot.Control() }
func (p *Prepared) Config() agent.Config {
	c := p.config
	s := c.ServiceStorage.Clone()
	c.ServiceStorage = &s
	return c
}
func duration(value string) time.Duration { d, _ := time.ParseDuration(value); return d }
func (p *Prepared) ClusterConfig() cluster.ControllerConfig {
	c := p.config.ServiceStorage
	return cluster.ControllerConfig{Key: serviceControlKey, Incarnation: p.owner.Incarnation, ExpectedLayoutDigest: p.digest, MaxControlBytes: c.MaxControlBytes, RenewalInterval: duration(c.RenewalInterval), SuspectAfter: duration(c.SuspectAfter)}
}
func (p *Prepared) MembershipConfig() cluster.MembershipConfig {
	c := p.config.ServiceStorage
	return cluster.MembershipConfig{Prefix: "membership", Cluster: c.ClusterID, ExpectedLayoutDigest: p.digest, Self: p.owner, FailureAfter: duration(c.MembershipFailureAfter), MaxEntries: c.MaxMembershipEntries, MaxRecordBytes: c.MaxMembershipBytes, ReadsPerScan: c.MembershipReadBatch}
}
func (p *Prepared) PartitionConfigs() []partitions.ControllerConfig {
	out := make([]partitions.ControllerConfig, 0, len(p.config.ServiceStorage.Layout.Partitions))
	for _, physical := range p.config.ServiceStorage.Layout.Partitions {
		out = append(out, partitions.ControllerConfig{Key: serviceControlKey, Partition: physical.ID, Incarnation: p.owner.Incarnation, ExpectedLayoutDigest: p.digest, MaxControlBytes: p.config.ServiceStorage.MaxControlBytes})
	}
	return out
}
func readServiceManifest(ctx context.Context, client *s3.Client, bucket, key string) (serviceManifest, error) {
	var m serviceManifest
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return m, err
	}
	if out == nil || out.Body == nil {
		return m, ErrBootstrap
	}
	defer out.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(out.Body, 4097))
	if err != nil {
		return m, err
	}
	if len(raw) > 4096 || json.Unmarshal(raw, &m) != nil {
		return m, ErrBootstrap
	}
	if m.Format == 1 {
		return m, ErrLegacyPrefix
	}
	canonical, err := json.Marshal(m)
	if err != nil || !bytes.Equal(raw, canonical) || m.Format != 2 || m.Initialization.Validate() != nil {
		return m, ErrBootstrap
	}
	if err = ctx.Err(); err != nil {
		return m, err
	}
	return m, nil
}
func missingObject(err error) bool {
	var api smithy.APIError
	return errors.As(err, &api) && (api.ErrorCode() == "NoSuchKey" || api.ErrorCode() == "NotFound")
}

// PrepareServiceStorage claims only an explicitly empty namespace or joins an
// existing exact format2 manifest with valid control. It never overwrites a
// legacy manifest or repairs missing control. This boundary does not open native
// data, publish readiness, or install an RPC service.
func PrepareServiceStorage(ctx context.Context, c agent.Config, client *s3.Client, ids identity.Source) (*Prepared, error) {
	if ctx == nil || client == nil || ids == nil || c.ServiceStorage == nil {
		return nil, ErrBootstrap
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	settings := c.ServiceStorage.Clone()
	ctx, cancel := context.WithTimeout(ctx, duration(settings.RegistryTimeout))
	defer cancel()
	c.ServiceStorage = &settings
	digest, err := settings.Layout.Digest()
	if err != nil {
		return nil, err
	}
	inc, err := identity.NewIncarnationID(ids)
	if err != nil {
		return nil, err
	}
	store, err := registrys3.New(client, c.Bucket, c.Prefix+"/metadata/registry")
	if err != nil {
		return nil, err
	}
	p := &Prepared{config: c, owner: cluster.Owner{Node: settings.NodeID, Incarnation: inc, Address: c.Address(8)}, digest: digest, store: store}
	initial := cluster.Control{Format: cluster.ControlFormat, Cluster: settings.ClusterID, Layout: &settings.Layout, Coordinator: cluster.Coordinator{Incarnation: inc, Generation: 1}, AssignmentRevision: 1, Partitions: map[identity.PartitionID]cluster.PartitionControl{}}
	for _, physical := range settings.Layout.Partitions {
		initial.Partitions[physical.ID] = cluster.PartitionControl{Path: physical.Path, Desired: p.owner, AssignmentRevision: 1}
	}
	transition, entropyErr := identity.NewTransitionID(ids)
	if entropyErr != nil {
		return nil, entropyErr
	}
	write, writeErr := cluster.BootstrapWrite(serviceControlKey, transition, initial, settings.MaxControlBytes)
	if writeErr != nil {
		return nil, writeErr
	}
	want := serviceManifest{Format: 2, Cluster: c.Cluster, HistoryShards: c.HistoryShards, LayoutVersion: 1, WireVersion: 1, ClusterID: settings.ClusterID, LayoutDigest: digest}
	manifestKey := c.Prefix + "/metadata/cluster.json"
	got, err := readServiceManifest(ctx, client, c.Bucket, manifestKey)
	newlyClaimed := false
	if missingObject(err) {
		if !c.Bootstrap || !settings.FreshNamespace {
			return nil, fmt.Errorf("%w: explicit bootstrap and fresh namespace assertion required", ErrBootstrap)
		}
		// Listing detects pre-manifest topology/data occupancy, not a lock. Conditional
		// create at the legacy manifest key arbitrates protocol-honoring concurrent starts.
		listed, listErr := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(c.Bucket), Prefix: aws.String(c.Prefix + "/"), MaxKeys: aws.Int32(1)})
		if listErr != nil {
			return nil, listErr
		}
		if listed == nil || listed.KeyCount == nil {
			return nil, ErrBootstrap
		}
		if len(listed.Contents) != 0 || *listed.KeyCount != 0 || aws.ToBool(listed.IsTruncated) {
			got, err = readServiceManifest(ctx, client, c.Bucket, manifestKey)
			if err != nil {
				if missingObject(err) || errors.Is(err, ErrLegacyPrefix) {
					return nil, errors.Join(ErrLegacyPrefix, err)
				}
				return nil, err
			}
		} else {
			init, entropyErr := identity.NewTransitionID(ids)
			if entropyErr != nil {
				return nil, entropyErr
			}
			want.Initialization = init
			raw, _ := json.Marshal(want)
			_, writeErr := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(c.Bucket), Key: aws.String(manifestKey), IfNoneMatch: aws.String("*"), Body: bytes.NewReader(raw), ContentType: aws.String("application/json")}, func(o *s3.Options) { o.Retryer = aws.NopRetryer{}; o.RetryMaxAttempts = 1 })
			// Exact readback of this unique initialization recovers a lost response. A
			// matching other claimant is an ordinary join, never transferred bootstrap rights.
			got, err = readServiceManifest(ctx, client, c.Bucket, manifestKey)
			if err != nil {
				return nil, errors.Join(ErrBootstrap, writeErr, err)
			}
			newlyClaimed = got.Initialization == init
		}
	}
	if err != nil {
		return nil, err
	}
	if !got.matches(want) {
		return nil, fmt.Errorf("%w: cluster metadata or layout mismatch", ErrBootstrap)
	}
	if newlyClaimed {
		_, writeErr = store.Create(ctx, serviceControlKey, write)
		observed, readErr := store.Read(ctx, serviceControlKey)
		resolution, reconcileErr := registry.Reconcile(serviceControlKey, "", write, observed, readErr)
		if resolution != registry.Published {
			return nil, errors.Join(ErrIncompleteInitialization, writeErr, readErr, reconcileErr)
		}
	}
	record, err := store.Read(ctx, serviceControlKey)
	if err != nil {
		return nil, errors.Join(ErrIncompleteInitialization, err)
	}
	p.snapshot, err = cluster.DecodeControl(serviceControlKey, record, settings.MaxControlBytes)
	if err == nil {
		err = p.snapshot.ValidateLayout(digest)
	}
	if err != nil || p.snapshot.Control().Cluster != settings.ClusterID {
		return nil, errors.Join(ErrIncompleteInitialization, err)
	}
	return p, nil
}
