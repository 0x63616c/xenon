package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/0x63616c/xenon/internal/cluster"
	"github.com/0x63616c/xenon/internal/registry"
	registrys3 "github.com/0x63616c/xenon/internal/registry/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type InspectionStatus string

const (
	InspectionObserved    InspectionStatus = "observed"
	InspectionUnknown     InspectionStatus = "unknown"
	InspectionUnavailable InspectionStatus = "unavailable"
	InspectionInvalid     InspectionStatus = "invalid"
)

// Inspection is a point-in-time authority observation, not a liveness probe or
// census of all running nodes. Control owners are persisted desired owners;
// Ready records publication, not current process health.
type Inspection struct {
	Schema           int              `json:"schema"`
	Status           InspectionStatus `json:"status"` // observed, unknown, unavailable, invalid
	AuthorityVersion registry.Version `json:"authority_version,omitempty"`
	Control          *cluster.Control `json:"control,omitempty"`
	Error            string           `json:"error,omitempty"`
}

// Inspect uses only GET operations. It never provisions a missing namespace,
// generates an incarnation, registers membership, or opens a native writer.
func Inspect(ctx context.Context, c Config) (Inspection, error) {
	if err := ValidateServiceLayout(c); err != nil {
		return inspectionFailure("invalid", err)
	}
	if ctx == nil {
		return inspectionFailure("invalid", errors.New("inspection context required"))
	}
	if err := ctx.Err(); err != nil {
		return inspectionFailure("unavailable", err)
	}
	client, err := serviceS3Client()
	if err != nil {
		return inspectionFailure("unavailable", err)
	}
	return inspectService(ctx, c, client)
}
func inspectionFailure(status InspectionStatus, err error) (Inspection, error) {
	return Inspection{Schema: 1, Status: status, Error: err.Error()}, err
}
func inspectService(ctx context.Context, c Config, client *s3.Client) (Inspection, error) {
	settings := c.ServiceStorage.Clone()
	c.ServiceStorage = &settings
	ctx, cancel := context.WithTimeout(ctx, duration(settings.RegistryTimeout))
	defer cancel()
	digest, err := settings.Layout.Digest()
	if err != nil {
		return inspectionFailure("invalid", err)
	}
	m, err := readServiceManifest(ctx, client, c.Bucket, c.Prefix+"/metadata/cluster.json")
	if err != nil {
		state := InspectionUnavailable
		if missingObject(err) || errors.Is(err, ErrLegacyPrefix) || errors.Is(err, ErrBootstrap) {
			state = InspectionUnknown
		}
		return inspectionFailure(state, err)
	}
	want := serviceManifest{Format: 2, Cluster: c.Cluster, HistoryShards: c.HistoryShards, LayoutVersion: 1, WireVersion: 1, ClusterID: settings.ClusterID, LayoutDigest: digest}
	if !m.matches(want) {
		return inspectionFailure("unknown", fmt.Errorf("manifest does not match configured cluster/layout"))
	}
	store, err := registrys3.New(client, c.Bucket, c.Prefix+"/metadata/registry")
	if err != nil {
		return inspectionFailure("invalid", err)
	}
	record, err := store.Read(ctx, serviceControlKey)
	if err != nil {
		state := InspectionUnavailable
		var missing *registry.NotFound
		var corrupt *registry.Corrupt
		if errors.As(err, &missing) || errors.As(err, &corrupt) {
			state = InspectionUnknown
		}
		return inspectionFailure(state, err)
	}
	snapshot, err := cluster.DecodeControl(serviceControlKey, record, settings.MaxControlBytes)
	if err == nil {
		err = snapshot.ValidateLayout(digest)
	}
	if err != nil {
		return inspectionFailure("unknown", err)
	}
	control := snapshot.Control()
	if control.Cluster != settings.ClusterID {
		return inspectionFailure("unknown", errors.New("control cluster mismatch"))
	}
	if err = ctx.Err(); err != nil {
		return inspectionFailure("unavailable", err)
	}
	return Inspection{Schema: 1, Status: "observed", AuthorityVersion: record.Version, Control: &control}, nil
}
