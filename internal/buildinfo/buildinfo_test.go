package buildinfo

import (
	"runtime/debug"
	"testing"
)

func TestBuildIdentityPreservesUnknownAndReplacements(t *testing.T) {
	i := fromBuild(nil)
	if i.Temporal.Version != "unknown" || i.SlateDBNative.IdentitySource != "unknown" {
		t.Fatalf("invented identity: %+v", i)
	}
	i = fromBuild(&debug.BuildInfo{GoVersion: "go-test", Main: debug.Module{Version: "(devel)"}, Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc"}, {Key: "vcs.modified", Value: "true"}}, Deps: []*debug.Module{
		{Path: "go.temporal.io/server", Version: "v1.2.3", Replace: &debug.Module{Path: "../local-server"}},
		{Path: "slatedb.io/slatedb-go", Version: "v4.5.6", Sum: "binding-sum"},
	}})
	if i.Revision != "abc" || i.Modified != "true" || i.Go != "go-test" || i.Xenon != "(devel)" {
		t.Fatalf("lost build identity: %+v", i)
	}
	if i.Temporal.Version != "v1.2.3" || i.Temporal.Replacement == nil || i.Temporal.Replacement.Path != "../local-server" || i.Temporal.Replacement.Version != "unknown" {
		t.Fatalf("hidden replacement: %+v", i.Temporal)
	}
	if i.SlateDBGo.Version != "v4.5.6" || i.SlateDBNative.SourceCommit != "unknown" {
		t.Fatalf("binding is not native identity: %+v", i)
	}
}

func TestNativeIdentityRequiresCompleteAttestation(t *testing.T) {
	oldCommit, oldHash := NativeCommit, NativeSHA256
	defer func() { NativeCommit, NativeSHA256 = oldCommit, oldHash }()
	NativeCommit, NativeSHA256 = "source", ""
	if got := fromBuild(nil).SlateDBNative.IdentitySource; got != "unknown" {
		t.Fatalf("partial attestation: %s", got)
	}
	NativeSHA256 = "artifact"
	if got := fromBuild(nil).SlateDBNative.IdentitySource; got != "build-attestation" {
		t.Fatalf("missing attestation: %s", got)
	}
}

func TestExplicitSourceRevisionOverridesUnavailableVCSMetadata(t *testing.T) {
	old := SourceRevision
	defer func() { SourceRevision = old }()
	SourceRevision = "container-source"
	if got := fromBuild(nil).Revision; got != "container-source" {
		t.Fatalf("source revision = %q", got)
	}
}
