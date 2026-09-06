// Package buildinfo reports the dependencies embedded in the running executable.
// It does not infer the loaded native library from the Go binding version.
package buildinfo

import "runtime/debug"

// Release and native identities may be injected with -ldflags -X by a release
// builder AFTER verifying its artifacts. Empty values are reported as unknown.
// A native build attestation is not runtime verification of a dynamic loader.
var (
	Release        string
	SourceRevision string
	NativeCommit   string
	NativeSHA256   string
)

type Module struct {
	Path        string  `json:"path"`
	Version     string  `json:"version"`
	Sum         string  `json:"sum,omitempty"`
	Replacement *Module `json:"replacement,omitempty"`
}

type Native struct {
	SourceCommit   string `json:"source_commit"`
	ArtifactSHA256 string `json:"artifact_sha256"`
	IdentitySource string `json:"identity_source"`
}

type Info struct {
	Xenon         string `json:"xenon"`
	Go            string `json:"go"`
	Revision      string `json:"revision"`
	Modified      string `json:"modified"`
	Temporal      Module `json:"temporal"`
	SlateDBGo     Module `json:"slatedb_go"`
	SlateDBNative Native `json:"slatedb_native"`
}

func known(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

func module(m *debug.Module) Module {
	r := Module{Path: m.Path, Version: known(m.Version), Sum: m.Sum}
	if m.Replace != nil {
		v := module(m.Replace)
		r.Replacement = &v
	}
	return r
}

// Read reports actual Go build metadata; omitted dependencies stay unknown.
func Read() Info {
	b, _ := debug.ReadBuildInfo()
	return fromBuild(b)
}

func fromBuild(b *debug.BuildInfo) Info {
	i := Info{Xenon: known(Release), Go: "unknown", Revision: "unknown", Modified: "unknown",
		Temporal:      Module{Path: "go.temporal.io/server", Version: "unknown"},
		SlateDBGo:     Module{Path: "slatedb.io/slatedb-go", Version: "unknown"},
		SlateDBNative: Native{SourceCommit: known(NativeCommit), ArtifactSHA256: known(NativeSHA256), IdentitySource: "unknown"}}
	if SourceRevision != "" {
		i.Revision = SourceRevision
	}
	if NativeCommit != "" && NativeSHA256 != "" {
		i.SlateDBNative.IdentitySource = "build-attestation"
	}
	if b == nil {
		return i
	}
	i.Go = known(b.GoVersion)
	if Release == "" {
		i.Xenon = known(b.Main.Version)
	}
	for _, s := range b.Settings {
		switch s.Key {
		case "vcs.revision":
			i.Revision = known(s.Value)
		case "vcs.modified":
			i.Modified = known(s.Value)
		}
	}
	for _, d := range b.Deps {
		switch d.Path {
		case i.Temporal.Path:
			i.Temporal = module(d)
		case i.SlateDBGo.Path:
			i.SlateDBGo = module(d)
		}
	}
	return i
}
