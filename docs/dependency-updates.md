# Dependency updates and build identity

Customers upgrade Xenon as a tested release. Embedded Temporal and the current
SlateDB storage implementation do not independently update in a running agent.

Dependabot proposes weekly Go dependency PRs. Temporal Server updates form one
critical-dependency lane; its API and SDK companions are ignored as independent
updates because Server's own `go.mod` defines the versions it compiles against.
A reviewed Server upgrade updates all three together. SlateDB bindings form a
separate lane so engine changes are not mixed with workflow-engine changes. This
configuration enables neither automatic merging nor production deployment.
GitHub must run Dependabot and CI to supply results; configuration alone is not
execution evidence.

## Review gates

For Temporal updates, review upstream persistence contracts, serialization,
configuration and behaviour changes; run the upgrade-impact tooling, compatibility
suite and existing-state workflow continuation proof. Test mixed versions before
allowing rolling upgrades, and rollback separately before documenting it as safe.

For SlateDB updates, coordinate `go.mod`, `go.sum`, `tools/slatedb-native.json` and
native build tooling. Dependabot only updates the Go dependency: it cannot select
or verify the matching Rust core commit or native ABI. Review generated binding
compatibility, build the pinned native source, and execute lifecycle, durability,
replay and recovery proofs. Keep credentials outside manifests and evidence.

Do not merge these critical updates automatically. Required branch checks should
include the relevant executed proof gates; a dependency PR or successful compile
is not upgrade acceptance. Preserve exact source, configuration, input, tool and
artifact hashes in release evidence. Record unavailable CI or real-S3 gates as
blockers, never passes.

## Version reporting

`internal/buildinfo.Read` obtains Go module versions, replacements, toolchain and
VCS identity from the running binary's `runtime/debug` build information. Local
module replacements remain visible. Missing information is `unknown`; `(devel)`
is an unreleased Go build, not a release number. `Release` can be set by the
release builder using Go's `-ldflags -X` facility.

`tools/slatedb-native.json` is the intended native source/binding/toolchain pin.
It does **not** prove which library a particular executable loaded. The release
builder may set `internal/buildinfo.NativeCommit` and `NativeSHA256` only after
verifying the native build artifact and calculating its SHA-256. Both are empty
by default; only a complete pair is labelled `build-attestation`. This attests to
the packaged artifact, not runtime verification of a dynamically loaded library.
A loader-path override can invalidate that association. Native runtime identity
verification remains a separate packaging gate; never label the target manifest
pin as an observed loaded version.

The package adds no data-format version claim: supported storage formats and
upgrade admission require their own actual implementation and validation.
