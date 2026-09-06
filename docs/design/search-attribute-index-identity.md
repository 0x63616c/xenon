# Search attribute index identity and the hosted join failure

## Reproduced configuration defect

The embedded Temporal visibility datastore specified `options.index` as
`xenon-visibility` but omitted `customDatastore.indexName`. In pinned Temporal
v1.31.2, `common/config/persistence.go`'s `DataStore.GetIndexName` reads the latter.
`temporal/fx.go` uses that value when initializing cluster metadata's preallocated
custom search-attribute slots. Xenon's `VisibilityFactory` instead reads the
option and returns `xenon-visibility` from the actual store. Startup therefore
initialized slots under the empty index while workflow validation read another
index. A later SDK-probe seed masked this configuration dependency.

The embedded config now explicitly names `xenon-visibility` in both places.
`TestStartupSearchAttributeIndexMatchesVisibilityStore` constructs the actual
configuration and custom visibility store without starting a backend. Before the
fix it fails with `startup seeds index "", runtime reads "xenon-visibility"`;
afterward it passes. The existing standalone ministack fixtures retain their
historical explicit probe-seeding journey; this correction changes the active
embedded-agent configuration and does not rewrite their frozen inputs.

## Hosted evidence and attribution limit

Run [34053219690](https://github.com/0x63616c/xenon/actions/runs/34053219690), revision
`5f1dc97bd62231591b43882b53f0b7bd9989d88c`, retains artifact
[9995276036](https://github.com/0x63616c/xenon/actions/runs/34053219690/artifacts/9995276036).
Receipt `agent-20260906T185536-f89e1c` failed with Omes exit 1. Its primary is
iteration 3's workflow-start rejection at `19:01:24.781Z`:
`search attribute OmesExecutionID is not defined`. The subsequent canceled
iteration is shutdown fallout. This remains a failed workload gate.

A and B initialized at approximately `18:59:27Z` and `18:59:32Z`; bootstrap
completed at `18:59:38Z`. B logged that `OmesExecutionID` already existed at
`19:00:56.137Z`. C initialized at `19:01:03.087Z`. C also logged a failed search
attribute metadata read against the killed B at `19:01:23.971Z`. These observations
show a schema lookup failure during failover; they do not identify which process
returned Omes's missing-attribute response or its cached/persisted map. The index
mismatch is a reproduced defect, not a demonstrated complete explanation of this
particular hosted failure.

## Metadata writes and cache behavior

The reviewed pinned startup path reads the current cluster record, adds missing
preallocated fields without replacing existing fields, and saves with the exact
read version (`temporal/fx.go`, `updateCurrentClusterMetadataRecord` and
`updateIndexSearchAttributes`). Xenon's `ClusterService` stages `ApplyCluster`
and its replay outcome under one admitted transaction and awaits durability.
`ApplyCluster` rejects a different current version, increments an accepted version
by one and preserves the submitted serialized blob. No source path was found
where a stale startup save silently overwrites a newer search-attribute map.

For custom visibility, `OperatorService.AddSearchAttributes` allocates existing
cluster slots and updates namespace aliases through `UpdateNamespace`; it does
not register the aliases as independent cluster slot names. Namespace persistence
uses its notification-version condition. The SDK probe seeds cluster slots first,
then calls this operator API.

Pinned `common/searchattribute/manager.go` caches the entire cluster map for 60
seconds and returns an empty custom map for an absent index. Startup's missing
index can therefore be observed as an empty map before later probe seeding.
A non-forced read need not immediately observe that later seed. The validator
reports the user alias even when the missing field is its underlying allocated
slot. Namespace aliases have a separate registry cache (default refresh 2s).
These are propagation behaviors; the hosted evidence does not prove which cache
state was present at failure.

## Supported schema observations before workload admission

A bounded schema check should retain each instance address and the returned
maps, rather than checking only the load-balanced frontend health service:

- `OperatorService.ListSearchAttributes` with the workload namespace forces a
  cluster search-attribute refresh on that frontend and combines it with the
  namespace's persisted aliases. Require `XenonProof`, `OmesExecutionID`,
  `KS_Keyword` as Keyword and `KS_Int` as Int.
- `WorkflowService.DescribeNamespace` returns the namespace configuration's
  `CustomSearchAttributeAliases`; record the field-to-alias mapping.
- `WorkflowService.GetSearchAttributes` reads that frontend's regular cached
  index map. Verify that each mapped field has the corresponding expected type.

Those APIs establish the observed frontend schema; they do not flush every
history/worker process cache or guarantee future storage availability. If the
harness requires an end-to-end readiness gate, an explicit stable-ID canary via
`StartWorkflowExecution` and completion/history observation must exercise the
actual namespace and attributes before counted Omes work. Such a gate must use
its declared setup budget and preserve any failed Omes iteration as failure; it
must not extend workload deadlines or suppress schema errors. This batch does
not introduce that additional harness journey or claim the hosted failure closed.
