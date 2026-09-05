# Runtime scenarios

Scenario-local configurations, fixtures, tool pins and component manifests belong
inside one named directory. Shared command-line runners remain under `scripts/`.
The first migrated scenario is `ministack/`; existing proof CLI names remain valid.

`make check-layout` validates both unmigrated `experiments/` and migrated scenario
manifests, plus scenario runner/input links. `ministack/migration.json` binds the
initial relocation to exact original bytes. A later intended input change needs
its own reviewed contract update; this migration does not authorize one.

The full acceptance profile and Omes corpus remain under `proof/` with their
existing integrity hashes. Do not move or regenerate those bytes as incidental
cleanup. Historical reports retain their original source paths and commits.

Next slices can migrate one coherent scenario at a time: add an explicit resolver
entry in `scripts/prove.py`, move its manifest and fixtures, update current callers,
and preserve exact input content. Do not leave duplicate live manifests in both
locations. A layout/control pass is not a new native or runtime proof pass.
