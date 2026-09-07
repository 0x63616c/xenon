# GitHub labels

Labels describe facts that are not already represented by the GitHub Project or
the issue hierarchy. Keep the vocabulary small. Project `Status` owns workflow
state, milestones own release membership, assignees own claims, and native
relationships own parent and blocking edges.

## Vocabulary

| Label | Use |
| --- | --- |
| `type:bug` | Something is broken and needs a fix. |
| `type:docs` | Documentation, guides, website copy, or diagrams. |
| `type:spike` | A time-boxed investigation or prototype outside committed delivery scope. |
| `backlog` | Possible future work; not committed delivery scope. |
| `idea` | An idea to consider later; not committed delivery scope. |
| `release:blocker` | Required to complete the current release milestone. |
| `blocked:external` | Waiting on external access, credentials, service state, or spend. |
| `wayfinder:map` | The canonical Wayfinder map or delivery epic. |
| `wayfinder:research` | Bounded research that resolves an evidence question. |
| `wayfinder:grilling` | An architecture or product decision requiring structured review. |
| `wayfinder:task` | Autonomous implementation or validation work. |
| `dependencies` | Applied automatically to dependency-update pull requests. |
| `go` | Applied automatically to Go dependency-update pull requests. |

An ordinary implementation issue needs no type label. Add a `type:*` label only
when the distinction changes how the work is handled. A Wayfinder child carries
exactly one `wayfinder:*` role. `release:blocker` supplements, rather than
replaces, its milestone. Use `blocked:external` only for an external dependency;
ordinary dependency blocking belongs in GitHub's native blocked-by relationship.

Either `backlog` or `idea` means the work may be picked up later. Neither label
authorizes starting it or makes it a release requirement. Keep deferred ideas
without a delivery milestone or active claim until explicitly selected.

## Do not encode

- Do not add Todo, Ready, In Progress, Blocked, or Done labels; use Project
  `Status`.
- Do not add priority labels; use a Project field.
- Do not add release-version labels; use milestones.
- Do not add broad `enhancement` or `task` labels to ordinary implementation
  issues. Their titles, hierarchy, and acceptance criteria already say that.
- Add newcomer and community labels only when the repository is ready to accept
  that class of contribution.
