# Hosted MinIO smoke receipt

Unchanged `unified-agent-evidence` artifact 9997535719 from
[run 34060699429](https://github.com/0x63616c/xenon/actions/runs/34060699429),
receipt `agent-20260906T212003-ee86b0`: clean `component-passed`,
`full_acceptance: false`.

Workflow PR head: `0b497cc07745067db06c5e175174c7d7f81f77a6`.
Actual tested checkout and binary revision:
`692e0fcd947692e28a2609cdd8a042dbe8e84529` (GitHub test merge).
Its parents are `f1af9c155249ed441723cb086ed06f4abb84e5d1` and that PR head.
GitHub commit API confirms both head and merge have identical source tree
`9f4494301f5fc24d6df4a3f90fa06cb4118ef157`; build revisions remain distinct.

Independent artifact review matched every recorded command output and aggregate
log hash against downloaded files. The receipt records no cleanup errors.
This is the finite Linux MinIO unified-agent smoke schedule, not overall CI,
ten-minute, real AWS S3, or exhaustive safety acceptance. Full logs remain in
the hosted artifact; historical executable bytes are not archived here.

Receipt SHA-256: `d964109620a307b273305d49a345ecd0346143e40056482d6c4918130f242a50`.
Verified 71 command hashes and 83 aggregate log hashes.
