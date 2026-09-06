# Explicit S3 owner management proof

From a clean checkout with Docker Compose, Python 3, Git, Go 1.27.1, Rust 1.94.0
and a C toolchain, run:

```sh
python3 scripts/prove.py owner-manager
```

The manifest builds the pinned native library and Go node, runs actual MinIO with
scoped resources on local port 19004, and executes the committed race-enabled
fixture. It checks conditional topology races and lost responses, multiple actual
node processes, any-node shard requests, movement of two partitions, same-member
incarnation activation, active-owner SIGKILL/recovery, a captured old read racing a
new fence, two finite paused native openers with durable outcome recovery, and the
topology-read/READY-CAS race. The JSON result records source/configuration/tool and
native/binary hashes. The controller and outer runner clean only their Compose
project. Dirty runs are development results with proof_pass=false.

For managed operation, set XENON_TOPOLOGY_PREFIX, XENON_BUCKET, XENON_NODE,
XENON_LISTEN, optionally XENON_ADVERTISE, and explicit AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY, optional AWS_SESSION_TOKEN and AWS_DEFAULT_REGION (or
AWS_REGION). The native library must be on LD_LIBRARY_PATH or DYLD_LIBRARY_PATH.
AWS_ENDPOINT plus AWS_ALLOW_HTTP=true and AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false
select the local emulator. Credentials never belong in topology JSON.

A process announces `INGRESS node incarnation address`; ingress availability is
not partition readiness. Copy the exact announced incarnation/address into the
members object in a topology JSON and explicitly assign partitions:

```json
{"members":{"node-a":{"address":"127.0.0.1:7235","incarnation":"<announced UUID>"}},"partitions":{"shard/1":{"node":"node-a","data_prefix":"data/shard-1"}}}
```

`go run ./cmd/xenon-topology topology.json` conditionally publishes that intention
against the exact currently observed topology. A concurrent admin change fails
rather than silently overwriting it. Existing partition prefixes cannot be rebound
or removed. Adding a process does not automatically activate it or move assignments;
publish those changes explicitly. On owner death, activate a freshly announced
replacement and explicitly reassign the partition. Metadata/data prefixes must be
disjoint. Configuration is bounded to 64 members, 256 partitions and 64 KiB.

Without XENON_TOPOLOGY_PREFIX the historical single-partition experiment mode
remains available for existing component fixtures; it is not the coordinated mode.
Automatic balancing/failure detection, ambient IAM credential discovery, complete
Temporal boot, real AWS and Omes remain separate acceptance gates. This proof sends
shard RPCs; descriptor coverage checks all generated service registrations, while
other families retain their component proofs until combined integration testing.
