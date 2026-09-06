# Unified Xenon agent

The default product direction is one Xenon installation and a fleet of identical
agents. Each agent embeds upstream Temporal services and Xenon persistence. The
customer connects existing Temporal SDKs and application workers to a public
Temporal endpoint; application workers still run separately.

This user-authorized direction supersedes a separately operated Temporal fleet as
the default experience. Existing `xenon-temporal`, `xenon-go-node` and proof commands
remain available for advanced integration and regression testing. Combining the
runtimes does not remove internal RPC or make replicas independent databases.

## Implemented assembly

`cmd/xenon` exposes `version`, `check-config --config FILE` and `start --config FILE`.
The shared configuration generates upstream Temporal configuration internally.
The complete single-machine example is [deploy/agent.example.json](../../deploy/agent.example.json).
Copy it to `agent.json`, set your bucket and prefix, and update every layout path
under that prefix before starting a new cluster. The explicit `service_storage`
format 2 block pins the ordered logical-to-physical partition layout, stable
cluster/node IDs, placement configuration, timing and resource limits. All nodes
in a cluster share the same layout and cluster ID; each node needs a distinct
stable node ID and reachable advertised address.

The example selects `bootstrap: true` and `fresh_namespace: true` for an explicitly
fresh namespace. It is not a legacy-format migration configuration. Preserve the
cluster ID, layout ordering, partition IDs and paths once initialized; joining
nodes use that same layout with `fresh_namespace: false`. See
[fresh runtime bootstrap](fresh-runtime-bootstrap.md) for the empty-namespace and
existing-state checks.

Check it with `xenon check-config --config agent.json`, then start with
`xenon start --config agent.json` using an appropriately built executable and
external AWS credentials/region. Configuration checking does not contact S3 or
prove credentials, network reachability or existing-state compatibility.

Reserve ten consecutive ports from `base_port`: offsets 0–3 are Temporal service
RPC, 4–7 are membership, 8 is internal Xenon persistence, and 9 is frontend HTTP.
On separate machines, configure reachable advertised IPs; on the same machine use
nonoverlapping port blocks. Agents share bucket, prefix, cluster and fixed history
settings, but use distinct node IDs. The public addresses must route to the
cluster's public frontend services. This CLI does not provision a load balancer.
Current networking is plaintext and requires a trusted experimental network;
authenticated production deployment is not established by this configuration.

Storage registers the process once through `ownership.Join`. Only explicit
bootstrap may create missing topology. S3 metadata resides beneath
`prefix/metadata`, with partition data beneath `prefix/data`. The ten fixed logical
partitions and their durable prefixes cannot change during joining. Sorted member
IDs receive balanced sorted partitions. See [membership](agent-membership.md) for
CAS retries, identity collision and placement limitations.

The agent starts storage first, probes routed persistence, then constructs and
starts Temporal and checks its API health. Periodic probes detect loss of service.
On termination it stops Temporal while storage remains available, then retires
storage admission. Bounded lifecycle timeouts require whole-process termination;
Go cancellation cannot cancel an in-flight native call. The current assembly does
not expose a separate public readiness HTTP endpoint merely because it maintains
an internal readiness flag.

## Small integration boundaries

- `internal/agent` assembles lifecycle and customer configuration.
- `internal/temporal` owns upstream configuration and server embedding.
- `internal/temporalstore` and `internal/adapter` adapt persistence contracts.
- `internal/storage` assembles ownership, routing and the current embedded engine.
- `internal/node` still implements operation semantics using native transactions.
- `internal/persistence` shares journal decisions with a controlled replay test.

SlateDB is the current implementation behind S3 durability, not a customer-facing
product requirement or an interchangeable-engine framework. Native transaction
usage remains in node handlers; the assembly wrapper alone does not make replacing
the engine automatic. Temporal updates likewise require actual compatibility and
existing-state testing despite using upstream embedding APIs rather than a fork.

`xenon version` reports actual Go build/module metadata, including replacements.
Native identity is unknown unless a builder supplies an explicit artifact
attestation; the target pin is not runtime proof of the loaded library. See
[dependency updates](../dependency-updates.md).

## Gates still requiring evidence

Unit tests and assembly source are not evidence of a production service. Retain
separate gates for multiple unified agents running SDK/UI/Omes workloads, real
ownership movement, cold recovery, real S3, container packaging, and an actual
old/new Temporal upgrade over existing state. Neither mixed-version operation nor
rollback is implied by the wrapper.

Joining and failed-member eviction are automatic. Recovery requires
same-node replacement or explicit topology administration. Production ownership,
routing and replay under controlled scheduling still need their full deterministic
scenario and trace/provenance runner. The replay unit scenario alone is narrower.
No hosted control plane, billing or custom Kubernetes operator is introduced here.

## Container packaging slice

`Dockerfile.unified-agent` provides a declared container build for
`cmd/xenon`:

- Multi-stage build runs `scripts/build-go-node.py` in the declared toolchain stage to
  produce the `xenon` binary and pinned SlateDB native library.
- Runtime image executes as non-root UID/GID `65532`.
- Image build performs smoke validation using:
  - `xenon version`
  - `xenon check-config --config /etc/xenon/agent.example.json`

From this checkout:

```sh
docker build -f Dockerfile.unified-agent -t xenon-unified-agent:local .
docker run --rm xenon-unified-agent:local version
docker run --rm xenon-unified-agent:local check-config --config /etc/xenon/agent.example.json
```

This host has not completed that image build because its disk filled while linking
the existing native test stack. The recipe is reviewable, but container execution
remains an open delivery check until CI or another clean host builds and runs it.

For container deployment, copy and edit the example as `agent.json`; set a bind
address and advertised address reachable on your container network, and expose
the required ports. Pass that real config at runtime:

```sh
docker run --rm \
  -v "$(pwd)/agent.json:/etc/xenon/agent.json:ro" \
  -p 7233:7233 \
  -e AWS_ENDPOINT=http://host.docker.internal:19006 \
  -e AWS_ACCESS_KEY_ID=... \
  -e AWS_SECRET_ACCESS_KEY=... \
  xenon-unified-agent:local start --config /etc/xenon/agent.json
```
