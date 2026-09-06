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
An illustrative single-machine configuration is:

```json
{
  "cluster": "example",
  "node": "agent-a",
  "bucket": "customer-xenon",
  "prefix": "clusters/example",
  "bind_ip": "127.0.0.1",
  "advertise_ip": "127.0.0.1",
  "base_port": 17233,
  "public_address": "127.0.0.1:17233",
  "public_http_address": "127.0.0.1:17242",
  "diagnostics_address": "127.0.0.1:17250",
  "history_shards": 4,
  "bootstrap": true
}
```

This example is configuration documentation, not executed deployment evidence.
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
- `internal/temporalruntime` owns upstream configuration and server embedding.
- `internal/temporalstore` and `internal/adapter` adapt persistence contracts.
- `internal/storage` assembles ownership, routing and the current embedded engine.
- `internal/node` still implements operation semantics using native transactions.
- `internal/replay` shares journal decisions with a controlled replay test.

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

Joining is automatic at startup; failed-member eviction is not. Recovery requires
same-node replacement or explicit topology administration. Production ownership,
routing and replay under controlled scheduling still need their full deterministic
scenario and trace/provenance runner. The replay unit scenario alone is narrower.
No hosted control plane, billing or custom Kubernetes operator is introduced here.
