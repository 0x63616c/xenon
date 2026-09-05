# Nexus HTTP transport

Run `python3 scripts/prove.py nexus-http` from a clean checkout. The gate uses pinned Temporal v1.31.2 config.Load (including Validate), rejects removed required fields, and executes the real launcher `--check-config` for both configurations without starting services. Functional named-endpoint readiness and the saved fuzz corpus remain separate runtime gates.

Pinned source contract: docs/architecture/nexus.md “Enabling Nexus” requires services.frontend.rpc.httpPort and clusterMetadata.clusterInformation.active.httpAddress. service/frontend/service.go:493 emits the missing-HTTP warning observed in failed run96ea9ccced55. common/cluster/frontend_http_client.go:49 rejects an absent HTTPAddress; it expects host:port and determines HTTP/TLS scheme separately.

Frontend A listens on18243, B on19243, following the pinned test convention of frontend gRPC port+10 (common/cluster/metadata_test_config.go:22). Both advertise127.0.0.1:17243 through the existing HAProxy service, with a separate TCP HTTP listener and both frontend backends. Compose publishes only loopback17243. gRPC remains17233 and Xenon ingress17935; no additional frontend process or application storage is introduced.

Both configs additionally set publicClient.httpHostPort to127.0.0.1:17243. This is necessary for different per-process HTTP ports: common/resource/fx.go:486 otherwise uses membership discovery; common/rpc/rpc.go's roundTripper replaces a selected host's port with the calling process's configured frontend HTTPPort. The explicit stable address avoids incorrectly dialing B with A's HTTP port (or the reverse). HAProxy TCP mode preserves HTTP paths, headers and bodies.

The1.31.2 source uses system callback behavior; do not add removed useSystemCallbackURL configuration from older deployment examples. External callback destinations and real-S3 credentials are outside this local proof. This change repairs missing transport configuration; it does not claim to repair the independently observed matching queue backlog or to prove async Nexus recovery.
