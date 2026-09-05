# Conditional S3 directory primitive

Run `python3 scripts/prove.py directory` from a clean checkout with Docker Compose and Go1.27.1. The runner starts pinned MinIO on loopback19003 in an isolated project, executes actual AWS SDK conditional PUTs and fault assertions, captures source/config/tool/output hashes and verifies scoped cleanup. SDK versions match pinned Temporal1.31.2. Test credentials are local fixtures.

Opening and ready records bind partition/data prefix, monotonic generation, random transitionUUID and desired process incarnation. Snapshot/ETag fields are private and directory-bound. A reservation is locally one-shot; uncertain PUT results are reconciled only by exact fresh readback. Changed owners are conflicts; unchanged prewrite state remains unknown. All operations have a5s ceiling and earlier caller deadlines. Record reads cap4096bytes.

READY is only recorded intent. This package does not open/fence SlateDB or implement routing.Resolver, controller, lease, leader election or safe ownership transfer. Application keys are never deleted by this module. External deletion/rollback of directory objects is outside the protocol; ABA resistance assumes authorized writers use this monotonic API. Real AWS remains unexecuted.
