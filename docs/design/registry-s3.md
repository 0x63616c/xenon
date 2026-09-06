# S3 registry backend (#114)

`internal/registry/s3` implements the new format-1 registry contract. It does not
change production `directory`/`ownership` objects or callers. Construct it with the
pinned AWS `*s3.Client`, bucket and a canonical nonempty prefix. Each logical key
maps to `prefix/key`, rejecting noncanonical segments and S3's 1024-byte key limit.
The encoded envelope is bounded to 1 MiB on ingress and read, including reads with
missing or dishonest content length. Successful reads validate the entire canonical
envelope and return owned bytes. Caller contexts supply the complete budget; the
backend creates no background context, timer or retry loop.

Every mutation first reads to detect current transition replay/reuse and validate
its observed condition, then publishes with `If-None-Match: *` or exact `If-Match`.
That read does not provide atomicity: S3's conditional PUT arbitrates the race.
There is no unconditional mutation API. Exact current transition/digest replay
returns its original record. Reusing the current transition with another digest is
invalid. Historical transition reuse cannot recreate old encoded bytes because the
new envelope binds its different expected version. ETags are opaque, key-scoped
versions; changing transition/expected bytes changes the object even for identical
application payloads. This depends on collision-resistant object/version identity
and the backend honoring conditional writes, not on application payload inequality.

The namespace must exclude deletes, object lifecycle expiry, restoration of old
objects and external unconditional writers. Enforce that boundary in deployment
permissions before production cutover; this adapter cannot police other S3 clients.
It neither requires versioned buckets nor attempts to CAS S3 VersionID (which is not
an If-Match condition). It offers no unbounded journal proving historical identity
reuse or publication after a later transition replaces the receipt.

## Single attempts and ambiguity

Source inspected: `aws-sdk-go-v2@v1.45.1/aws/retryer.go` defines `NopRetryer` with
`MaxAttempts() == 1` and every error non-retryable.
`service/s3@v1.110.0/api_client.go` applies operation options before
`finalizeOperationRetryMaxAttempts`; the backend sets both `Retryer=NopRetryer{}`
and `RetryMaxAttempts=1` per PUT. The concrete client prevents a mock implementation
from silently ignoring those options. Custom HTTP transports/middleware must not
implement their own mutation replay. The tests configure four client attempts and
assert exactly one actual HTTP PUT for 409, 412, 500, 503 and dropped responses.

A successful nonempty ETag returns the encoded publication. After an ambiguous
response, exact authoritative readback may establish success. An unchanged old
record, missing record, later record or failed/corrupt read cannot prove historical
nonpublication and returns outer `UnknownOutcome`. A single service 412 is a known
nonpublication conflict; there was no earlier SDK attempt. Cancellation after PUT
dispatch is unknown even if the service committed, and a canceled caller never gets
a reconciled late success. Failures before dispatch are unavailable/corrupt/invalid
as appropriate. Callers that retry after an earlier unknown result must preserve
that aggregate ambiguity across **separate API invocations**, using `Reconcile`;
a subsequent call's conflict is not proof about the earlier call.

## Reproduction

Run from a clean committed checkout with Docker, Compose and the pinned toolchain:

```sh
python3 scripts/prove.py registry-contracts
```

The existing directory proof controller creates an isolated Compose project using
`deploy/directory.compose.yaml`'s digest-pinned MinIO and scoped disposable volume.
It uses loopback port 19003; do not run another directory/registry proof on that port
at the same time. `test/scenarios/registry/s3.json` records the explicit 90-second
budget and ordered schedule. Go fixtures commit exact identities, payloads and
fault behavior. No real cloud credentials are used. Controller `finally` and outer
proof cleanup both run `docker compose down --volumes` for the owned project;
remaining scoped containers/volumes fail controller cleanup. Logs and provenance
stay in `.local/evidence/` after resource teardown.

The runner records source revision, dirty state, input hashes, tool versions,
command outputs and assertions. The controller records Docker and Compose versions;
the exact MinIO image digest is a hashed manifest input. A dirty invocation is only
development evidence; clean-checkout completion is the reproducible gate.

The shared `contracttest.Run(t, ctx, store)` requires an empty exclusive namespace;
its caller owns setup, context and cleanup. S3-only fault tests make signed calls to
real MinIO and drop the successful HTTP response before delivering it to the SDK.
One schedule first publishes a subsequent conditional record through an independent
client. Cancellation is injected at the same actual successful PUT boundary. These
are executed conditional-storage/HTTP-loss tests, not a simulated storage model.

Local emulator success does not qualify AWS, prove emulator server-power-loss
behavior, perform production migration, or establish the later controller/native
engine acceptance gates. Filesystem backend qualification is separate.
