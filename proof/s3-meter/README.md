# Local HTTP request accounting

`python3 scripts/prove.py s3-meter` runs committed partial-transfer controls and
signed SDK requests through the meter against the pinned disposable MinIO image.
The shared runner records exact source/input hashes, tool versions, commands,
assertions and cleanup. No real AWS endpoint or credentials are accepted by the
meter. Port 19009 belongs only to this isolated fixture.

The optional acceptance helper is `go run ./cmd/xenon-s3-meter -config
proof/s3-meter/local.json`. This profile reserves loopback 19007 for requests,
19008 for `GET /report`, and forwards only to the existing MinIO 19006. The current
smoke profile is unchanged. A future acceptance controller must explicitly point
both native SlateDB and directory AWS_ENDPOINT at 19007 and bind the config hash
and report to its evidence. On SIGTERM the helper drains requests for 10 seconds
and writes a final JSON report to stdout; report inflight must be zero for a
complete accounting window. Ready output is a distinct event before reports.

Counts are inbound HTTP attempts (client retries that reach the proxy count
again), final status codes, bytes read from request bodies, and bytes successfully
written from response bodies. Totals are published when each attempt finishes;
`inflight` exposes attempts not yet included. A 404 is a completed HTTP transfer,
not a successful S3 operation. Transport failures, read/write failures, aborted
responses and observed cancellation are separate counters and exclude completion.
They may overlap, so do not sum them as mutually exclusive classifications.

The byte totals exclude headers, TLS, HTTP framing and retransmission; AWS
content-encoding within an HTTP body would be included. They measure neither
remote peer receipt nor billable AWS requests. A Go transport can retry an
idempotent upstream connection internally without a second inbound attempt.
No per-object path, host, query, authorization header or credential is retained
in the report: only bounded method/status aggregates are stored in memory. SIGKILL
can lose the current aggregate; this disposable helper is never durable app
storage. Real-S3 traffic and equivalent measurement remain an external gate.
