#!/usr/bin/env bash
# Primitive S3 emulator experiment; this is not the full Temporal proof.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f deploy/compose.yaml up -d --wait
export AWS_ACCESS_KEY_ID=xenon-local
export AWS_SECRET_ACCESS_KEY=xenon-local-test-only
export AWS_REGION=us-east-1
export AWS_ENDPOINT=http://127.0.0.1:19000
export AWS_ALLOW_HTTP=true
export AWS_EC2_METADATA_DISABLED=true
for attempt in $(seq 1 30); do
  if curl -fsS "$AWS_ENDPOINT/minio/health/live" >/dev/null; then break; fi
  sleep 1
done
if ! aws --endpoint-url "$AWS_ENDPOINT" s3api head-bucket --bucket xenon-probe >/dev/null 2>&1; then
  aws --endpoint-url "$AWS_ENDPOINT" s3api create-bucket --bucket xenon-probe >/dev/null
fi
export XENON_PROBE_BACKEND=s3
export XENON_PROBE_BUCKET=xenon-probe
export XENON_PROBE_PREFIX="probe-$(date -u +%Y%m%dT%H%M%SZ)-$RANDOM"
cargo run --locked -p slatedb-probe
printf '%s\n' 'Emulator primitive probe passed. Real S3 and Temporal end-to-end gates remain open.'
