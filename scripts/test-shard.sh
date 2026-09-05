#!/usr/bin/env bash
# Repeatable bounded shard proof; not the full system shipping gate.
set -euo pipefail
cd "$(dirname "$0")/.."
[[ "$(protoc --version)" == 'libprotoc 36.0' ]] || { echo 'protoc 36.0 required' >&2; exit 1; }
if [[ "${1:-}" == '--proof' && -n "$(git status --porcelain)" ]]; then
  echo 'Proof mode requires a clean committed tree; run without --proof for development tests.' >&2
  exit 1
fi
mkdir -p .local/evidence/shard
exec > >(tee .local/evidence/shard/result.txt) 2>&1
printf 'source_commit=%s\n' "$(git rev-parse HEAD)"
printf 'dirty=%s\n' "$(test -z "$(git status --porcelain)" && echo false || echo true)"
rustc --version
go version
protoc --version
shasum -a 256 Cargo.lock go.sum proof/shard/case.json proto/xenon/v1/persistence.proto scripts/test-shard.sh
cargo build --locked -p xenon-node
cargo test --locked -p xenon-node
go test ./internal/adapter -run 'TestShardRPC|TestShardTransportBoundsAndTypes' -count=1 -v
printf '%s\n' 'SHARD_SLICE_PASS (not a complete Xenon shipping verdict)'
