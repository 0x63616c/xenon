#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
protoc_dir="$(python3 scripts/install-protoc.py)"
export PATH="$protoc_dir:$PATH"
if [[ "$(protoc --version)" != 'libprotoc 36.0' ]]; then
  printf '%s\n' 'Install protoc 36.0 before generating the pinned wire contract.' >&2
  exit 1
fi
mkdir -p .local/tools
GOBIN="$PWD/.local/tools" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10
PATH="$PWD/.local/tools:$PATH" protoc --go_out=. --go_opt=module=github.com/0x63616c/xenon proto/xenon/v1/query.proto

GOBIN="$PWD/.local/tools" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
PATH="$PWD/.local/tools:$PATH" protoc --go_out=. --go_opt=module=github.com/0x63616c/xenon --go-grpc_out=. --go-grpc_opt=module=github.com/0x63616c/xenon proto/xenon/v1/persistence.proto
PATH="$PWD/.local/tools:$PATH" protoc --go_out=. --go_opt=module=github.com/0x63616c/xenon --go-grpc_out=. --go-grpc_opt=module=github.com/0x63616c/xenon proto/xenon/v1/cluster.proto
