#!/usr/bin/env bash
# Exact-revision Linux overlayfs proof; no host bind mounts or package installs.
set -euo pipefail
root=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
revision=$(git -C "$root" rev-parse "${2:-HEAD}^{commit}")
image=golang@sha256:648f440f42a0958804efb24df176f806f9d353b41f1c0627f666428e40310f6b
platform=linux/arm64
if [[ $# -lt 1 ]]; then
  echo "usage: $0 FRESH_EVIDENCE_DIRECTORY [SOURCE_REVISION]" >&2
  exit 2
fi
mkdir "$1"
evidence=$(cd "$1" && pwd)
temporary=$(mktemp -d)
container="xenon-fs-linux-$(basename "$temporary" | tr '[:upper:]' '[:lower:]')"
created=0
cleanup() {
  status=$?
  printf '%s\n' "$status" >"$evidence/primary-exit-code"
  trap - EXIT
  if [[ $created == 1 ]]; then
    mkdir -p "$evidence/results"
    if ! docker cp "$container:/evidence/." "$evidence/results" >>"$evidence/cleanup.log" 2>&1; then
      echo 'evidence copy failed' >>"$evidence/cleanup.log"
      status=1
    fi
    if ! docker rm -f "$container" >>"$evidence/cleanup.log" 2>&1; then
      status=1
    fi
  fi
  rm -rf "$temporary"
  printf '%s\n' "$status" >"$evidence/exit-code"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
printf 'source_revision=%s\nimage=%s\nplatform=%s\n' "$revision" "$image" "$platform" >"$evidence/provenance.txt"
shasum -a 256 "$0" >>"$evidence/provenance.txt"
docker pull --platform "$platform" "$image" >"$evidence/image-pull.log" 2>&1
docker image inspect "$image" >"$evidence/image-inspect.json"
docker version --format '{{json .Server}}' >"$evidence/docker-server.json"
# A real shallow checkout preserves git provenance for the omission runner.
git init -q "$temporary/source"
git -C "$temporary/source" fetch -q --depth=1 "$root" "$revision"
git -C "$temporary/source" checkout -q --detach FETCH_HEAD
docker create --platform "$platform" --name "$container" \
  --label xenon.proof=registry-filesystem --workdir /work \
  --env GOENV=off --env GOWORK=off --env GOFLAGS=-mod=readonly \
  --env GOTOOLCHAIN=go1.27.1 --env TMPDIR=/proof-tmp \
  --env GIT_CONFIG_COUNT=1 --env GIT_CONFIG_KEY_0=safe.directory --env GIT_CONFIG_VALUE_0=/work \
  "$image" timeout --signal=TERM --kill-after=10s 600s bash -c '
    set -euo pipefail
    mkdir -p /evidence /proof-tmp
    { git rev-parse HEAD; go version; python3 --version; uname -a;
      stat -f -c "filesystem=%T" /proof-tmp; cat /etc/os-release;
      sha256sum go.mod go.sum internal/registry/filesystem/*.go;
    } > /evidence/environment.txt
    test "$(stat -f -c %T /proof-tmp)" = overlayfs
    go test -race -count=1 -timeout=90s -v ./internal/registry/filesystem 2>&1 | tee /evidence/go-test.log
    python3 test/scenarios/registry-filesystem/negative-controls.py --evidence /evidence/negative-controls
  ' >"$evidence/container-id"
created=1
docker cp "$temporary/source/." "$container:/work"
docker start -a "$container" 2>&1 | tee "$evidence/container.log"
