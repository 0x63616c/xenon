#!/usr/bin/env bash
# Compatibility entrypoint: the registered declarative runner owns proof verdicts.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ "${1:-}" == '--proof' ]]; then shift; fi
exec python3 scripts/prove.py shard "$@"
