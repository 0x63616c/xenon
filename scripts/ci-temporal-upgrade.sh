#!/usr/bin/env bash
set -euo pipefail

root=$(git rev-parse --show-toplevel)
old_version=v1.31.1
target_version=v1.31.2
old_temporal=e015b7a8c24d327a72e9dab8526966f4f58ba501
target_temporal=19a774302c613da9adc4436ab14278ccdca8e0a5
work=$(mktemp -d)
evidence="$root/.local/evidence/temporal-upgrade-${old_version}-${target_version}"
mkdir -p "$evidence"
trap 'rm -rf "$work"' EXIT
python3 - "$evidence/selection.json" "$old_version" "$old_temporal" "$target_version" "$target_temporal" <<'PY'
import json,sys
from pathlib import Path
Path(sys.argv[1]).write_text(json.dumps({'schema':1,'old_version':sys.argv[2],'old_commit':sys.argv[3],'target_version':sys.argv[4],'target_commit':sys.argv[5]},indent=2)+'\n')
PY

git clone --quiet --no-hardlinks "$root" "$work/old-xenon"
git clone --quiet --no-hardlinks "$root" "$work/target-xenon"
git clone --quiet --filter=blob:none --branch "$target_version" --depth=1 https://github.com/temporalio/temporal.git "$work/target-temporal"
git -C "$work/target-temporal" fetch --quiet --depth=1 origin "refs/tags/${old_version}:refs/tags/${old_version}"
git clone --quiet --filter=blob:none --branch "$old_version" --depth=1 https://github.com/temporalio/temporal.git "$work/old-temporal"

python3 "$root/scripts/temporal_upgrade.py" \
  --source "$work/target-temporal" --old "$old_temporal" --new "$target_temporal" \
  --output "$evidence/impact"

git -C "$work/old-xenon" config user.name 'Xenon upgrade proof'
git -C "$work/old-xenon" config user.email 'upgrade-proof@invalid.example'
(
  cd "$work/old-xenon"
  GOTOOLCHAIN=go1.27.1 GOENV=off GOWORK=off go get "go.temporal.io/server@${old_version}"
  GOTOOLCHAIN=go1.27.1 GOENV=off GOWORK=off go mod tidy
  git add go.mod go.sum
  git commit --quiet -m "Proof fixture: Temporal ${old_version}"
)
git -C "$work/old-xenon" rev-parse HEAD > "$evidence/old-xenon-commit.txt"
git -C "$work/old-xenon" rev-parse HEAD^ > "$evidence/old-xenon-parent.txt"
git -C "$work/old-xenon" format-patch -1 --stdout > "$evidence/old-xenon.patch"
git -C "$work/old-xenon" bundle create "$evidence/old-xenon.bundle" HEAD
cp "$work/old-xenon/go.mod" "$evidence/old-go.mod"
cp "$work/old-xenon/go.sum" "$evidence/old-go.sum"

build_bundle() {
  local checkout=$1
  (
    cd "$checkout"
    python3 scripts/build-go-node.py
    export GOTOOLCHAIN=go1.27.1 GOENV=off GOWORK=off GOFLAGS=-mod=readonly CGO_ENABLED=1
    export CGO_LDFLAGS="-L$checkout/.local/slatedb-native-target/debug"
    export LD_LIBRARY_PATH="$checkout/.local/slatedb-native-target/debug"
    go build -o .local/bin/xenon-topology ./cmd/xenon-topology
    go build -o .local/bin/xenon-sdk-probe ./cmd/xenon-sdk-probe
    go build -tags ministack -o .local/bin/xenon-temporal ./cmd/xenon-temporal
  )
}

build_bundle "$work/old-xenon"

# The native engine pin is identical across the pair. Copy its verified source,
# library and manifest; rebuild every target Go executable from the target commit.
mkdir -p "$work/target-xenon/.local/slatedb-native-target/debug" "$work/target-xenon/.local/bin"
cp -a "$work/old-xenon/.local/slatedb-native-source" "$work/target-xenon/.local/"
cp "$work/old-xenon/.local/slatedb-native-target/debug/libslatedb_uniffi.so" "$work/target-xenon/.local/slatedb-native-target/debug/"
cp "$work/old-xenon/.local/go-node-build.json" "$work/target-xenon/.local/"
(
  cd "$work/target-xenon"
  export GOTOOLCHAIN=go1.27.1 GOENV=off GOWORK=off GOFLAGS=-mod=readonly CGO_ENABLED=1
  export CGO_LDFLAGS="-L$work/target-xenon/.local/slatedb-native-target/debug"
  export LD_LIBRARY_PATH="$work/target-xenon/.local/slatedb-native-target/debug"
  go build -o .local/bin/xenon-go-node ./cmd/xenon-go-node
  go build -o .local/bin/xenon-topology ./cmd/xenon-topology
  go build -o .local/bin/xenon-sdk-probe ./cmd/xenon-sdk-probe
  go build -tags ministack -o .local/bin/xenon-temporal ./cmd/xenon-temporal
  python3 - <<'PY'
import hashlib,json
from pathlib import Path
p=Path('.local/go-node-build.json'); value=json.loads(p.read_text())
value['node_binary_sha256']=hashlib.sha256(Path('.local/bin/xenon-go-node').read_bytes()).hexdigest()
p.write_text(json.dumps(value,indent=2)+'\n')
PY
)

python3 "$root/scripts/upgrade_state.py" bundle --xenon-source "$work/old-xenon" \
  --temporal-source "$work/old-temporal" --temporal-commit "$old_temporal" \
  --temporal-version "$old_version" --output "$evidence/old-bundle.json"
python3 "$root/scripts/upgrade_state.py" bundle --xenon-source "$work/target-xenon" \
  --temporal-source "$work/target-temporal" --temporal-commit "$target_temporal" \
  --temporal-version "$target_version" --output "$evidence/target-bundle.json"

python3 - "$evidence" <<'PY'
import json,sys
from pathlib import Path
p=Path(sys.argv[1])
plan={'schema':1,'old':str((p/'old-bundle.json').resolve()),'target':str((p/'target-bundle.json').resolve()),'rehearsal':False}
(p/'plan.json').write_text(json.dumps(plan,indent=2)+'\n')
PY

docker compose -f "$root/test/scenarios/ministack/config/compose.json" pull s3 ingress
python3 "$root/scripts/upgrade_state.py" run --plan "$evidence/plan.json" --evidence "$evidence/run"
