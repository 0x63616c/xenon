#!/usr/bin/env bash
# Standalone bounded research runner. Production dependencies/code are unchanged.
set -euo pipefail
root=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
if [[ $# != 1 ]]; then echo "usage: $0 FRESH_EVIDENCE_DIRECTORY" >&2; exit 2; fi
mkdir "$1"
evidence=$(cd "$1" && pwd)
trap 'code=$?; printf "%s\n" "$code" >"$evidence/exit-code"; exit "$code"' EXIT
export GOENV=off GOWORK=off GOFLAGS=-mod=readonly GOTOOLCHAIN=go1.27.1 GOMAXPROCS=1
python3 - "$root" "$evidence" <<'PY'
import hashlib,json,os,pathlib,platform,subprocess,sys
root,out=map(pathlib.Path,sys.argv[1:]);paths=[root/'go.mod',root/'go.sum',root/'internal/ownership/join.go',root/'internal/registry/envelope.go']
paths+=sorted((root/'benchmarks/placement').glob('*.go'))+sorted((root/'benchmarks/placement').glob('go.*'))
paths+=sorted((root/'benchmarks/storage').glob('*.go'))+[root/'benchmarks/placement/scenarios.json',root/'benchmarks/storage/control_record.json',root/'benchmarks/placement/run.sh']
record={'source_revision':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),
 'tracked_dirty':bool(subprocess.check_output(['git','status','--porcelain'],cwd=root,text=True).strip()),
 'platform':platform.platform(),'toolchain':subprocess.check_output(['go','version'],env=os.environ,text=True).strip(),
 'gomaxprocs':1,'bench_iterations':100,'bench_repetitions':3,
 'input_hashes':{str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}}
(out/'provenance.json').write_text(json.dumps(record,indent=2)+'\n')
if record['tracked_dirty']:raise SystemExit('commit benchmark changes before qualification')
PY
# Assertions under the race detector are separate from timing measurements.
PLACEMENT_RESULTS="$evidence/placement.jsonl" go -C "$root/benchmarks/placement" test -race -v -count=1 -timeout=90s ./... 2>&1 | tee "$evidence/placement-tests.txt"
PLACEMENT_RESULTS="$evidence/placement-repeat.jsonl" go -C "$root/benchmarks/placement" test -count=1 -timeout=90s ./... >"$evidence/placement-repeat.txt" 2>&1
cmp "$evidence/placement.jsonl" "$evidence/placement-repeat.jsonl"
CONTROL_RECORD_RESULTS="$evidence/control-sizes.json" go -C "$root" test -race -v -count=1 -timeout=90s ./benchmarks/storage 2>&1 | tee "$evidence/storage-tests.txt"
go -C "$root/benchmarks/placement" test -run='^$' -bench=BenchmarkPlan -benchmem -benchtime=100x -count=3 -timeout=90s ./... 2>&1 | tee "$evidence/placement-bench.txt"
go -C "$root" test -run='^$' -bench=BenchmarkControlRecord -benchmem -benchtime=100x -count=3 -timeout=90s ./benchmarks/storage 2>&1 | tee "$evidence/storage-bench.txt"
printf 'passed\n' >"$evidence/status"
