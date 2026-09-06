# Filesystem registry qualification (#114)

The opt-in adapter is not wired into production authority or SlateDB. Filesystem
registry support makes no claim about the engine's supported storage. The
canonical envelope is unchanged; this adapter adds an `XENREG01` header and
nonzero uint64 generation, checked against the envelope's predecessor. Replacements
increment generation even for identical payloads; overflow fails closed.

Each key has a separate stable `.lock` inode. Under exclusive `flock`, the adapter
rereads/validates, compares the exact version, writes `.tmp`, fsyncs it, renames it
to `.record`, and fsyncs the directory. Read takes the same exclusive lock and
fsyncs file plus directory before returning, recovering a writer killed between
rename and directory sync. Failed recovery returns no record. From rename dispatch,
errors/cancellation are `UnknownOutcome`; no automatic mutation retry obscures it.
Same-transition/same-digest replay returns the recovered publication; changed
digest is invalid. Historical receipts remain the application's responsibility.

Provision the directory and ancestor entries durably before opening. Every
participant must use this protocol; unrelated writers must not have access. Never
unlink lock files, delete records or reset this namespace while participants run.
`os.Root`, flat SHA-256 filenames, regular-file checks and `O_NOFOLLOW` contain
paths. There is no delete/reset API. One scratch path per key bounds crash leftovers.
`Config.Wait` is the caller's context-aware contention backoff; no backend goroutine
outlives its operation. OS filesystem calls remain uninterruptible: the service
driver owns them through completion; remote hung mounts require process containment.

## Reproduction

From the clean combined checkout:

```sh
GOENV=off GOWORK=off GOFLAGS=-mod=readonly GOTOOLCHAIN=go1.27.1 go test -race -count=1 -timeout=90s ./internal/registry/filesystem
python3 test/scenarios/registry-filesystem/negative-controls.py --evidence /tmp/fresh-fs-evidence
```

The same `contracttest.Run` serves S3 and filesystem. FS tests add corruption,
containment, cancellation, SIGKILL exactly after rename, read recovery, pipe-gated
cross-process lock contention/CAS, stable lock inode and winner restart. No sleep
establishes a race. Negative controls copy pinned source to temporary directories
and remove actual fsync, flock or read-recovery calls. Each must fail its specific
invariant; generic failure/compile errors do not count. An actual syscall-boundary
audit is independent of protocol callbacks. The runner saves source and mutant
hashes, revision, commands/results/logs and removes its owned temporary trees.

| Target | Qualification in this implementation run |
| --- | --- |
| macOS 26.3 / Darwin 25.3 arm64 local volume | Race/process suite and three omission controls passed before integration; final evidence must bind integrated revision and mount identity. |
| Linux arm64 / OrbStack 7.0.5 / container overlayfs | Race/process suite and all three omission controls passed at `8f8bc6b`; [pinned receipt and logs](../../test/scenarios/registry-filesystem/evidence/linux-8f8bc6b/receipt.json). |
| NFS | Unqualified: no declared two-machine mount available. |
| SMB | Unqualified: no declared two-machine mount available. |

The Linux proof uses the official Go 1.27.1 Bookworm image pinned by digest in
`test/scenarios/registry-filesystem/run-linux.sh`. It copies an exact-revision
checkout into the container and places all test data on its Linux overlayfs;
there are no macOS source/data bind mounts. Reproduce with Docker and a fresh path:

```sh
test/scenarios/registry-filesystem/run-linux.sh /tmp/fresh-linux-evidence 8f8bc6b
```

Omit the revision argument to test current HEAD. The runner has a 600-second
container budget, records platform/source/image hashes and exit codes, copies
logs out, then removes its owned container and disposable source checkout.

These checks qualify process-crash/locking behavior on the declared configuration,
not hardware power loss, server failover or a filesystem family. In particular,
ordinary macOS fsync is not a physical storage-cache power-cut experiment. Storage
must honor its declared fsync/rename guarantees; backup rollback and namespace
reset are outside the protocol.

## Cross-machine mount recipe (not yet executed)

Use two distinct noninteractive SSH targets with Python 3 and the same NFS export
or SMB share mounted on each; paths may differ. Separate local directories do not
qualify. Build this exact clean revision natively for each OS/architecture, transfer
the binary using existing authorized access, and record SHA-256:

```sh
GOENV=off GOWORK=off GOFLAGS=-mod=readonly GOTOOLCHAIN=go1.27.1 go test -race -c -o /tmp/xenon-fs-participant ./internal/registry/filesystem
shasum -a 256 /tmp/xenon-fs-participant
git rev-parse HEAD
```

Fill [the manifest](../../test/scenarios/registry-filesystem/mount-manifest.example.json)
with actual hosts, binaries/hashes, durably provisioned private parent directories,
source commit, protocol/mount options, client/kernel and server/export versions:

```sh
python3 test/scenarios/registry-filesystem/cross-machine.py /absolute/mount-manifest.json --evidence /absolute/fresh-evidence
```

The runner verifies remote binary hashes/platforms, creates a unique shared child
on A which B must observe, runs the shared contract suite, kills A at `renamed`,
recovers through B, forces B to contend while A holds its CAS lock, verifies winner
and conflict, and restarts readers on both machines. Boundaries use pipes. Explicit
budgets govern steps and child processes. Cleanup verifies a unique command marker
before killing a recorded remote PID and waits for owned SSH children. Missing
boundaries, assertions, unexpected errors, timeouts or cleanup failures fail the
run. Evidence retains the exact run directory: after confirming all participants
exited, remove only that directory through one mount and the explicitly installed
binaries. Server restart, reconnect, partition and power-loss schedules remain
separate qualifications; no remote run is claimed here.
