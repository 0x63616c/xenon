# Real Omes fuzz soak

Run `python3 scripts/ministack-runtime.py --fuzz-soak` from a clean checkout.
The controller builds the pinned Go/native runtime, creates scoped MinIO and two
Temporal/two Xenon processes, registers the namespace/search attributes and a real
Nexus endpoint, then replays the exact saved corpus through the public frontend.
It does not run an embedded Omes server. Ordinary smoke behavior is unchanged.

`soak.json` requires at least one hour and two complete rounds of all twenty saved
inputs. Each Omes invocation retains its original 900-second bound and one attempt.
A ten-hour controller bound limits total execution; it is not a relaxed individual
operation deadline. A failed invocation stops the soak; input, command, output and
source/tool/binary hashes remain in `.local/evidence/`. Setup and teardown are
supervised and scoped to this controller's processes and Compose project.

This tests repeated saved inputs under naturally varying scheduling, not newly
generated action coverage or deterministic OS scheduling. Each Omes execution uses
its upstream execution identity, avoiding collisions despite a fixed task queue.
No faults are injected in this mode. Omes exit success is the workload oracle;
independent attempted-start census, semantic history checks, mixed workloads,
controlled fault schedules and the real-AWS run remain separate gates. A passing
soak never sets `full_acceptance` true. Incomplete/timeout runs are failures, not
shortened successful soaks. The saved corpus and full acceptance contract are not
modified by this runner.

Controller regression controls: `PYTHONPATH=scripts python3 -m unittest
scripts/test_fuzz_soak.py scripts/test_omes_workloads.py
scripts/test_ministack_runtime.py`. These check failure preservation, duration and
round bounds, source input validation and process supervision; they are not runtime
fuzz evidence.
