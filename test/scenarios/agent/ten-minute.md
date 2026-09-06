# Ten-minute identical-agent profile

This is an opt-in local S3-emulator profile. It has not passed a live runtime run
merely because its configuration and supervisor controls pass. Issue #94 remains
open until actual reviewed execution and the remaining oracles are accepted.

Use a clean checkout and the pinned tools/images from the agent and ministack
fixtures. Pull the declared Compose images beforehand, then run:

```sh
python3 scripts/agent-smoke.py --profile ten-minute
```

The existing `--profile smoke` default and historical ministack/full/one-hour soak
profiles remain separate. The helper reuses the same identical Go agents, shared
storage/Temporal ingress, exact format2 layout, S3 emulator, UI, SDK worker,
changed-authority serving RPC and cold-recovery code as agent smoke. It refuses
occupied ports before building or starting resources; do not overlap smoke runs.
A dedicated `xenon` CLI journey still needs integration; this helper alone does
not qualify the complete CLI user journey.

`ten-minute.json` declares independent budgets: setup 2400s, mixed40 900s,
600s fuzz admission followed by at most 300s drain, each verification phase 900s,
cold recovery 480s, and cleanup 120s. The historical mixed40 source-derived
history/Nexus oracle executes before the ten-minute window. It does not consume
or substitute for any of those 600 seconds.

The fuzz window starts only after actual SDK/Nexus setup and mixed verification.
It repeatedly executes the 20 exact corrected-v1 binary protobuf inputs through
the explicitly pinned Omes signal compatibility overlay. It admits one Omes
command at a time until 600 monotonic seconds elapse; it never waits idle after a
short corpus to claim ten minutes. No new work launches after the window. The
in-flight command must drain within its separate bound. At least one complete
corpus round and all declared faults must complete; the 1000-command ceiling,
missing triggers, incomplete corpus, deadline, cancellation or any error fail.
Saved-input repetition varies real scheduling, not generated action coverage.

Faults are not-before times relative to fuzz admission: add C at 60s (complete by
180s), SIGKILL B at 240s (eviction observed by 330s), restart the same B at 360s
(healthy by 480s). Each fault requires an active Omes process and a running fuzz
workflow observed through Temporal. C must serve a real RPC against an unchanged
ready reservation before/after that RPC. No fixed partition ownership is assumed.

Actual Nexus echo operations verify result nonces through both initial Temporal
instances and all three after faults. Real fuzz workers execute Nexus operations;
the saved histories must contain scheduled and completed Nexus events. Existing
Omes checks validate workload completion, the history oracle checks the complete
visible run-chain set and terminal history, and cold recovery compares identical
SDK acknowledged-result histories and all captured fuzz histories/inventory.
This is not an independent census of every attempted start or every acknowledged
persistence mutation; that broader oracle remains an explicit gap. The UI HTML
and public HTTP namespace/workflow surfaces are exercised; the full browser
interaction matrix is also separate.

Before backend launch, receipts save exact expanded argv, base64 input bytes,
input hashes, fault schedule and tracked source hashes. Omes overlay/worker,
Go/native artifacts and image/tool pins are retained separately. Launch records
are persisted before process creation. Command logs stream to disk; background
exit and pinned terminal Omes iteration failures interrupt blocked commands.
Logs have a 256 MiB aggregate ceiling and command outputs a 16 MiB ceiling.
The first failure is saved before bounded diagnostics/cleanup, which signals all
owned process groups, reaps them and removes only the run's Compose project.
Final integrity checks preserve input/binary/native drift as failure even when
an earlier primary already exists. Cleanup errors never replace that primary.

Controls (no storage resources):

```sh
python3 -m unittest discover -s scripts -p 'test_agent*.py'
```

They cover exact binary inputs, full-duration admission, failure/cancellation,
missed fault/drain deadlines, and actual blocked children detecting an independent
or Omes terminal failure before cleanup finishes. A passing control is not the
actual ten-minute Omes/Nexus/churn gate and is not AWS qualification.
