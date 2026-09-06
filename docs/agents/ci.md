# GitHub Actions gates

The repository uses ordinary GitHub Actions jobs and the same committed Make/proof commands as local development. There is no separate CI test engine.

- `persistence-gates`: PR and main checks. Fast Python harness/corpus controls run separately. Native component proofs, Go race/vet, generated binding drift, S3 emulator crash/directory/owner/maintenance, and original-versus-corrected Omes worker controls retain their declared assertions.
- `Xenon site`: relevant PR/main changes run pinned build, formatting and browser checks. Only curated static files are eligible for separately guarded manual deployment; this work does not authorize publication.
- `Temporal runtime gates`: manual choice of a committed scenario; weekly smoke, mixed and corrected-fuzz jobs run serially with independent receipts. Other fault/movement profiles are available manually. Original fuzz remains a distinct selectable gate and its known failure is not reclassified by the corrected overlay.

Runtime jobs use Ubuntu24.04, Go1.27.1, Rust1.94.0, hash-verified Node24.19.0 (the scenario installer), and AWS CLI2.36.39 (the official Linuxx86_64 archive hash is declared in the workflow). The same scenario pins specify Docker images, native engine, Omes, SDK and UI. Linux browser system packages come from the Ubuntu runner through pinned Playwright's dependency installer; the hosted OS package snapshot is not a hermetic machine image. Runtime receipts record actual versions. No AWS credentials are supplied: all object storage is the scoped MinIO emulator.

GitHub-hosted jobs allow at most six hours. The runtime step sends TERM after19800seconds, reserving30minutes for capped setup/preflight and keeping time for the controller's failed receipt, scoped teardown and always-upload artifacts. The corrected corpus's own36000second maximum is unchanged: an attempt that cannot finish within the CI host budget fails, never becomes a shortened passing soak. The one-hour minimum, two complete rounds, all20inputs and per-input deadlines remain enforced by the committed runner. A longer provisioned runner would be needed to cover the entire allowed worst-case wall time.

No runtime workflow was dispatched as part of authoring. At the latest checked integration runs, GitHub rejected jobs before execution because account payments failed or the spending limit needs increasing. The workflow changes cannot fix that account restriction. Do not alter billing, raise limits, bypass required checks, or call these configured jobs verified runs. Once access is restored, run the PR checks first, then selected runtime jobs; preserve any Linux-specific failure and fix it before claiming CI success.

Evidence uploads run with `always()` and retain hidden `.local/evidence` receipts/logs/screenshots plus setup logs. An absent expected artifact is an error in standalone proof/runtime jobs. Normal job logs still record failures that happen before checkout or artifact creation.
