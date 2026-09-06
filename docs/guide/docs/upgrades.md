# Upgrading Temporal

A Temporal version change is a compatibility project. Compilation alone cannot establish that old histories, outcomes and active workflows remain readable or continue correctly.

## Inventory the source change

Choose explicit old and proposed new revisions. The committed impact tool reads a clean local Temporal Git checkout containing both. Replace the uppercase placeholders below with those selected revisions and a real source path:

```sh
python3 scripts/temporal_upgrade.py \
  --source /absolute/path/to/temporal \
  --old OLD_COMMIT \
  --new NEW_COMMIT \
  --output .local/evidence/temporal-upgrade-impact
```

The output directory must be new and outside the Temporal source checkout. The tool resolves immutable commits, checks cleanliness, saves a binary-capable diff and maps changed paths to Xenon review boundaries. It does not fetch a version, change dependencies or declare compatibility.

Review every unmapped path too. A path inventory cannot detect every semantic change, including behavior changes behind an unchanged interface signature. The project-local skill is `.agents/skills/xenon-temporal-upgrade/SKILL.md`; the detailed runbook is `docs/temporal-upgrades.md`.

## Review the boundaries

| Upstream change | Xenon review area |
| --- | --- |
| Persistence interfaces and manager behavior | Factories, adapters, transaction guards, callbacks and typed errors. |
| Persistence protobufs or encodings | Opaque bytes, unknown fields, oneofs, stored outcomes and decoding. |
| SQL persistence behavior | Conditions, ordering, conflicts and version semantics. SQL is an oracle, not Xenon storage. |
| Visibility and search attributes | Nulls, raw values, numeric/time comparisons, aliases and pagination. |
| Server configuration | Both factories, launcher, multiple Temporal processes, SDK/UI/Omes behavior. |

Keep adaptation concentrated in the existing seams. Do not add placeholder successes for new methods just to satisfy an interface. A durable schema change needs an explicit compatibility or migration policy.

## Prove two different outcomes

**Fresh install:** run the target build against a new isolated S3 prefix. Exercise the required stores, real workflows, visibility and recovery scenarios.

**Existing-state upgrade:** create a committed fixture with the old build, including active/completed workflows, histories, queues, cursors, metadata, visibility and operation journals. Start the target against those same S3 objects with disposable local disks. Verify decoding, continued workflows, replay and preservation of acknowledged writes.

An empty prefix is not an upgrade test. A fresh-install pass cannot promote existing-state compatibility. Rollback and mixed-version operation require their own explicit policy and evidence.

The repository includes an automated old-build/target-build workflow component runner. At Xenon commit `5678ef8dfa687beb2c22b41771d614c2bb2257e4`, [hosted run 34017104608](https://github.com/0x63616c/xenon/actions/runs/34017104608) passed the component for Temporal Server `v1.31.1` to `v1.31.2`. It preserved completed history hashes and continued the original active RunID after a cold target start against the same MinIO objects.

That receipt covers one existing-state workflow component for that exact version pair. It does not prove fresh install, full fixture coverage, mixed-version operation, rollback, unified-agent upgrade or real S3. See the [full upgrade runbook on GitHub](https://github.com/0x63616c/xenon/blob/main/docs/temporal-upgrades.md) for the commands, retained artifact and remaining gates.
