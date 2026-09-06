# Local ten-minute profile: restart failure

Exact source: `3a83534f5b788a6042c8650f0f14e6fe70bb4b64`, clean at launch.
Run through `xenon test local-release-ten-minute` with the committed profile and explicit 10 ms WAL flush configuration. Raw receipts are copied unchanged.

Mixed40 and its independent history oracle passed (720 captured run histories). The saved 20-input corpus completed 43 commands during the sustained phase; node C joined and node B was killed and evicted while workflows continued. At the scheduled restart after 360 seconds, B exited with Temporal worker scanner startup failure: `failed reaching server: context deadline exceeded`. This is a failed profile, not a ten-minute acceptance pass. The later cold-restart and final recovery assertions were not reached.

CLI cleanup was verified. All 119 helper log hashes matched local artifacts when archived; only the primary restarted-node log is included here, while the complete original receipt retains all hashes. Other logs remain in the original local evidence directory. No claim of a root cause is made from the fatal message alone.
