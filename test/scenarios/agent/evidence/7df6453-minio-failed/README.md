# Failed runtime checkpoint

Source: `7df645339db12eae34b3e4fac974843d334d5d6d`, clean at start. Reproduce with
`python3 scripts/agent-smoke.py` at that source using committed pins/configuration.

Passed third-node join/serving, B crash/eviction/restart, Omes20 completion/UI and SDK results. During full cold restart A exited with storage readiness deadline. Cold history comparison did not complete.

The unchanged machine receipt and terminal log excerpt/hash are retained.
Scoped teardown returned zero and no cleanup error was recorded. A failing
run is not sustained-fuzz or release acceptance evidence.
