# Failed runtime checkpoint

Source: `495819cec3c2a96877a5eecde30cc4d7eeebb68c`, clean at start. Reproduce with
`python3 scripts/agent-smoke.py` at that source using committed pins/configuration.

Passed third-node join and assigned-partition serving. After intentional SIGKILL of B, A exited with agent health deadline and process exit required. Cold recovery was not reached.

The unchanged machine receipt and terminal log excerpt/hash are retained.
Scoped teardown returned zero and no cleanup error was recorded. A failing
run is not sustained-fuzz or release acceptance evidence.
