# Pinned UI history regression

The runtime probe checks the unchanged Temporal UI 2.53.3. Its event label includes
an SVG `<title>Workflow</title>`: DOM text is `Workflow Workflow Execution Completed`,
although visible text is `Workflow Execution Completed`. An exact Playwright text
match falsely rejected the rendered event. The probe now anchors the full terminal
event name within an event row, and waits for the actual detail route before taking
its detail screenshot. Failure DOM, screenshot and browser/API diagnostics remain
in the runtime evidence directory.

`completed-history.json` is the ten-event completed second run captured by the real
SDK verifier in failed smoke `20260905T214920Z-xenon-ministack-6c7a9a78ac7d` at
commit `cc23248`. That smoke remains failed; the fixture does not prove cold recovery.
The isolated regression imports these committed bytes into the unchanged pinned UI.
Only initial namespace/cluster/system metadata is stubbed. It asserts that the old
exact selector fails and that the corrected terminal event selector is visible.

After the declared ministack Node/Playwright setup, rerun from the repository root:

```sh
docker run -d --name xenon-ui-history-regression -p 127.0.0.1:18081:8080 -e TEMPORAL_ADDRESS=127.0.0.1:1 temporalio/ui:2.53.3@sha256:eef301146e60fad34b47adaecfae4149016e34b2d44ba94fca5fd8e5441f182a
cp test/scenarios/ministack/ui/history-import-regression.mjs .local/ministack-ui/
PLAYWRIGHT_BROWSERS_PATH="$PWD/.local/playwright-browsers" .local/node-v24.19.0-darwin-arm64/bin/node .local/ministack-ui/history-import-regression.mjs http://127.0.0.1:18081 test/scenarios/ministack/ui/completed-history.json .local/evidence/ui-history-regression
docker rm -f xenon-ui-history-regression
```

Choose the corresponding pinned Node platform directory on other hosts. This is a
UI selector control, not a replacement for the clean `scripts/ministack-runtime.py`
proof, which still requires live list/filter/detail/history and cold recovery.
