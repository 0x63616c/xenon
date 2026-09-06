import base64
import json
from pathlib import Path
import subprocess
import tempfile
import sys
import time
import unittest

from agent_ten_minute import expand, exercise

ROOT = Path(__file__).resolve().parents[1]


class Clock:
    def __init__(self): self.value = 0
    def now(self): return self.value
    def sleep(self, duration): self.value += duration


class TenMinuteTests(unittest.TestCase):
    def test_exact_corpus_and_declared_profile(self):
        expanded = expand(ROOT)
        self.assertEqual(expanded['profile']['workload_seconds'], 600)
        self.assertEqual(len(expanded['fuzz_commands']), 20)
        for path, saved in expanded['inputs'].items():
            self.assertEqual(base64.b64decode(saved['base64']), (ROOT/path).read_bytes())
        self.assertEqual(expanded['mixed_command'], json.loads((ROOT/'proof/acceptance/full-profile.json').read_text())['workloads']['mixed'])

    def drive(self, *, fail=False, blocked=False, cancel=False, late_fault=False):
        clock = Clock()
        profile = expand(ROOT)['profile']
        launches = []; events = []; faults = []; finished = []
        def launch(index, command):
            launches.append(index)
            end = clock.now() + 2
            class Child:
                def poll(self):
                    if blocked: return None
                    return (1 if fail else 0) if clock.now() >= end else None
            return Child()
        def fault(action):
            faults.append(action)
            if late_fault: clock.sleep(200)
        def health():
            if cancel and clock.now() >= 1: raise KeyboardInterrupt('canceled')
        result = exercise(profile, list(range(20)), launch, health, fault, events.append,
                          finish=finished.append, now=clock.now, sleep=clock.sleep)
        return result, launches, events, faults, finished

    def test_short_inputs_repeat_for_real_full_window_then_stop(self):
        result, launches, events, faults, finished = self.drive()
        self.assertGreaterEqual(result['elapsed_seconds'], 600)
        self.assertLess(result['elapsed_seconds'], 603)
        self.assertGreater(result['completed_commands'], 20)
        self.assertEqual(len(finished), len(launches))
        self.assertEqual(faults, ['join-c','kill-b','restart-b'])

    def test_failure_cancellation_and_late_fault_never_pass(self):
        for kwargs, error in [({'fail':True}, RuntimeError), ({'cancel':True}, KeyboardInterrupt),
                              ({'late_fault':True}, TimeoutError), ({'blocked':True}, TimeoutError)]:
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(error): self.drive(**kwargs)

    def test_actual_blocked_child_notices_asynchronous_failure(self):
        profile = expand(ROOT)['profile']
        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
        started = time.monotonic()
        calls = []
        def health():
            if time.monotonic() - started > .1: raise RuntimeError('independent failure')
        try:
            with self.assertRaisesRegex(RuntimeError, 'independent failure'):
                exercise(profile, [1], lambda *_:calls.append(1) or child, health,
                         lambda _:self.fail('fault launched after failure'), lambda _:None)
            self.assertLess(time.monotonic()-started, 2)
            self.assertEqual(calls, [1])
        finally:
            child.kill();child.wait(timeout=5)

    def test_terminal_omes_failure_detected_while_worker_cleanup_blocks(self):
        from test_agent_control import smoke
        with tempfile.TemporaryDirectory() as directory:
            log=Path(directory)/'omes.log'
            with log.open('wb') as stream:
                child=subprocess.Popen([sys.executable,'-c',
                    'import time; print("iteration 3 failed: invariant",flush=True); time.sleep(30)'],
                    stdout=stream,stderr=subprocess.STDOUT,start_new_session=True)
                try:
                    monitor=smoke.OmesLog(log)
                    end=time.monotonic()+2
                    while log.stat().st_size==0 and time.monotonic()<end:time.sleep(.01)
                    with self.assertRaisesRegex(smoke.ScenarioInvariant,'terminal Omes'):
                        monitor.check()
                    self.assertIsNone(child.poll())
                finally:
                    smoke.kill_and_collect(child)
                self.assertIsNotNone(child.returncode)


if __name__ == '__main__': unittest.main()
