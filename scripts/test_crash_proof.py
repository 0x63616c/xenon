import importlib.util
from pathlib import Path
import subprocess
import sys
import time
import unittest

spec = importlib.util.spec_from_file_location("crash_proof", Path(__file__).with_name("crash-proof.py"))
crash = importlib.util.module_from_spec(spec)
spec.loader.exec_module(crash)

class BarrierTests(unittest.TestCase):
    def test_partial_line_cannot_block_deadline(self):
        process = subprocess.Popen([sys.executable, "-c", "import sys,time;sys.stdout.write('{');sys.stdout.flush();time.sleep(10)"], stdout=subprocess.PIPE, bufsize=0)
        started = time.monotonic()
        try:
            with self.assertRaisesRegex(RuntimeError, "never observed"):
                crash.observe_barrier(process, 0.2, 3)
            self.assertLess(time.monotonic()-started, 2)
        finally:
            process.kill(); process.wait(); process.stdout.close()

    def test_multiple_buffered_events_are_observed(self):
        source = 'print(\'{"event":"ack","sequence":1}\\n{"event":"fault-ready","sequence":3}\',flush=True)'
        process = subprocess.Popen([sys.executable, "-c", source], stdout=subprocess.PIPE, bufsize=0)
        try:
            a, events = crash.observe_barrier(process, 2, 3)
            self.assertEqual(a,[1]); self.assertEqual(len(events),2)
        finally:
            process.wait(); process.stdout.close()

if __name__ == '__main__': unittest.main()
