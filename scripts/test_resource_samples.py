import importlib.util
import json
import select
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

spec = importlib.util.spec_from_file_location('resource_samples', Path(__file__).with_name('resource_samples.py'))
samples = importlib.util.module_from_spec(spec)
spec.loader.exec_module(samples)


class ResourceSamples(unittest.TestCase):
    def test_platform_cpu_formats_and_reused_root(self):
        self.assertEqual(samples.cpu_seconds('01:02.25'), 62.25)
        self.assertEqual(samples.cpu_seconds('1-02:03:04'), 93784)
        table = {1: {'pid': 1, 'ppid': 0, 'started': 'new'},
                 2: {'pid': 2, 'ppid': 1, 'started': 'child'}}
        self.assertEqual(samples.owned_rows(table, {'old': {'pid': 1, 'started': 'old'}}), [])
        self.assertEqual([x['pid'] for x in samples.owned_rows(table, {'live': {'pid': 1, 'started': 'new'}})], [1, 2])

    def test_actual_memory_cpu_and_bounded_completion(self):
        with tempfile.TemporaryDirectory() as directory:
            process = subprocess.Popen([sys.executable, '-u', '-c',
                'import time; x=bytearray(32*1024*1024); print("ready"); '
                'end=time.monotonic()+2;\nwhile time.monotonic()<end: sum(range(10000))\ntime.sleep(5)'],
                stdout=subprocess.PIPE, text=True)
            recorder = None
            try:
                self.assertTrue(select.select([process.stdout], [], [], 5)[0], 'child readiness deadline')
                self.assertEqual(process.stdout.readline().strip(), 'ready')
                path = Path(directory) / 'samples.jsonl'
                recorder = samples.ProcessSampler(path, interval=0.05, max_samples=200)
                recorder.register('controlled-child', process.pid)
                end = time.monotonic() + 4
                observed = []
                while time.monotonic() < end:
                    observed = [r for line in path.read_text().rsplit('\n', 1)[0].splitlines() for r in json.loads(line).get('processes', [])]
                    if observed and max(r['cpu_seconds'] for r in observed) >= 0.1:
                        break
                    time.sleep(0.05)
                self.assertTrue(observed)
                self.assertGreaterEqual(max(r['rss_bytes'] for r in observed), 32*1024*1024)
                self.assertGreaterEqual(max(r['cpu_seconds'] for r in observed), 0.1)
                self.assertTrue(recorder.stop()['complete'])
                limited = samples.ProcessSampler(Path(directory) / 'bounded.jsonl', interval=0.05, max_samples=1)
                limited.thread.join(timeout=3)
                self.assertFalse(limited.stop()['complete'])
            finally:
                if recorder:
                    recorder.stop()
                process.kill()
                process.wait(timeout=5)
                process.stdout.close()


if __name__ == '__main__':
    unittest.main()
