import importlib.util
from pathlib import Path
import unittest
import tempfile
import json
import hashlib

spec = importlib.util.spec_from_file_location('workflow_journey', Path(__file__).with_name('workflow_journey.py'))
journey = importlib.util.module_from_spec(spec)
spec.loader.exec_module(journey)

class OverlapTest(unittest.TestCase):
    def test_real_intervals_separate_serial_parallel_and_touching_runs(self):
        self.assertEqual(journey.overlap([(0, 1), (1, 2), (2, 3), (3, 4)]), 1)
        self.assertEqual(journey.overlap([(0, 10), (1, 9), (2, 8), (3, 7)]), 4)
        self.assertEqual(journey.overlap([(0, 2), (1, 3), (3, 4), (4, 5)]), 2)
        # Alternating runs from two Continue-As-New chains never overlap.
        # Merging each chain to min(start)/max(end) would incorrectly report two.
        chain_a, chain_b = [(0, 1), (4, 5)], [(2, 3), (6, 7)]
        self.assertEqual(journey.overlap(chain_a + chain_b), 1)
        self.assertEqual(journey.overlap([(0, 5), (2, 7)]), 2)

    def test_history_counts_does_not_fill_continue_as_new_gaps(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for case in range(3):
                for member in range(4):
                    directory = root / ('case-' + str(case)) / ('member-' + str(member)) / 'histories'
                    directory.mkdir(parents=True)
                    runs = []
                    intervals = [(0, 1), (10, 11)] if member == 0 else [(member * 2, member * 2 + 1)]
                    for index, (start, end) in enumerate(intervals):
                        attrs = {'workflowType': {'name': 'kitchenSink'}}
                        if index: attrs['continuedExecutionRunId'] = 'previous'
                        events = [{'eventTime': f'2026-01-01T00:00:{start:02}Z', 'workflowExecutionStartedEventAttributes': attrs},
                                  {'eventTime': f'2026-01-01T00:00:{end:02}Z'}]
                        raw = json.dumps({'events': events}).encode()
                        name = f'history-{index}.json'
                        (directory / name).write_bytes(raw)
                        runs.append({'workflow_id': f'root-{member}', 'history_file': name, 'history_sha256': hashlib.sha256(raw).hexdigest()})
                    (directory / 'result.json').write_text(json.dumps({'runs': runs}))
            actual = journey.history_counts(root)
            self.assertEqual(actual['case_execution_interval_peak'], {'case-0': 1, 'case-1': 1, 'case-2': 1})
            self.assertEqual(actual['totals']['initial_roots'], 12)
            self.assertEqual(actual['totals']['continued_runs'], 3)

if __name__ == '__main__': unittest.main()
