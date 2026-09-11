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


class AdmissionTest(unittest.TestCase):
    def receipt(self, size):
        roots = [dict(queue=f'omes-xenon-generated-{42:016x}-{case:016x}-{member:02d}',
                      workflow_id=f'w-{case}-{member}', run_id=f'r-{case}-{member}', status='Running')
                 for case in range(3) for member in range(4)]
        return dict(schema=1, concurrency=size, windows=[dict(released=True, roots=roots[i:i+size]) for i in range(0, 12, size)]), roots

    def check(self, receipt, roots, size):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'admission.json'
            path.write_text(json.dumps(receipt))
            return journey.admission_counts(path, size, roots)

    def test_four_and_one(self):
        for size in (1, 4):
            receipt, roots = self.receipt(size)
            self.assertEqual(self.check(receipt, roots, size)['observed_running_peak'], size)

    def test_negative_controls(self):
        for mutation in ('missing', 'fifth', 'duplicate', 'unreleased', 'not_running', 'wrong_history'):
            with self.subTest(mutation=mutation):
                receipt, roots = self.receipt(4)
                histories = [dict(r) for r in roots]
                if mutation == 'missing': receipt['windows'].pop()
                if mutation == 'fifth': receipt['windows'][0]['roots'].append(dict(roots[-1]))
                if mutation == 'duplicate': roots[1]['workflow_id'] = roots[0]['workflow_id']
                if mutation == 'unreleased': receipt['windows'][0]['released'] = False
                if mutation == 'not_running': roots[0]['status'] = 'Completed'
                if mutation == 'wrong_history': histories[0]['run_id'] = 'unrelated'
                with self.assertRaises(ValueError): self.check(receipt, histories, 4)

if __name__ == '__main__': unittest.main()
