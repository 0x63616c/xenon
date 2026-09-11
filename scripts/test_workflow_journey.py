import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('workflow_journey', Path(__file__).with_name('workflow_journey.py'))
journey = importlib.util.module_from_spec(spec)
spec.loader.exec_module(journey)

class OverlapTest(unittest.TestCase):
    def test_real_intervals_separate_serial_parallel_and_touching_runs(self):
        self.assertEqual(journey.overlap([(0, 1), (1, 2), (2, 3), (3, 4)]), 1)
        self.assertEqual(journey.overlap([(0, 10), (1, 9), (2, 8), (3, 7)]), 4)
        self.assertEqual(journey.overlap([(0, 2), (1, 3), (3, 4), (4, 5)]), 2)

if __name__ == '__main__': unittest.main()
