import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('proof', Path(__file__).with_name('cli-generated-search-proof.py'))
proof = importlib.util.module_from_spec(spec)
spec.loader.exec_module(proof)


class ProofControls(unittest.TestCase):
    def test_native_receipt_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            library = Path(directory) / 'library'
            library.write_bytes(b'pinned')
            pins = dict(source_commit='rev', go_module='binding', go_version='v1')
            receipt = dict(source_clean=True, source_commit='rev', binding_module='binding',
                           binding_version='v1', binding_sum='sum', shared_library_sha256=proof.digest(library))
            proof.validate_native(pins, receipt, library, 'binding v1 sum')
            for key, value in [('source_clean', False), ('source_commit', 'wrong'),
                               ('binding_version', 'v2'), ('binding_sum', 'wrong'),
                               ('shared_library_sha256', 'wrong')]:
                with self.subTest(key=key), self.assertRaises(ValueError):
                    proof.validate_native(pins, receipt | {key: value}, library, 'binding v1 sum')
            library.write_bytes(b'changed')
            with self.assertRaises(ValueError):
                proof.validate_native(pins, receipt, library, 'binding v1 sum')

    def test_result_cannot_hide_incomplete_or_wrong_mode(self):
        result = dict(schema=1, qualification='component', mode='generated',
                      result=dict(completed=4, stop_reason='completed'))
        proof.check_output(result, 'generated', 4)
        for key, value in [('mode', 'finite'), ('qualification', 'development'), ('schema', 2)]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                proof.check_output(result | {key: value}, 'generated', 4)
        for value in [dict(completed=3, stop_reason='completed'), dict(completed=4, stop_reason='budget')]:
            with self.assertRaises(ValueError):
                proof.check_output(result | {'result': value}, 'generated', 4)


if __name__ == '__main__':
    unittest.main()
