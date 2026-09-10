import copy
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

    def test_actual_binary_metadata_must_match_pins(self):
        pins = dict(source_commit='native-rev', go_module='binding',
                    go_version='v1', go_toolchain='go1.27.1')
        temporal = dict(module='temporal', version='v2')
        sums = {('binding', 'v1'): 'binding-sum', ('temporal', 'v2'): 'temporal-sum'}
        info = dict(revision='revision', modified='false', go='go1.27.1',
                    slatedb_go=dict(path='binding', version='v1', sum='binding-sum'),
                    temporal=dict(path='temporal', version='v2', sum='temporal-sum'),
                    slatedb_native=dict(source_commit='native-rev', artifact_sha256='native-sha',
                                        identity_source='build-attestation'))
        proof.check_cli_identity(info, 'revision', pins, temporal, 'native-sha', sums)
        for field, value in [('revision', 'other'), ('modified', 'true'), ('go', 'other')]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                proof.check_cli_identity(info | {field: value}, 'revision', pins, temporal, 'native-sha', sums)
        for dependency in ['slatedb_go', 'temporal']:
            for field, value in [('path', 'other'), ('version', 'other'), ('sum', 'other'),
                                 ('replacement', {'path': '/local/replacement'})]:
                changed = copy.deepcopy(info)
                changed[dependency][field] = value
                with self.subTest(dependency=dependency, field=field), self.assertRaises(ValueError):
                    proof.check_cli_identity(changed, 'revision', pins, temporal, 'native-sha', sums)
        for field in ['source_commit', 'artifact_sha256', 'identity_source']:
            changed = copy.deepcopy(info)
            changed['slatedb_native'][field] = 'wrong'
            with self.subTest(native=field), self.assertRaises(ValueError):
                proof.check_cli_identity(changed, 'revision', pins, temporal, 'native-sha', sums)
        for field in ['slatedb_go', 'temporal', 'slatedb_native']:
            changed = copy.deepcopy(info)
            del changed[field]
            with self.subTest(missing=field), self.assertRaises(ValueError):
                proof.check_cli_identity(changed, 'revision', pins, temporal, 'native-sha', sums)
        with self.assertRaises(ValueError):
            proof.check_cli_identity(info, 'revision', pins, temporal, 'native-sha', {})

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
