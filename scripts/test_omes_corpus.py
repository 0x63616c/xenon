#!/usr/bin/env python3
"""Regression: optimized Python must still reject corrupted committed inputs."""
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]

class CorpusVerification(unittest.TestCase):
    def test_optimized_integrity_and_replay(self):
        with tempfile.TemporaryDirectory(prefix='xenon-corpus-test-') as tmp:
            root = Path(tmp)
            shutil.copytree(ROOT / 'proof/omes-corpus', root / 'proof/omes-corpus')
            (root / 'scripts').mkdir()
            shutil.copy2(ROOT / 'scripts/omes-corpus.py', root / 'scripts/omes-corpus.py')
            def git(*args):
                subprocess.run(['git', *args], cwd=root, check=True, capture_output=True, timeout=30)
            def commit():
                git('add', '.')
                git('-c', 'user.name=Corpus test', '-c', 'user.email=corpus@example.invalid', 'commit', '-qm', 'fixture')
            def verify():
                return subprocess.run([sys.executable, '-O', str(root / 'scripts/omes-corpus.py'), 'verify'], cwd=root, capture_output=True, text=True, timeout=30)
            git('init', '-q')
            commit()
            self.assertEqual(verify().returncode, 0)
            path = root / 'proof/omes-corpus/inputs/2026090501.proto'
            original = path.read_bytes()
            path.write_bytes(bytes([original[0] ^ 1]) + original[1:])
            commit()
            result = verify()
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('Input hash mismatch', result.stderr)
            path.write_bytes(original)
            replay_path = root / 'proof/omes-corpus/replay.json'
            replay = json.loads(replay_path.read_text())
            replay['commands'][0][-1] = replay['commands'][1][-1]
            replay_path.write_text(json.dumps(replay))
            commit()
            result = verify()
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('Replay profile', result.stderr)

if __name__ == '__main__':
    unittest.main()
