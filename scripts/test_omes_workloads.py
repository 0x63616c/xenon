import importlib.util
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).parent))
import omes_workloads as runner

class OmesWorkloadControls(unittest.TestCase):
 def test_exit_and_timeout(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp)
   ok=runner.command([sys.executable,'-c','print("actual control")'],root/'ok.log',2)
   self.assertEqual(ok['exit_code'],0)
   failed=runner.command([sys.executable,'-c','raise SystemExit(7)'],root/'failed.log',2)
   self.assertEqual(failed['exit_code'],7)
   hung=runner.command([sys.executable,'-c','import time;time.sleep(10)'],root/'hung.log',0.05)
   self.assertTrue(hung['timed_out']);self.assertNotEqual(hung['exit_code'],0)
 def test_changed_corpus(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);shutil.copytree(runner.ROOT/'proof/omes-corpus',root/'proof/omes-corpus')
   self.assertEqual(len(runner.corpus(root)),20)
   p=root/'proof/omes-corpus/inputs/2026090501.proto';raw=p.read_bytes();p.write_bytes(bytes([raw[0]^1])+raw[1:])
   with self.assertRaises(ValueError):runner.corpus(root)
if __name__=='__main__':unittest.main()
