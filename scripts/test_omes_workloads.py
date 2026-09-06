import importlib.util
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).parent))
import omes_workloads as runner

class OmesWorkloadControls(unittest.TestCase):
 def test_effective_sdk_replacements(self):
  normal='\tdep\tgo.temporal.io/sdk\tv1.48.0\t'+runner.SDK_SUM+'\n'
  self.assertFalse(runner.effective_sdk(normal)['replacement'])
  original='\tdep\tgo.temporal.io/sdk\tv1.48.0\n'
  replacement='\t=>\tgo.temporal.io/sdk\tv1.48.0\t'+runner.SDK_SUM+'\n'
  self.assertTrue(runner.effective_sdk(original+replacement)['replacement'])
  for invalid in [normal.replace('v1.48.0','v1.47.0'),original+'\t=>\t../sdk\t(devel)\n', original+replacement.replace(runner.SDK_SUM,'h1:wrong'), original+replacement.replace('go.temporal.io/sdk','example.org/sdk')]:
   with self.assertRaises(ValueError):runner.effective_sdk(invalid)
  self.assertTrue(runner.effective_sdk('\tdep\texample.org/other\tv1.0\n\t=>\t../other\t(devel)\n'+original+replacement)['replacement'])
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
