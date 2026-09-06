import importlib.util
from pathlib import Path
import shutil
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('acceptance', ROOT/'scripts/validate_acceptance.py')
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class AcceptanceProfile(unittest.TestCase):
 def test_required_budgets_cannot_silently_change(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp)
   shutil.copytree(ROOT/'proof/acceptance',root/'proof/acceptance')
   shutil.copytree(ROOT/'proof/omes-corpus',root/'proof/omes-corpus')
   module.validate(root)
   path=root/'proof/acceptance/full-profile.json'
   original=path.read_text()
   for before,after in [('"100"','"20"'),('zero_acknowledged_operation_loss','ignored'),('"NOT_EXECUTED"','"PASS"')]:
    path.write_text(original.replace(before,after))
    with self.assertRaises(ValueError):module.validate(root)
   path.write_text(original)
   replay=root/'proof/omes-corpus/replay.json'
   replay.write_text('{}')
   with self.assertRaises(ValueError):module.validate(root)

if __name__=='__main__':unittest.main()
