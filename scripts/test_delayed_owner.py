import json
from pathlib import Path
import tempfile
import unittest
import delayed_owner

class DelayedOwnerControls(unittest.TestCase):
    def test_receipt_binding(self):
        with tempfile.TemporaryDirectory() as name:
            path=Path(name)/'receipt.json'
            self.assertFalse(delayed_owner.read_receipt(path,'session','boot'))
            receipt={'session':'session','record':{'partition':'matching','node':'c','incarnation':'boot'},'events':['paused-before-build']}
            path.write_text(json.dumps(receipt))
            self.assertEqual(delayed_owner.read_receipt(path,'session','boot'),receipt)
            with self.assertRaises(RuntimeError):delayed_owner.read_receipt(path,'session','other')
            receipt['events']=['FAILED-no-build'];path.write_text(json.dumps(receipt))
            with self.assertRaises(RuntimeError):delayed_owner.read_receipt(path,'session','boot')
    def test_release_exact(self):
        with tempfile.TemporaryDirectory() as name:
            path=Path(name)/'release.json'
            receipt={'session':'session','record':{'generation':19,'transition':'transition'},'events':['paused-before-build']}
            delayed_owner.release(path,receipt)
            self.assertEqual(json.loads(path.read_text()),{'session':receipt['session'],'record':receipt['record']})
            self.assertFalse(path.with_suffix('.tmp').exists())
