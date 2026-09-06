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
    def test_dispatch_requires_post_ready_increment(self):
        def snapshot(generation,count):
            return {'incarnation':'boot','partitions':{'matching':{'owner_record':{'node':'b','incarnation':'boot','state':'ready','generation':generation},'count':count}}}
        successful=lambda value,partition:value['partitions'][partition]['count']
        def exercise(values):
            samples=iter(values)
            def wait(check,timeout):
                for _ in values:
                    result=check()
                    if result:return result
                raise TimeoutError('no post-generation work')
            return delayed_owner.dispatch_after_ready(lambda node:next(samples),successful,wait,'b','boot',10,120)
        # This increase belongs to the pre-fence generation, not recovered work.
        with self.assertRaises(TimeoutError):exercise([snapshot(10,5),snapshot(10,20),snapshot(11,20),snapshot(11,20)])
        self.assertEqual(exercise([snapshot(11,20),snapshot(11,21)])['partitions']['matching']['count'],21)
        # A generation change resets the observation, even when its count rose.
        with self.assertRaises(TimeoutError):exercise([snapshot(11,20),snapshot(12,21),snapshot(12,21)])
        wrong=snapshot(11,20);wrong['partitions']['matching']['owner_record']['incarnation']='other'
        with self.assertRaises(RuntimeError):exercise([wrong])
