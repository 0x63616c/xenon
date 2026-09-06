import importlib.util
from pathlib import Path
import os
import sys
import tempfile
import time
import unittest

spec=importlib.util.spec_from_file_location('ministack_runtime',Path(__file__).with_name('ministack-runtime.py'))
runtime=importlib.util.module_from_spec(spec);spec.loader.exec_module(runtime)
class RuntimeSupervisionTests(unittest.TestCase):
    def test_topology_event_is_a_snapshot(self):
        fields={'members':{'a':{'incarnation':'old'}},'assignments':{'global':{'node':'a'}}}
        event=runtime.event_record('topology-published',1.23456,fields)
        fields['members']['a']['incarnation']='new'
        fields['assignments']['global']['node']='cold-b'
        self.assertEqual(event['members']['a']['incarnation'],'old')
        self.assertEqual(event['assignments']['global']['node'],'a')
        self.assertEqual(event['elapsed_seconds'],1.235)

    def test_early_exit_does_not_wait_until_readiness_timeout(self):
        with tempfile.TemporaryDirectory() as folder:
            process=runtime.Process([sys.executable,'-c','print("wrong readiness")'],folder,os.environ.copy(),Path(folder)/'log')
            try:
                with self.assertRaises(RuntimeError):process.line('READY',timeout=2)
            finally:process.stop()
    def test_readiness_queue_is_bounded_and_complete_log_retained(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'log'
            process=runtime.Process([sys.executable,'-c','for i in range(4096): print(i)'],folder,os.environ.copy(),path)
            try:
                process.process.wait(timeout=5);process.thread.join(timeout=5)
                self.assertFalse(process.thread.is_alive())
                self.assertEqual(process.lines.qsize(),1024)
            finally:process.stop()
            self.assertEqual(path.read_text().splitlines(),[str(i) for i in range(4096)])
    def test_stop_cleans_descendant_even_after_parent_exit(self):
        with tempfile.TemporaryDirectory() as folder:
            marker=Path(folder)/'survived'
            child="import pathlib,time; time.sleep(1); pathlib.Path("+repr(str(marker))+").write_text('leaked')"
            parent="import subprocess,sys; subprocess.Popen([sys.executable,'-c',"+repr(child)+"]); print('SPAWNED',flush=True)"
            process=runtime.Process([sys.executable,'-c',parent],folder,os.environ.copy(),Path(folder)/'log')
            process.line('SPAWNED');process.process.wait(timeout=2);process.stop();process.stop()
            time.sleep(1.2)
            self.assertFalse(marker.exists(),'descendant survived scoped shutdown')
if __name__=='__main__':unittest.main()
