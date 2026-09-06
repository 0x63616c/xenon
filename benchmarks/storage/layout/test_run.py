import json
import os
from pathlib import Path
import tempfile
import unittest

import run

class SupervisorTests(unittest.TestCase):
    def test_nearest_rank_tail(self):
        self.assertEqual(run.quantiles(list(range(1,257))), {'count':256,'p50':128,'p95':244,'p99':254})

    def fake(self,root,body):
        executable=root/'child'
        executable.write_text('#!/usr/bin/env python3\n'+body)
        executable.chmod(0o755)
        return executable

    def test_resource_bound_kills_and_reaps_real_child(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            binary=self.fake(root,'import time\nmemory=bytearray(16*1024*1024)\ntime.sleep(30)\n')
            with self.assertRaisesRegex(RuntimeError,'RSS resource bound'):
                run.execute_case(binary,1,0,os.environ.copy(),{'case_timeout_seconds':10,'rss_sample_ms':10,'max_rss_mib':1},root)
            samples=json.loads((root/'case-0-samples.json').read_text())
            self.assertTrue(samples)
            with self.assertRaises(ProcessLookupError): os.kill(samples[-1]['pid'],0)

    def test_nonzero_child_cannot_report_case_pass(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            binary=self.fake(root,'import sys\nprint(\'{"event":"passed"}\',flush=True)\nsys.exit(7)\n')
            with self.assertRaisesRegex(RuntimeError,'case failed: exit=7'):
                run.execute_case(binary,1,0,os.environ.copy(),{'case_timeout_seconds':10,'rss_sample_ms':10,'max_rss_mib':256},root)
            self.assertIn('passed',(root/'case-0-writers-1.jsonl').read_text())

    def test_failure_event_preempts_blocked_cleanup(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            binary=self.fake(root,'import time\nprint(\'{"event":"failure","error":"primary invariant"}\',flush=True)\ntime.sleep(30)\n')
            with self.assertRaisesRegex(RuntimeError,'workload failure: primary invariant'):
                run.execute_case(binary,1,0,os.environ.copy(),{'case_timeout_seconds':2,'rss_sample_ms':10,'max_rss_mib':256},root)
            self.assertIn('primary invariant',(root/'case-0-writers-1.jsonl').read_text())
            samples=json.loads((root/'case-0-samples.json').read_text())
            with self.assertRaises(ProcessLookupError): os.kill(samples[-1]['pid'],0)

if __name__=='__main__': unittest.main()
