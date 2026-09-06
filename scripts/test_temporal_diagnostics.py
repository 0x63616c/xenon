import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import temporal_diagnostics as diagnostic

class Diagnostics(unittest.TestCase):
    def test_partial_history_and_failed_describe_are_preserved(self):
        responses=[io.BytesIO(json.dumps({'executions':[{'execution':{'workflowId':'a/b','runId':'run'}}],'nextPageToken':'more'}).encode()),OSError('unavailable'),io.BytesIO(b'{"history":{"events":[]},"nextPageToken":"more"}')]
        with tempfile.TemporaryDirectory() as root,patch.object(diagnostic.urllib.request,'urlopen',side_effect=responses) as opened:
            report=diagnostic.capture('http://127.0.0.1:17243','test',Path(root)/'evidence',diagnostic.LIMITS)
            self.assertTrue(report['truncated']);self.assertIn('unavailable',report['requests'][1]['error'])
            self.assertEqual(len(report['requests']),3)
            self.assertIn('/a%2Fb/history?runId=run',opened.call_args_list[-1].args[0])
            self.assertTrue((Path(root)/'evidence/00-history.json').exists())
            self.assertEqual(json.loads((Path(root)/'evidence/result.json').read_text()),report)
    def test_byte_limit_preserves_failed_receipt(self):
        with tempfile.TemporaryDirectory() as root,patch.object(diagnostic.urllib.request,'urlopen',return_value=io.BytesIO(b'x'*(diagnostic.LIMITS['response_bytes']+1))):
            report=diagnostic.capture('http://localhost','test',Path(root)/'evidence',diagnostic.LIMITS)
            self.assertIn('byte limit',report['error']);self.assertEqual(len(report['requests']),1)
            self.assertFalse((Path(root)/'evidence/list.json').exists())

if __name__=='__main__':unittest.main()
