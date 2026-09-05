import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
import runtime_measurements as m

class MeasurementTests(unittest.TestCase):
    def event(self, kind, identity='one', duration=10):
        return {'kind':kind,'invocation_id':identity,'family':'shard','method':'/xenon.Shard/Execute',
                'status':'OK','started':'2026-09-05T00:00:00Z','duration_ns':duration}

    def trace(self, records, confirmed=True, **bounds):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'trace.jsonl'
            path.write_text(''.join(json.dumps(row)+'\n' for row in records))
            return m.read_trace(path,confirmed,**bounds)

    def test_invocations_and_attempts_are_distinct(self):
        records=[]
        for i in range(100):
            records.extend([self.event('rpc_attempt',str(i),5),self.event('rpc_attempt',str(i),7),self.event('rpc_invocation',str(i),100)])
        result=self.trace(records+[{'kind':'trace_end','status':'true'}])
        self.assertTrue(result['complete']);self.assertEqual(result['invocations'],100);self.assertEqual(result['attempts'],200)
        groups={row['kind']:row for row in result['groups']}
        self.assertEqual(groups['invocation']['p99_ns'],100);self.assertEqual(groups['attempt']['p99_ns'],7)
        self.assertEqual(sum(groups['attempt']['bucket_counts']),200)
        self.assertIsNone(m.histogram([100]*99)['p99_ns'])

    def test_incomplete_trace_controls(self):
        attempt=self.event('rpc_attempt');invocation=self.event('rpc_invocation');footer={'kind':'trace_end','status':'true'}
        for records in ([attempt,footer],[invocation,invocation,footer],[invocation],
                        [invocation,{'kind':'trace_end','status':'false'}],
                        [invocation,footer,invocation],[{'kind':'trace_error'},footer],
                        [dict(attempt,family='wrong'),invocation,footer],
                        [dict(invocation,duration_ns=-1),footer],
                        [dict(invocation,duration_ns=True),footer],
                        [dict(invocation,duration_ns='10'),footer],
                        [dict(invocation,schema=99),footer],
                        [dict(invocation,status='invented'),footer]):
            with self.subTest(records=records):self.assertFalse(self.trace(records)['complete'])
        self.assertFalse(self.trace([invocation,footer],False)['complete'])
        self.assertFalse(self.trace([attempt,invocation,footer],max_events=1)['complete'])
        self.assertFalse(self.trace([invocation,footer],max_line_bytes=10)['complete'])
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'trace';path.write_text(json.dumps(footer))
            self.assertFalse(m.read_trace(path,True)['complete'])
            path.write_text('{broken\n');self.assertFalse(m.read_trace(path,True)['complete'])

    def meter_report(self):
        counters=('attempts','finished','completed','canceled','aborted','transport_errors','request_read_errors','response_read_errors','response_write_errors','request_body_bytes_read','response_body_bytes_written','inflight')
        return {'schema':1,**{k:0 for k in counters},'methods':{},'status':{}}

    def test_meter_completeness_controls(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'meter'
            def write(report):path.write_text(json.dumps({'event':'ready'})+'\n'+json.dumps(report)+'\n')
            valid=self.meter_report();write(valid)
            self.assertTrue(m.read_meter(path,0)['complete'])
            self.assertFalse(m.read_meter(path,-9)['complete'])
            for fields in ({'inflight':1},{'attempts':1},{'completed':1},{'status':{'200':-1}},{'methods':{'SECRET':0}},{'schema':True},{'schema':99}):
                write(dict(valid,**fields));self.assertFalse(m.read_meter(path,0)['complete'])
            path.write_text('{}\n');self.assertFalse(m.read_meter(path,0)['complete'])

    def test_producers_stop_before_storage_and_meter_survives_cold(self):
        meter, node, temporal, cold = object(), object(), object(), object()
        processes=[meter,node,temporal,cold]
        traces={'temporal':temporal}
        self.assertEqual(m.producer_first(processes,traces,meter),[temporal,node,cold])
        self.assertEqual(m.producer_first(list(reversed(processes)),traces),[temporal,cold,node,meter])

    def test_killed_producer_never_becomes_complete(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);(root/'temporal.log').write_text('TEMPORAL_STARTED\n')
            (root/'temporal.rpc.jsonl').write_text(json.dumps(self.event('rpc_invocation'))+'\n')
            (root/'resources.jsonl').write_text('{}\n')
            (root/'s3-meter.log').write_text(json.dumps({'event':'ready'})+'\n'+json.dumps(self.meter_report())+'\n')
            sampler=SimpleNamespace(stop=lambda:{'complete':True})
            process=SimpleNamespace(process=SimpleNamespace(returncode=-9))
            meter=SimpleNamespace(process=SimpleNamespace(returncode=0))
            result=m.finalize(root,{'temporal':process},meter,sampler,{'max_trace_events':100,'max_trace_line_bytes':4096,'p99_minimum_samples':100})
            self.assertFalse(result['measurement_complete']);self.assertTrue(result['s3']['complete']);self.assertFalse(result['rpc'][0]['complete'])

if __name__=='__main__':unittest.main()
