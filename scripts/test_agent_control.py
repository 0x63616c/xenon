import base64
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import struct
import subprocess
import sys
from unittest.mock import patch
from types import SimpleNamespace
import unittest

from agent_control import decode_control, ready_assignments, absent_owner, same_authority

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('agent_smoke', ROOT/'scripts/agent-smoke.py')
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


class AgentControlTests(unittest.TestCase):
    def setUp(self):
        self.config=json.loads((ROOT/'test/scenarios/agent/a.json').read_text())
        settings=self.config['service_storage']
        self.control={'format':2,'cluster':settings['cluster_id'],'layout':copy.deepcopy(settings['layout']),
                      'partitions':{p['id']:{'path':p['path'],'desired':{'node':'nod_c','incarnation':'inc_c'},
                                            'assignment_revision':2,'generation':3,'reservation':'trn_c','ready':True}
                                    for p in settings['layout']['partitions']}}

    def envelope(self, control):
        body=json.dumps(control).encode()
        digest=hashlib.sha256(b'xenon.registry.write.v1\0')
        for field in (b'cluster/control',b'',body):
            digest.update(struct.pack('>Q',len(field)));digest.update(field)
        return json.dumps({'format':1,'key':'cluster/control','expected':None,
                           'body':base64.b64encode(body).decode(),'digest':digest.hexdigest()})

    def test_dynamic_assignment_and_aba(self):
        control=decode_control(self.envelope(self.control),self.config)
        owned=ready_assignments(control,'nod_c')
        self.assertEqual(len(owned),10)
        self.assertTrue(absent_owner(control,'nod_b'))
        first=owned['global']; changed=copy.deepcopy(first);changed['generation']+=1
        self.assertFalse(same_authority(first,changed))
        changed=copy.deepcopy(first);changed['desired']['incarnation']='replacement'
        self.assertFalse(same_authority(first,changed))

    def test_layout_or_payload_tampering_fails(self):
        changed=copy.deepcopy(self.control)
        changed['layout']['partitions'].reverse()
        with self.assertRaises(ValueError):decode_control(self.envelope(changed),self.config)
        changed=copy.deepcopy(self.control);next(iter(changed['partitions'].values()))['path']='other/data'
        with self.assertRaises(ValueError):decode_control(self.envelope(changed),self.config)
        envelope=json.loads(self.envelope(self.control));envelope['digest']='0'*64
        with self.assertRaises(ValueError):decode_control(json.dumps(envelope),self.config)

    def test_background_failure_is_not_a_health_retry(self):
        child=SimpleNamespace(pid=1,poll=lambda:1)
        with self.assertRaises(smoke.ScenarioInvariant):smoke.check_children([child],set(),{1:'a'})
        with self.assertRaises(smoke.ScenarioInvariant):smoke.check_children([child],set(),{1:'omes'})
        smoke.check_children([child],{1},{1:'a'})
        child.poll=lambda:0
        smoke.check_children([child],set(),{1:'omes'})
        with self.assertRaises(smoke.ScenarioInvariant):smoke.check_children([child],set(),{1:'worker'})

    def test_exit_between_poll_and_kill_still_reaps_and_keeps_output(self):
        child=subprocess.Popen([sys.executable,'-c',
            'import sys; sys.stdin.read(1); print("preserved child output", flush=True)'],
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,
            text=True,start_new_session=True)
        def exited_before_kill(pid,signal):
            self.assertEqual(pid,child.pid)
            child.stdin.write('x');child.stdin.flush()
            child.wait(timeout=5)
            raise ProcessLookupError('already exited')
        try:
            with patch.object(smoke.os,'killpg',side_effect=exited_before_kill) as kill:
                output,_=smoke.kill_and_collect(child)
            kill.assert_called_once()
            self.assertEqual(child.returncode,0)
            self.assertEqual(output,'preserved child output\n')
        finally:
            if child.poll() is None:child.kill()
            child.communicate(timeout=5)


if __name__=='__main__':unittest.main()
