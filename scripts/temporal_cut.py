"""Metadata-only binding of an actual workflow UPDATE to native cut control."""
from datetime import datetime, timezone
import json
import re
import time
import urllib.request
import uuid

STAGES=('commit_before_await','after_await','before_result_publication')

def valid_operation_reference(value):
    """Accept canonical Go IDs and the exact legacy ingress alphabet."""
    if not isinstance(value, str):
        return False
    if re.fullmatch(r'[a-zA-Z0-9-]{1,128}', value):
        return True
    if not re.fullmatch(r'op_[0-9A-Za-z]{22}', value):
        return False
    alphabet = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
    number = 0
    for character in value[3:]:
        number = number * 62 + alphabet.index(character)
    return number < 1 << 128

def validate_state(state, control, pid, expected_state, watch=None, selector=None):
    if state.get('schema')!=1 or state.get('session')!=control['session'] or state.get('pid')!=pid or state.get('stage')!=control['stage'] or state.get('state')!=expected_state:
        raise ValueError('wrong process-cut state identity')
    uuid.UUID(state['incarnation'])
    if control.get('incarnation') and state['incarnation']!=control['incarnation']:
        raise ValueError('process-cut boot changed')
    if watch is not None and state.get('watch')!=watch:
        raise ValueError('process-cut workflow identity changed')
    if expected_state in ('candidate','paused'):
        selected=state.get('selector',{})
        if selected.get('family')!='execution' or selected.get('mutation_kind')!='UPDATE' or selected.get('partition')!=control['partition'] or not valid_operation_reference(selected.get('operation_id','')) or not re.fullmatch('[0-9a-f]{64}',selected.get('command_sha256','')):
            raise ValueError('invalid actual execution selector')
        if selector is not None and selected!=selector:raise ValueError('candidate selector changed')
    if expected_state=='paused':
        hit=datetime.fromisoformat(state['hit_utc'].replace('Z','+00:00'))
        if hit.tzinfo is None:raise ValueError('cut hit lacks timezone')
        age=(datetime.now(timezone.utc)-hit).total_seconds()
        if age<0 or age>=5:raise ValueError('cut hit expired or is in future')
    return state

def read(control):
    with urllib.request.urlopen(control['url']+'/state',timeout=1) as response:
        raw=response.read(16385)
        if len(raw)>16384:raise ValueError('cut metadata exceeds bound')
        return json.loads(raw)

def post(control,path,value):
    req=urllib.request.Request(control['url']+path,data=json.dumps(value).encode(),headers={'Content-Type':'application/json'},method='POST')
    with urllib.request.urlopen(req,timeout=1) as response:
        if response.status!=202:raise ValueError('cut control rejected transition')

def watch(control,pid,identity):
    state=validate_state(read(control),control,pid,'unarmed')
    control['incarnation']=state['incarnation']
    post(control,'/watch',{'session':control['session'],'incarnation':control['incarnation'],'watch':identity})
    return validate_state(read(control),control,pid,'watching',identity)

def await_stage(control,pid,identity,state_name,selector=None,timeout=20):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        state=read(control)
        if state.get('state')==state_name:return validate_state(state,control,pid,state_name,identity,selector)
        if state.get('state') not in ('watching','candidate','armed'):
            raise ValueError('cut reached unexpected or expired state')
        # Even during polling, a changed process/session is never a new candidate.
        validate_state(state,control,pid,state['state'],identity,selector if state['state']=='candidate' else None)
        time.sleep(.01)
    raise TimeoutError('actual execution cut stage was not observed')

def arm(control,candidate):
    post(control,'/arm',{'session':control['session'],'incarnation':control['incarnation'],'selector':candidate['selector']})

def validate_config(value):
    expected={'schema':1,'stages':list(STAGES),'pause_timeout_ms':5000,'candidate_timeout_seconds':20,'control_port_offset':200,'update_mode':'update-only','fault':'SIGKILL','recovery_budget':'test/scenarios/ministack/case.json:recovery_seconds','full_acceptance':False}
    if value!=expected:raise ValueError('process-cut declaration changed')
    return value

def validate_signal(returncode):
    import signal
    if returncode!=-signal.SIGKILL:raise ValueError('declared cut was not actual SIGKILL')
