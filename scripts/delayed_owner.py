"""One exact live stale reservation; no synthetic native or authority result."""
import json
import os
import time

def read_receipt(path, session, incarnation):
    if not path.exists(): return False
    value=json.loads(path.read_text())
    if value.get('session')!=session or value['record']['incarnation']!=incarnation or value['record']['node']!='c' or value['record']['partition']!='matching':
        raise RuntimeError('delayed opener receipt identity mismatch')
    if any(event.startswith('FAILED') for event in value['events']):raise RuntimeError('delayed opener failed')
    return value

def release(path, receipt):
    temporary=path.with_suffix('.tmp')
    temporary.write_text(json.dumps({'session':receipt['session'],'record':receipt['record']}))
    os.replace(temporary,path)

def exercise(directory, session, members, assignments, publish, metrics, successful, wait, event, omes):
    baseline=assignments['matching']['node']
    assignments['matching']['node']='c';publish()
    paused=wait(lambda:read_receipt(directory/'receipt.json',session,members['c']['incarnation']),30)
    if paused['events']!=['paused-before-build'] or paused['record']['state']!='opening':raise RuntimeError('not a real before-Build reservation')
    if omes.process.poll() is not None:raise RuntimeError('Omes stopped before delayed reservation')
    assignments['matching']['node']=baseline;publish()
    before=successful(metrics(baseline),'matching')
    def serving(min_generation, min_count):
        snapshot=metrics(baseline)
        record=snapshot['partitions'].get('matching',{}).get('owner_record',{})
        if record.get('state')=='ready' and record.get('generation',0)>min_generation and successful(snapshot,'matching')>min_count:
            return snapshot
        return False
    ready=wait(lambda:serving(paused['record']['generation'],before),60)
    generation=ready['partitions']['matching']['owner_record']['generation']
    count=successful(ready,'matching')
    if omes.process.poll() is not None:raise RuntimeError('Omes stopped before stale release')
    deadline=time.monotonic()+120
    def remaining():
        value=deadline-time.monotonic()
        if value<=0:raise TimeoutError('stale opener recovery120s expired')
        return value
    release(directory/'release.json',paused)
    event('delayed-opener-released',reservation=paused,baseline_ready=ready)
    def retired():
        value=read_receipt(directory/'receipt.json',session,members['c']['incarnation'])
        if value and value['events']==['paused-before-build','released','build-succeeded','superseded-before-ready-retired']:return value
        return False
    terminal=wait(retired,remaining())
    if terminal['record']!=paused['record']:raise RuntimeError('delayed reservation changed')
    recovered=wait(lambda:serving(generation,count),remaining())
    remaining()
    if successful(metrics('c'),'matching')!=0:raise RuntimeError('superseded C served matching')
    result={'reservation':terminal,'baseline_ready':ready,'recovered':recovered,'recovery_seconds':120-remaining(),'full_acceptance':False}
    event('delayed-opener-retired-and-baseline-recovered',receipt=result)
    return result
