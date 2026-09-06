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

def dispatch_after_ready(metrics, successful, wait, node, incarnation, minimum_generation, timeout):
    # Counts are process-lifetime totals. Establish a fresh baseline only after
    # observing the desired READY generation, then require subsequent work.
    observed = None
    count = None
    def check():
        nonlocal observed, count
        snapshot = metrics(node)
        record = snapshot['partitions'].get('matching', {}).get('owner_record', {})
        if snapshot.get('incarnation') != incarnation or record.get('incarnation') != incarnation or record.get('node') != node:
            raise RuntimeError('baseline owner incarnation mismatch')
        generation = record.get('generation', 0)
        if record.get('state') != 'ready' or generation <= minimum_generation:
            observed = None
            count = None
            return False
        current = successful(snapshot, 'matching')
        if observed != generation:
            observed, count = generation, current
            return False
        return snapshot if current > count else False
    return wait(check, timeout)

def exercise(directory, session, members, assignments, publish, metrics, successful, wait, event, omes):
    baseline=assignments['matching']['node']
    assignments['matching']['node']='c';publish()
    paused=wait(lambda:read_receipt(directory/'receipt.json',session,members['c']['incarnation']),30)
    if paused['events']!=['paused-before-build'] or paused['record']['state']!='opening':raise RuntimeError('not a real before-Build reservation')
    if paused['record']['address']!=members['c']['address'] or paused['record']['data_prefix']!=assignments['matching']['data_prefix']:raise RuntimeError('undeclared stale reservation address/prefix')
    if omes.process.poll() is not None:raise RuntimeError('Omes stopped before delayed reservation')
    assignments['matching']['node']=baseline;publish()
    ready=dispatch_after_ready(metrics,successful,wait,baseline,members[baseline]['incarnation'],paused['record']['generation'],60)
    generation=ready['partitions']['matching']['owner_record']['generation']
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
    recovered=dispatch_after_ready(metrics,successful,wait,baseline,members[baseline]['incarnation'],generation,remaining())
    remaining()
    if successful(metrics('c'),'matching')!=0:raise RuntimeError('superseded C served matching')
    result={'reservation':terminal,'baseline_ready':ready,'recovered':recovered,'recovery_seconds':120-remaining(),'full_acceptance':False}
    event('delayed-opener-retired-and-baseline-recovered',receipt=result)
    return result
