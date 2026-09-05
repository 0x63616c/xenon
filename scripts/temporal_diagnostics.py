"""Bounded best-effort public API snapshots after failure; never an oracle."""
import hashlib
import json
from pathlib import Path
import time
import urllib.parse
import urllib.request

LIMITS={'schema':1,'seconds':15,'workflows':20,'response_bytes':2097152,'request_seconds':2}

def capture(base,namespace,directory,limits):
    if limits!=LIMITS:raise ValueError('diagnostic limits changed')
    directory=Path(directory);directory.mkdir(exist_ok=False)
    report={'scope':'post-failure diagnostic only; first pages may be incomplete','requests':[],'truncated':False}
    deadline=time.monotonic()+limits['seconds']
    prefix=base.rstrip('/')+'/api/v1/namespaces/'+urllib.parse.quote(namespace,safe='')+'/workflows'
    def get(url,name):
        remaining=deadline-time.monotonic()
        if remaining<=0:raise TimeoutError('diagnostic time budget exhausted')
        entry={'url':url};report['requests'].append(entry)
        try:
            with urllib.request.urlopen(url,timeout=min(limits['request_seconds'],remaining)) as response:
                raw=response.read(limits['response_bytes']+1)
            if len(raw)>limits['response_bytes']:raise ValueError('diagnostic response exceeds byte limit')
            (directory/name).write_bytes(raw)
            entry.update(file=name,sha256=hashlib.sha256(raw).hexdigest())
            value=json.loads(raw)
            if not isinstance(value,dict):raise ValueError('diagnostic response is not an object')
            if value.get('nextPageToken'):report['truncated']=True
            return value
        except Exception as error:
            entry['error']=type(error).__name__+': '+str(error)
            raise
    try:
        listing=get(prefix+'?'+urllib.parse.urlencode({'pageSize':limits['workflows']}),'list.json')
        executions=listing.get('executions',[])
        if not isinstance(executions,list):raise ValueError('invalid execution list')
        if len(executions)>limits['workflows']:report['truncated']=True
        for index,info in enumerate(executions[:limits['workflows']]):
            execution=info['execution'];wid=execution['workflowId'];rid=execution['runId']
            if not isinstance(wid,str) or not wid or not isinstance(rid,str) or not rid:raise ValueError('missing execution identity')
            path=prefix+'/'+urllib.parse.quote(wid,safe='')
            query=urllib.parse.urlencode({'runId':rid})
            for kind,url in [('describe',path+'?'+query),('history',path+'/history?'+query)]:
                try:get(url,f'{index:02d}-{kind}.json')
                except Exception:pass
            if time.monotonic()>=deadline:raise TimeoutError('diagnostic time budget exhausted')
    except Exception as error:report['error']=type(error).__name__+': '+str(error)
    finally:
        report['elapsed_seconds']=limits['seconds']-(deadline-time.monotonic())
        (directory/'result.json').write_text(json.dumps(report,indent=2)+'\n')
    return report
