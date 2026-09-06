"""Independent read-only control assertions for the real multi-agent scenario."""
import base64
import hashlib
import json
import struct


def decode_control(raw, config):
    envelope = json.loads(raw)
    if envelope['format'] != 1 or envelope['key'] != 'cluster/control':
        raise ValueError('unexpected registry envelope')
    body = base64.b64decode(envelope['body'], validate=True)
    expected = base64.b64decode(envelope['expected'] or '', validate=True)
    digest = hashlib.sha256(b'xenon.registry.write.v1\0')
    for field in (b'cluster/control', expected, body):
        digest.update(struct.pack('>Q', len(field)))
        digest.update(field)
    if digest.hexdigest() != envelope['digest']:
        raise ValueError('registry payload digest mismatch')
    control = json.loads(body)
    settings = config['service_storage']
    layout = settings['layout']
    if control['format'] != 2 or control['cluster'] != settings['cluster_id'] or control['layout'] != layout:
        raise ValueError('control does not match configured immutable layout')
    if set(control['partitions']) != {p['id'] for p in layout['partitions']}:
        raise ValueError('physical partition inventory changed')
    for physical in layout['partitions']:
        actual = control['partitions'][physical['id']]
        if actual['path'] != physical['path'] or actual['assignment_revision'] < 1:
            raise ValueError('partition path or assignment invalid')
        if actual['ready'] and (actual['generation'] < 1 or not actual.get('reservation')):
            raise ValueError('ready partition has no reservation')
    return control


def ready_assignments(control, node):
    return {p['logical_name']: control['partitions'][p['id']]
            for p in control['layout']['partitions']
            if control['partitions'][p['id']]['ready']
            and control['partitions'][p['id']]['desired']['node'] == node}


def absent_owner(control, node):
    return all(p['desired']['node'] != node for p in control['partitions'].values())


def same_authority(a, b):
    return all(a.get(k) == b.get(k) for k in
               ('desired', 'path', 'assignment_revision', 'generation', 'reservation', 'ready'))
