"""Strict, bounded measurement evidence parsing; never upgrades functional results."""
import collections
import datetime
import hashlib
import json
import math
from pathlib import Path


def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def histogram(values, minimum=100):
    values = sorted(values)
    bounds = [1_000_000, 5_000_000, 10_000_000, 50_000_000, 100_000_000,
              500_000_000, 1_000_000_000, 5_000_000_000, 30_000_000_000]
    buckets = [0] * (len(bounds) + 1)
    for value in values:
        buckets[next((i for i, bound in enumerate(bounds) if value <= bound), len(bounds))] += 1
    return {'samples': len(values), 'minimum_ns': min(values) if values else None,
            'maximum_ns': max(values) if values else None,
            'p50_ns': values[math.ceil(len(values) * .5) - 1] if values else None,
            'p99_ns': values[math.ceil(len(values) * .99) - 1] if len(values) >= minimum else None,
            'p99_eligible': len(values) >= minimum, 'p99_minimum_samples': minimum,
            'bucket_upper_bounds_ns': bounds, 'bucket_counts': buckets}


def read_trace(path, close_confirmed, max_events=2_000_000, max_line_bytes=4096, minimum=100):
    """A footer AND successful post-producer Close confirmation are mandatory."""
    result = {'complete': False, 'file': Path(path).name, 'invocations': 0, 'attempts': 0}
    invocations, attempts, groups = {}, collections.defaultdict(list), {}
    try:
        count = 0
        footer = False
        with open(path, 'rb') as stream:
            while True:
                line = stream.readline(max_line_bytes + 1)
                if not line:
                    break
                if len(line) > max_line_bytes or not line.endswith(b'\n'):
                    raise ValueError('oversized or truncated trace line')
                if footer:
                    raise ValueError('records after trace footer')
                event = json.loads(line)
                if not isinstance(event, dict):
                    raise ValueError('trace record is not an object')
                kind = event.get('kind')
                if kind == 'trace_end':
                    if event != {'kind': 'trace_end', 'status': 'true'}:
                        raise ValueError('trace footer is unsuccessful')
                    footer = True
                    continue
                if kind not in ('rpc_invocation', 'rpc_attempt'):
                    raise ValueError('unknown or failed trace record')
                allowed = {'kind', 'invocation_id', 'family', 'method', 'partition', 'operation_id', 'status', 'started', 'duration_ns'}
                if set(event) - allowed:
                    raise ValueError('unknown trace fields or schema')
                for field in ('method', 'partition', 'operation_id'):
                    if field in event and (not isinstance(event[field], str) or len(event[field]) > 256):
                        raise ValueError('invalid optional trace field')
                if event.get('status') not in ('OK', 'Canceled', 'Unknown', 'InvalidArgument', 'DeadlineExceeded', 'NotFound', 'AlreadyExists', 'PermissionDenied', 'ResourceExhausted', 'FailedPrecondition', 'Aborted', 'OutOfRange', 'Unimplemented', 'Internal', 'Unavailable', 'DataLoss', 'Unauthenticated'):
                    raise ValueError('unknown RPC status')
                count += 1
                if count > max_events:
                    raise ValueError('trace event capacity exceeded')
                identity = event.get('invocation_id')
                for field in ('invocation_id', 'family', 'status', 'started'):
                    if not isinstance(event.get(field), str) or not 1 <= len(event[field]) <= 256:
                        raise ValueError('missing or unbounded event field')
                duration = event.get('duration_ns', 0)
                if isinstance(duration, bool) or not isinstance(duration, int) or duration < 0:
                    raise ValueError('invalid duration')
                stamp = datetime.datetime.fromisoformat(event['started'])
                if stamp.tzinfo is None:
                    raise ValueError('trace timestamp lacks timezone')
                if kind == 'rpc_invocation':
                    if identity in invocations:
                        raise ValueError('duplicate invocation')
                    invocations[identity] = event
                else:
                    if not isinstance(event.get('method'), str) or not 1 <= len(event['method']) <= 256:
                        raise ValueError('attempt lacks method')
                    attempts[identity].append(event)
        if not footer or not close_confirmed:
            raise ValueError('missing successful footer or producer-close confirmation')
        for identity, records in attempts.items():
            invocation = invocations.get(identity)
            if invocation is None:
                raise ValueError('unmatched attempt')
            for event in records:
                if event['family'] != invocation['family'] or event['method'] != invocation.get('method'):
                    raise ValueError('attempt family or method mismatch')
        for kind, events in [('invocation', invocations.values()), ('attempt', (e for rows in attempts.values() for e in rows))]:
            for event in events:
                key = (event['family'], kind)
                if key not in groups:
                    if len(groups) >= 256:
                        raise ValueError('histogram group capacity exceeded')
                    groups[key] = {'durations': [], 'statuses': collections.Counter()}
                groups[key]['durations'].append(event.get('duration_ns', 0))
                groups[key]['statuses'][event['status']] += 1
        result.update(complete=True, invocations=len(invocations), attempts=sum(map(len, attempts.values())),
                      groups=[{'family': family, 'kind': kind, **histogram(data['durations'], minimum),
                               'statuses': dict(data['statuses'])} for (family, kind), data in sorted(groups.items())])
    except (ValueError, OSError, UnicodeError) as error:
        result['error'] = str(error)
    if Path(path).is_file():
        result['sha256'] = digest(path)
    return result


def read_meter(path, exit_code):
    result = {'complete': False, 'file': Path(path).name}
    try:
        if exit_code != 0:
            raise ValueError('meter did not exit cleanly')
        with open(path, 'rb') as stream:
            raw = stream.read(65537)
        lines = raw.splitlines(keepends=True)
        if len(raw) > 65536 or len(lines) != 2 or not all(line.endswith(b'\n') for line in lines):
            raise ValueError('meter must emit only ready and final report')
        ready, final = [json.loads(line) for line in lines]
        if not isinstance(ready, dict) or not isinstance(final, dict) or ready.get('event') != 'ready' or type(final.get('schema')) is not int or final['schema'] != 1:
            raise ValueError('meter schema/ready mismatch')
        required = ('attempts', 'finished', 'completed', 'canceled', 'aborted', 'transport_errors',
                    'request_read_errors', 'response_read_errors', 'response_write_errors',
                    'request_body_bytes_read', 'response_body_bytes_written', 'inflight')
        if set(final) != {'schema', 'methods', 'status', *required}:
            raise ValueError('unknown meter fields or schema')
        if any(type(final.get(key)) is not int or final[key] < 0 for key in required):
            raise ValueError('invalid meter counters')
        if final['inflight'] != 0 or final['attempts'] != final['finished'] or final['completed'] > final['finished']:
            raise ValueError('meter attempts did not drain')
        if not isinstance(final.get('methods'), dict) or not isinstance(final.get('status'), dict):
            raise ValueError('missing meter aggregate maps')
        if any(k not in ('GET', 'PUT', 'HEAD', 'POST', 'DELETE', 'OPTIONS', 'OTHER') for k in final['methods']):
            raise ValueError('unknown meter method')
        if any(not k.isdigit() or not 100 <= int(k) <= 999 for k in final['status']):
            raise ValueError('invalid meter status')
        if any(type(v) is not int or v < 0 for values in (final['methods'], final['status']) for v in values.values()):
            raise ValueError('invalid meter aggregate count')
        if sum(final['methods'].values()) != final['attempts'] or sum(final['status'].values()) > final['finished']:
            raise ValueError('meter aggregates mismatch')
        result.update(complete=True, report=final)
    except (ValueError, OSError, TypeError) as error:
        result['error'] = str(error)
    if Path(path).is_file():
        result['sha256'] = digest(path)
    return result


def finalize(evidence, traces, meter, sampler, config):
    """Processes are stopped before this function; a killed trace stays incomplete."""
    reports = []
    for name, process in traces.items():
        log = Path(evidence) / (name + '.log')
        with log.open() as stream:
            close_markers = sum(line.rstrip('\n') == 'TEMPORAL_TRACE_CLOSED' for line in stream)
        confirmed = process.process.returncode == 0 and close_markers == 1
        reports.append(read_trace(Path(evidence) / (name + '.rpc.jsonl'), confirmed,
                                  config['max_trace_events'], config['max_trace_line_bytes'], config['p99_minimum_samples']))
    resources = sampler.stop()
    resource_path = Path(evidence) / 'resources.jsonl'
    resources['sha256'] = digest(resource_path)
    meter_report = read_meter(Path(evidence) / 's3-meter.log', meter.process.returncode) if meter else {'complete': False, 'error': 'meter never started'}
    complete = bool(reports) and all(r['complete'] for r in reports) and resources['complete'] and meter_report['complete']
    return {'enabled': True, 'measurement_complete': complete, 'rpc': reports, 'resources': resources, 's3': meter_report,
            'scope': 'whole recorded process lifetimes; helper invocations and generated Execute attempts are separate',
            'limitations': ['SIGKILL traces are incomplete and cannot establish complete fault-window latency.',
                            'No whole public persistence API latency, container resources or real-S3 wire/billing measurements.']}


def producer_first(processes, traces, keep_alive=None):
    """Stop Temporal producers before their storage and measurement helpers."""
    producers = list(traces.values())
    return [p for p in producers if p is not keep_alive] + [p for p in processes if p is not keep_alive and p not in producers]
