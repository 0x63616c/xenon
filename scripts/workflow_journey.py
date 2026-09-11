"""Generated real-workflow component, called inside the owned CLI dev journey."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time


def overlap(intervals):
    events = [(start, 1) for start, end in intervals] + [(end, -1) for start, end in intervals]
    current = peak = 0
    for _, change in sorted(events):
        current += change
        peak = max(peak, current)
    return peak


def history_counts(directory):
    members = []
    totals = dict(initial_roots=0, child_runs=0, continued_runs=0, nexus_handler_runs=0)
    for receipt in sorted(directory.glob('**/histories/result.json')):
        result = json.loads(receipt.read_text())
        roots = set()
        intervals = []
        for run in result['runs']:
            path = receipt.parent / run['history_file']
            raw = path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != run['history_sha256']:
                raise ValueError('history checksum mismatch')
            events = json.loads(raw)['events']
            first = events[0]['workflowExecutionStartedEventAttributes']
            continued = bool(first.get('continuedExecutionRunId'))
            child = bool(first.get('parentWorkflowExecution'))
            handler = 'NexusHandler' in first['workflowType']['name']
            totals['continued_runs'] += int(continued)
            totals['child_runs'] += int(child)
            totals['nexus_handler_runs'] += int(handler)
            if not continued and not child and not handler:
                totals['initial_roots'] += 1
            if not child and not handler:
                def timestamp(event):
                    return datetime.datetime.fromisoformat(event['eventTime'].replace('Z', '+00:00')).timestamp()
                start, end = timestamp(events[0]), timestamp(events[-1])
                roots.add(run['workflow_id'])
                intervals.append((start, end))
        if len(roots) != 1:
            raise ValueError('expected one root chain per member')
        members.append((receipt.relative_to(directory).parts[0], intervals))
    cases = {}
    for case, interval in members:
        cases.setdefault(case, []).append(interval)
    if len(cases) != 3 or any(len(items) != 4 for items in cases.values()) or totals['initial_roots'] != 12:
        raise ValueError('expected three cases each containing four initial roots')
    return dict(totals=totals, case_execution_interval_peak={case: overlap([interval for member in items for interval in member]) for case, items in cases.items()},
                limitations=['No admission barrier yet: observed overlap does not prove four roots simultaneously Running before release.',
                             'History inventory is visibility-derived; complete expected child/Nexus graph census remains required.',
                             'Concurrency-one comparison and independent fan-out limits remain unqualified.'])


def run_search(cli, bundle, oracle, evidence, fixture, env, run):
    bundle, oracle = bundle.resolve(), oracle.resolve()
    build_path = bundle / 'worker/build.json'
    build = json.loads(build_path.read_text())
    worker = Path(build['source']) / 'workers/go/prepared/program'
    expected = build['prepared_sha256']['program']
    if hashlib.sha256(worker.read_bytes()).hexdigest() != expected:
        raise ValueError('prepared worker checksum mismatch')
    address = '127.0.0.1:' + str(fixture['temporal_port'])
    resident = evidence / 'resident.json'
    resident.write_text(json.dumps(dict(version=1, address=address, namespace='xenon-ministack', nexus_endpoint='xenon-fuzz', nexus_task_queue='omes-xenon-ministack-fuzz')) + '\n')
    argv = [str(worker), 'worker', '--err-on-unimplemented', '--server-address', address,
            '--namespace', 'xenon-ministack', '--task-queue', 'omes-xenon-ministack-fuzz']
    ownership = dict(argv=argv, pid=None, cleanup_verified=False)
    owner_path = evidence / 'nexus-worker-owner.json'
    owner_path.write_text(json.dumps(ownership) + '\n')
    process = None
    with (evidence / 'nexus-worker.log').open('xb') as log:
        try:
            process = subprocess.Popen(argv, cwd=build['source'], env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            ownership['pid'] = process.pid
            owner_path.write_text(json.dumps(ownership) + '\n')
            deadline = time.monotonic() + 15
            while b'Started Worker' not in (evidence / 'nexus-worker.log').read_bytes():
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError('Nexus worker failed before ready')
                time.sleep(.1)
            output = run([cli, 'search', '--mode', 'real', '--max-cases', '3', '--duration', '6m',
                          '--workflows-per-case', '4', '--workflow-concurrency', '4', '--workload-seed', '42',
                          '--fault-seed', '1', '--bundle', str(bundle), '--runtime-build', str(build_path),
                          '--resident-fixture', str(resident), '--history-oracle', str(oracle),
                          '--history-oracle-sha256', hashlib.sha256(oracle.read_bytes()).hexdigest(),
                          '--runtime-evidence', str(evidence / 'workflow-runtime'), '--evidence', str(evidence / 'workflow-search'),
                          '--development'], timeout=420)
            result = json.loads(output)
            if result['result']['completed'] != 3 or result['result']['stop_reason'] != 'completed':
                raise RuntimeError('finite three-case target not completed')
            observations = history_counts(evidence / 'workflow-runtime')
            (evidence / 'workflow-observations.json').write_text(json.dumps(observations, indent=2) + '\n')
            return observations
        finally:
            if process is not None:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGINT)
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=5)
                ownership['exit_code'] = process.returncode
                try:
                    os.killpg(process.pid, 0)
                except ProcessLookupError:
                    ownership['cleanup_verified'] = True
                owner_path.write_text(json.dumps(ownership, indent=2) + '\n')
                if not ownership['cleanup_verified']:
                    raise RuntimeError('Nexus worker process group remains pending')
