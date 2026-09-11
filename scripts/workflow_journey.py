"""Generated real-workflow component, called inside the owned CLI dev journey."""
from contextlib import contextmanager
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
    initial_root_runs = []
    totals = dict(initial_roots=0, child_runs=0, continued_runs=0, nexus_handler_runs=0, child_starts=0, activity_scheduled=0, activity_started=0, nexus_operation_scheduled=0, nexus_operation_started=0)
    expected = dict(roots=0, children=0, continuations=0, activities=0, nexus_operations=0, nexus_handlers=0)
    observed = dict(expected)
    fanout = dict(children=0, activities=0, nexus=0)
    limits = None
    for receipt in sorted(directory.glob('**/histories/result.json')):
        result = json.loads(receipt.read_text())
        proof = result.get('generated_intent')
        if not proof or proof.get('contract') != 'omes-generated-intent-v1' or proof.get('expected') != proof.get('observed'):
            raise ValueError('missing or mismatched generated intent audit')
        if limits is None:
            limits = proof['limits']
        if proof['limits'] != limits:
            raise ValueError('generated intent limits changed between members')
        for key in expected:
            expected[key] += proof['expected'][key]
            observed[key] += proof['observed'][key]
        for key in fanout:
            fanout[key] = max(fanout[key], proof['observed_max_fanout'][key])
            if proof['observed_max_fanout'][key] > proof['limits'][key]:
                raise ValueError('generated workflow fanout exceeds explicit limit')
        roots = set()
        intervals = []
        for run in result['runs']:
            path = receipt.parent / run['history_file']
            raw = path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != run['history_sha256']:
                raise ValueError('history checksum mismatch')
            events = json.loads(raw)['events']
            event_counters = {
                'EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED': 'child_starts',
                'EVENT_TYPE_ACTIVITY_TASK_SCHEDULED': 'activity_scheduled',
                'EVENT_TYPE_ACTIVITY_TASK_STARTED': 'activity_started',
                'EVENT_TYPE_NEXUS_OPERATION_SCHEDULED': 'nexus_operation_scheduled',
                'EVENT_TYPE_NEXUS_OPERATION_STARTED': 'nexus_operation_started',
            }
            for event in events:
                counter = event_counters.get(event.get('eventType'))
                if counter:
                    totals[counter] += 1
            first = events[0]['workflowExecutionStartedEventAttributes']
            continued = bool(first.get('continuedExecutionRunId'))
            child = bool(first.get('parentWorkflowExecution'))
            handler = 'NexusHandler' in first['workflowType']['name']
            totals['continued_runs'] += int(continued)
            totals['child_runs'] += int(child)
            totals['nexus_handler_runs'] += int(handler)
            if not continued and not child and not handler:
                totals['initial_roots'] += 1
                initial_root_runs.append(dict(workflow_id=run['workflow_id'], run_id=run.get('run_id')))
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
    if limits is None or expected != observed:
        raise ValueError('generated intent census missing')
    actual = dict(roots=totals['initial_roots'], children=totals['child_runs'], continuations=totals['continued_runs'], activities=totals['activity_scheduled'], nexus_operations=totals['nexus_operation_scheduled'], nexus_handlers=totals['nexus_handler_runs'])
    if actual != observed:
        raise ValueError('history event census disagrees with generated intent audit')
    return dict(totals=totals, initial_root_runs=initial_root_runs, case_execution_interval_peak={case: overlap([interval for member in items for interval in member]) for case, items in cases.items()},
                generated_intent=dict(expected=expected, observed=observed, observed_max_fanout=fanout, limits=limits),
                limitations=['No admission barrier yet: observed overlap does not prove four roots simultaneously Running before release.',
                             'Concurrency-one comparison remains unqualified.'])


@contextmanager
def admission_proxy(binary, upstream, concurrency, evidence, env):
    """Own one local fixture relay; its receipt precedes every barrier release."""
    binary = binary.resolve()
    receipt = evidence / 'admission.json'
    argv = [str(binary), '--upstream', upstream, '--concurrency', str(concurrency),
            '--namespace', 'xenon-ministack', '--receipt', str(receipt)]
    owner = dict(argv=argv, binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                 pid=None, cleanup_verified=False)
    owner_path = evidence / 'admission-proxy-owner.json'
    owner_path.write_text(json.dumps(owner) + '\n')
    process = None
    address_path = evidence / 'admission-proxy-address.json'
    with address_path.open('xb') as stdout, (evidence / 'admission-proxy.log').open('xb') as stderr:
        try:
            process = subprocess.Popen(argv, env=env, stdout=stdout, stderr=stderr, start_new_session=True)
            owner['pid'] = process.pid
            owner_path.write_text(json.dumps(owner) + '\n')
            deadline = time.monotonic() + 10
            while True:
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError('admission proxy failed before ready')
                try:
                    address = json.loads(address_path.read_text())['address']
                    break
                except (ValueError, KeyError):
                    time.sleep(.05)
            yield address
        finally:
            if process is not None:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=5)
                owner['exit_code'] = process.returncode
                try:
                    os.killpg(process.pid, 0)
                except ProcessLookupError:
                    owner['cleanup_verified'] = True
                owner_path.write_text(json.dumps(owner, indent=2) + '\n')
                if not owner['cleanup_verified']:
                    raise RuntimeError('admission proxy process group remains pending')


def admission_counts(path, concurrency, initial_root_runs):
    receipt = json.loads(path.read_text())
    windows = receipt['windows']
    if receipt['schema'] != 1 or receipt['concurrency'] != concurrency or len(windows) != 12 // concurrency:
        raise ValueError('unexpected admission receipt shape')
    roots = [root for window in windows for root in window['roots']]
    if any(not w['released'] or len(w['roots']) != concurrency for w in windows):
        raise ValueError('incomplete admission window')
    if len(roots) != 12 or len({r['workflow_id'] for r in roots}) != 12 or len({r['run_id'] for r in roots}) != 12:
        raise ValueError('expected twelve distinct root executions')
    if any(r['status'] != 'Running' for r in roots):
        raise ValueError('root was not observed Running before release')
    if {(r['workflow_id'], r['run_id']) for r in roots} != {(r['workflow_id'], r['run_id']) for r in initial_root_runs}:
        raise ValueError('admitted roots disagree with saved history census')
    cases = {}
    for r in roots:
        case, member = r['queue'].rsplit('-', 1)
        cases.setdefault(case, []).append(member)
    if len(cases) != 3 or any(sorted(m) != ['00', '01', '02', '03'] for m in cases.values()):
        raise ValueError('expected three cases of four independent roots')
    if concurrency == 4 and any(len({r['queue'].rsplit('-', 1)[0] for r in w['roots']}) != 1 for w in windows):
        raise ValueError('admission window mixed cases')
    return dict(observed_running_peak=concurrency, admitted_roots=len(roots), windows=len(windows),
                receipt_sha256=hashlib.sha256(path.read_bytes()).hexdigest())

def run_search(cli, bundle, oracle, evidence, fixture, env, run, proxy=None, concurrency=4):
    bundle, oracle = bundle.resolve(), oracle.resolve()
    build_path = bundle / 'worker/build.json'
    build = json.loads(build_path.read_text())
    worker = Path(build['source']) / 'workers/go/prepared/program'
    expected = build['prepared_sha256']['program']
    if hashlib.sha256(worker.read_bytes()).hexdigest() != expected:
        raise ValueError('prepared worker checksum mismatch')
    address = '127.0.0.1:' + str(fixture['temporal_port'])
    if proxy is not None:
        with admission_proxy(proxy, address, concurrency, evidence, env) as relay:
            result = _run_search(cli, bundle, oracle, evidence, env, run, build_path, build, worker, address, relay, concurrency)
            result['admission'] = admission_counts(evidence / 'admission.json', concurrency, result['initial_root_runs'])
            if any(peak != concurrency for peak in result['case_execution_interval_peak'].values()):
                raise RuntimeError('actual execution interval peak disagrees with admission limit')
            result['limitations'] = []
            (evidence / 'workflow-observations.json').write_text(json.dumps(result, indent=2) + '\n')
            return result
    return _run_search(cli, bundle, oracle, evidence, env, run, build_path, build, worker, address, address, concurrency)


def _run_search(cli, bundle, oracle, evidence, env, run, build_path, build, worker, address, relay, concurrency):
    resident = evidence / 'resident.json'
    resident.write_text(json.dumps(dict(version=1, address=relay, namespace='xenon-ministack', nexus_endpoint='xenon-fuzz', nexus_task_queue='omes-xenon-ministack-fuzz')) + '\n')
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
                          '--workflows-per-case', '4', '--workflow-concurrency', str(concurrency), '--workload-seed', '42',
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
