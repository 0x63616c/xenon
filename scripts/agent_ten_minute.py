"""Opt-in ten-minute saved Omes corpus on the shared agent supervisor."""
import base64
import json
import time

from corrected_fuzz import configuration, check_build
from omes_workloads import sha

PROFILE = 'test/scenarios/agent/ten-minute.json'


def expand(root):
    profile = json.loads((root / PROFILE).read_text())
    if profile['schema'] != 1 or profile['workload_seconds'] != 600:
        raise ValueError('ten-minute profile requires exactly 600 seconds of workload')
    for field in ('setup_seconds','mixed_seconds','drain_seconds','verification_seconds','cold_recovery_seconds','cleanup_seconds','maximum_commands'):
        if type(profile[field]) is not int or not 1 <= profile[field] <= 3600:
            raise ValueError('invalid bounded profile field: '+field)
    expected = [('join-c', 60, 180), ('kill-b', 240, 330), ('restart-b', 360, 480)]
    if [(f['action'], f['not_before_seconds'], f['complete_by_seconds']) for f in profile['faults']] != expected:
        raise ValueError('unregistered fault schedule')
    _, replay, contract = configuration(root)
    mixed = json.loads((root / 'proof/acceptance/full-profile.json').read_text())['workloads']['mixed']
    commands = []
    inputs = {}
    for original in replay['commands']:
        command = list(original)
        for index, arg in enumerate(command):
            if arg.startswith('input-file='):
                path = root / arg.removeprefix('input-file=')
                inputs[str(path.relative_to(root))] = {'sha256': sha(path), 'base64': base64.b64encode(path.read_bytes()).decode('ascii')}
                command[index] = 'input-file=' + str(path)
        commands.append(command)
    if len(commands) != 20 or len(inputs) != 20:
        raise ValueError('expected all 20 exact corrected corpus inputs')
    return dict(profile=profile, fuzz_commands=commands, mixed_command=mixed,
                inputs=inputs, compatibility_overlay=True, contract=contract)


def exercise(schedule, commands, launch, poll_health, fault, record, *, finish=lambda process:None, now=time.monotonic, sleep=time.sleep):
    """600 seconds of real command admission; drain is a separate bounded phase.

    No sleeping after a short corpus to manufacture duration: repeat saved inputs,
    preserving input identity. Any child/fault error stops admission immediately.
    """
    started = now()
    end = started + schedule['workload_seconds']
    drain_end = end + schedule['drain_seconds']
    pending = None
    next_fault = 0
    completed = 0
    launched = 0
    while True:
        poll_health()
        current = now()
        if pending is not None:
            code = pending.poll()
            if code is not None:
                if code != 0:
                    raise RuntimeError('Omes fuzz command failed: ' + str(code))
                finish(pending)
                record({'event': 'fuzz-completed', 'index': launched - 1, 'input': (launched - 1) % len(commands), 'elapsed_seconds': current - started})
                completed += 1
                pending = None
        if next_fault < len(schedule['faults']):
            item = schedule['faults'][next_fault]
            if current - started > item['complete_by_seconds']:
                raise TimeoutError('missed fault deadline: '+item['action'])
            if current - started >= item['not_before_seconds']:
                if pending is None:
                    # Faults require an actually active Omes child; the callback
                    # additionally observes a running workflow through Temporal.
                    if current >= end:
                        raise RuntimeError('fault trigger missing before workload end')
                else:
                    record({'event':'fault-started','action':item['action'],'elapsed_seconds':current-started})
                    fault(item['action'])
                    elapsed = now() - started
                    if elapsed > item['complete_by_seconds']:
                        raise TimeoutError('fault completion deadline: ' + item['action'])
                    record({'event': 'fault-completed', 'action': item['action'], 'elapsed_seconds': elapsed})
                    next_fault += 1
                    continue
        if current >= end:
            if pending is None:
                break
            if current >= drain_end:
                raise TimeoutError('Omes drain deadline')
        elif pending is None:
            if launched >= schedule['maximum_commands']:
                raise RuntimeError('command ceiling reached before ten minutes')
            command = commands[launched % len(commands)]
            # Exact expanded argv was persisted before launch by the caller.
            pending = launch(launched, command)
            launched += 1
        sleep(.1)
    if completed < len(commands) or next_fault != len(schedule['faults']):
        raise RuntimeError('incomplete corpus or fault schedule')
    return {'workload_seconds': schedule['workload_seconds'], 'elapsed_seconds': now() - started,
            'completed_commands': completed, 'complete_corpus_rounds': completed // len(commands),
            'faults_completed': next_fault}


def run(root, receipt, expanded, *, command, launch, stop, wait, probe, topology,
        joined_owner, start, agents, check_children, set_budget, record, report):
    profile = expanded['profile']
    source = root / '.local/omes-source'
    destination = receipt / 'omes-overlay'
    command(['python3', 'scripts/prepare-corrected-omes.py', '--source', str(source), '--output', str(destination)], timeout=profile['setup_seconds'])
    build = destination / 'build.json'
    manifest = json.loads(build.read_text())
    overlay_source, binary = check_build(manifest, build)
    report['corrected_omes_build'] = manifest
    report['corrected_omes_manifest_sha256'] = sha(build)
    probe('fuzz-endpoint')
    for node in ('a','b'):
        config=json.loads((root/'test/scenarios/agent'/(node+'.json')).read_text())
        readiness = probe('fuzz-endpoint-ready','--address','127.0.0.1:'+str(config['base_port']))
        record({'event': 'actual-nexus-result-through-temporal-instance', 'node':node, 'result': readiness})
    # The historical mixed40 profile and oracle are reused without changing their
    # frozen counts or semantics. This phase does not count toward ten minutes.
    set_budget(profile['mixed_seconds'])
    command([str(binary), *expanded['mixed_command'][1:]], timeout=profile['mixed_seconds'], cwd=overlay_source)
    probe('mixed-inventory')
    set_budget(profile['verification_seconds'])
    oracle = root / '.local/bin/xenon-omes-oracle'
    command([str(oracle), '--omes-run-id', 'xenon-full-mixed', '--mixed-profile', '--output', str(receipt / 'mixed-histories')], timeout=profile['verification_seconds'])
    set_budget(profile['workload_seconds'] + profile['drain_seconds'])

    def active():
        value = probe('visibility-count', '--query', "TaskQueue = 'omes-xenon-ministack-fuzz' AND ExecutionStatus = 'Running'")
        return value if value.get('count', 0) > 0 else False

    def fault(action):
        observed = wait(active, timeout=30)
        record({'event': 'active-fuzz-before-fault', 'action': action, 'visibility': observed})
        if action == 'join-c':
            before = topology('control-before-c.json')
            agents['c'] = start('c')
            record({'event': 'joined-agent-served-assigned-partition', **wait(lambda: joined_owner(before), timeout=60)})
        elif action == 'kill-b':
            stop(agents['b'], kill=True)
            from agent_control import absent_owner
            node = json.loads((root / 'test/scenarios/agent/b.json').read_text())['service_storage']['node_id']
            wait(lambda: absent_owner(topology('control-after-eviction.json'), node), timeout=45)
        elif action == 'restart-b':
            agents['b'] = start('b', 'b-restarted')
        else:
            raise ValueError('unknown fault')

    def launch_fuzz(index, argv):
        if any(sha(root/path)!=saved['sha256'] for path,saved in expanded['inputs'].items()):
            raise RuntimeError('saved fuzz inputs changed')
        check_build(manifest, build)
        return launch('workload-fuzz-' + str(index), [str(binary), *argv[1:]], cwd=overlay_source)

    report['ten_minute_workload'] = exercise(profile, expanded['fuzz_commands'], launch_fuzz,
                                              check_children, fault, record, finish=stop)
    set_budget(profile['verification_seconds'])
    check_build(manifest, build)
    if sha(build) != report['corrected_omes_manifest_sha256']:
        raise RuntimeError('Omes build manifest changed')
    for node in ('a','b','c'):
        config=json.loads((root/'test/scenarios/agent'/(node+'.json')).read_text())
        result=probe('fuzz-endpoint-ready','--address','127.0.0.1:'+str(config['base_port']))
        record({'event':'post-fault-nexus-result-through-temporal-instance','node':node,'result':result})
    audit_path = receipt / 'fuzz-histories'
    command([str(oracle), '--omes-run-id', 'xenon-ministack-fuzz', '--minimum-runs', '20', '--output', str(audit_path)], timeout=profile['verification_seconds'])
    audit = json.loads((audit_path / 'result.json').read_text())
    report['fuzz_history_oracle'] = audit
    # The oracle verifies every discovered run chain and terminal history. Require
    # real scheduled/completed Nexus events in the persisted histories as well.
    histories = list(audit_path.glob('history-*.json'))
    events = [event for path in histories for event in json.loads(path.read_text())['events']]
    for required in ['nexusOperationScheduledEventAttributes', 'nexusOperationCompletedEventAttributes']:
        if not any(required in event for event in events):
            raise RuntimeError('actual fuzz Nexus history missing: ' + required)
    record({'event': 'fuzz-nexus-histories-verified', 'history_count': len(histories)})
    set_budget(profile['cold_recovery_seconds'])
