"""Strict evidence binding for the frozen throughput_stress invocation."""
import json
import re
from pathlib import Path
from omes_workloads import sha
from validate_acceptance import validate

SUMMARY = re.compile(r'\[Scenario completion summary\] Run ID: xenon-full-mixed, Total iterations completed: (\d+), Total child workflows: (\d+) \((\d+) per iteration\), Total continue-as-new workflows: (\d+) \((\d+) per iteration\), Total workflows completed: (\d+)')

def validate_result(root, evidence):
    expected=validate(root)['workloads']['mixed']
    report=json.loads((evidence/'result.json').read_text())
    if report.get('stage')!='mixed' or report.get('profile')!='without-faults' or not report.get('workload_completed') or report.get('full_acceptance') is not False:
        raise ValueError('mixed workload did not complete its declared component')
    commands=report.get('commands',[])
    if len(commands)!=1 or commands[0]['argv'][1:]!=expected[1:] or commands[0]['timeout_seconds']!=900 or commands[0]['exit_code']!=0 or commands[0]['timed_out'] or commands[0].get('interrupted'):
        raise ValueError('mixed invocation differs from frozen command or failed')
    log=evidence/commands[0]['log']
    if log.parent.resolve()!=evidence.resolve() or sha(log)!=commands[0]['log_sha256']:
        raise ValueError('mixed log missing or modified')
    matches=SUMMARY.findall(log.read_text())
    if len(matches)!=1 or tuple(map(int,matches[0]))!=(40,160,4,40,1,240):
        raise ValueError('missing, duplicate or incomplete upstream mixed summary')
    return {'upstream_reported_iterations':40,'upstream_reported_children':160,'upstream_reported_continue_as_new':40,'upstream_reported_total_workflows':240,
            'summary_source':'pinned Omes completion log; not independent history verification',
            'log_sha256':sha(log),'result_sha256':sha(evidence/'result.json'),'full_history_action_oracle':'NOT_EXECUTED','faults_injected':False}
