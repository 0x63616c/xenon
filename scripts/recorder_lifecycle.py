"""Proof-only static fault-phase recorder supervision; no application durability."""
import hashlib
import json
from pathlib import Path
import signal
import subprocess
import urllib.request
import uuid

EXPECTED = {'schema': 1, 'phase': 'runtime-fault', 'phase_type': 'fault',
            'capacity': 1000000, 'http_timeout_seconds': 3,
            'validator_timeout_seconds': 10, 'steady_gate_executed': False,
            'full_acceptance': False}


def config(path):
    value = json.loads(Path(path).read_text())
    if value != EXPECTED:
        raise ValueError('unregistered recorder lifecycle configuration')
    return value


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("recorder redirect rejected")


class RecorderLifecycle:
    def __init__(self, evidence, binary, launch, settings):
        if settings != EXPECTED:
            raise ValueError('unregistered recorder lifecycle settings')
        self.evidence, self.binary, self.settings = Path(evidence), str(binary), settings
        self.journal = self.evidence / 'rpc-registrations.jsonl'
        self.errors, self.producers = [], {}
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        self.process = launch('rpc-recorder', [self.binary, '--journal', str(self.journal), '--capacity', str(settings['capacity'])])
        try:
            ready = json.loads(self.process.line('{', timeout=10))
            if ready.get('event') != 'ready' or not ready.get('address', '').startswith('127.0.0.1:'):
                raise ValueError('invalid recorder readiness')
            self.url = 'http://' + ready['address']
            self.post({'kind': 'open', 'phase': settings['phase'], 'status': 'fault'})
        except BaseException as original:
            try:
                self.process.stop()
            except Exception as cleanup:
                original.add_note("recorder startup cleanup failed: " + type(cleanup).__name__)
            raise

    def post(self, event):
        try:
            request = urllib.request.Request(self.url + '/event', json.dumps(event).encode(), {'Content-Type': 'application/json'})
            with self.opener.open(request, timeout=self.settings['http_timeout_seconds']) as response:
                if response.status != 204:
                    raise ValueError('recorder rejected controller event')
        except Exception as error:
            self.errors.append(type(error).__name__)
            raise

    def environment(self, name):
        if name in self.producers:
            raise ValueError('producer name reused')
        identity = name + '-' + uuid.uuid4().hex
        self.producers[name] = {'incarnation': identity, 'process': None, 'declared_kill': False}
        return {'XENON_RPC_RECORDER_URL': self.url, 'XENON_RPC_RECORDER_PRODUCER': identity,
                'XENON_RPC_RECORDER_PHASE': self.settings['phase']}

    def bind(self, name, process):
        self.producers[name]['process'] = process

    def declare_kill(self, process):
        found = [v for v in self.producers.values() if v['process'] is process]
        if len(found) != 1 or process.process.poll() is not None:
            raise ValueError('declared kill must name one live producer')
        found[0]['declared_kill'] = True

    def finalize(self):
        report = {'enabled': True, 'scope': 'static declared fault phase; registration census only',
                  'registration_census_complete': False, 'steady_gate_executed': False,
                  'full_acceptance': False, 'fault_latency_distribution_complete': False, 'producers': {}}
        try:
            if not self.producers:
                raise ValueError('no registered Temporal producers')
            for name, value in self.producers.items():
                process = value['process']
                if process is None or process.process.poll() is None:
                    raise ValueError('producer not bound or still running')
                code = process.process.returncode
                report['producers'][name] = {'incarnation': value['incarnation'], 'pid': process.process.pid,
                                              'exit_code': code, 'declared_kill': value['declared_kill']}
                if code != 0:
                    self.post({'kind': 'death', 'producer': value['incarnation']})
                    if code != -signal.SIGKILL or not value['declared_kill']:
                        self.errors.append('unexpected_producer_exit')
                else:
                    markers = sum(line == 'TEMPORAL_TRACE_CLOSED' for line in (self.evidence / (name + '.log')).read_text().splitlines())
                    if markers != 1 or value['declared_kill']:
                        self.errors.append('missing_graceful_close_or_kill')
            self.post({'kind': 'close', 'phase': self.settings['phase']})
            self.process.stop()
            if self.process.process.returncode != 0:
                raise ValueError('recorder did not finalize successfully')
            # A validator is a separate bounded child process; no shell or inherited
            # request data, and no source/application mutation.
            result = subprocess.run([self.binary, '--validate', '--journal', str(self.journal)],
                                    capture_output=True, text=True, timeout=self.settings['validator_timeout_seconds'])
            if result.returncode:
                raise ValueError('recorder offline validation failed')
            summary = json.loads(result.stdout)
            if summary.get('census_complete') is not True or not summary.get('registered'):
                raise ValueError('empty or incomplete registration census')
            report['census'] = summary
            report['registration_census_complete'] = not self.errors
        except Exception as error:
            self.errors.append(type(error).__name__)
        finally:
            try:
                self.process.stop()
            except Exception as error:
                self.errors.append(type(error).__name__)
            try:
                if self.journal.is_file():
                    digest = hashlib.sha256()
                    with self.journal.open('rb') as stream:
                        for block in iter(lambda: stream.read(1024 * 1024), b''):
                            digest.update(block)
                    report['journal_sha256'] = digest.hexdigest()
            except Exception as error:
                self.errors.append('journal_hash_' + type(error).__name__)
            report['errors'] = list(self.errors)
            if self.errors:
                report['registration_census_complete'] = False
            try:
                (self.evidence / 'recorder-result.json').write_text(json.dumps(report, indent=2) + '\n')
            except Exception as error:
                self.errors.append('receipt_write_' + type(error).__name__)
                report['errors'] = list(self.errors)
                report['registration_census_complete'] = False
                # The caller embeds this receipt in the independent runtime result.
                # A broken evidence filesystem cannot be repaired by another write.
        return report
