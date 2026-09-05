"""Bounded host process CPU-time/RSS samples for declared proof process trees.

These are interval samples, not exact memory peaks or container resource metrics.
Only numeric process metadata and start identities are retained, never commands.
"""
import json
import subprocess
import threading
import time


def cpu_seconds(value):
    days = 0
    if '-' in value:
        day, value = value.split('-', 1)
        days = int(day)
    parts = value.split(':')
    if len(parts) not in (2, 3):
        raise ValueError('invalid process CPU time')
    seconds = float(parts[-1]) + int(parts[-2]) * 60
    if len(parts) == 3:
        seconds += int(parts[0]) * 3600
    return days * 86400 + seconds


def process_table():
    result = subprocess.run(
        ['ps', '-axo', 'pid=,ppid=,time=,rss=,lstart='],
        capture_output=True, text=True, check=True, timeout=5,
    )
    rows = {}
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) != 9:
            raise ValueError('unexpected process metadata columns')
        pid, parent = int(fields[0]), int(fields[1])
        rows[pid] = {
            'pid': pid, 'ppid': parent, 'cpu_seconds': cpu_seconds(fields[2]),
            'rss_bytes': int(fields[3]) * 1024, 'started': ' '.join(fields[4:]),
        }
    return rows


def owned_rows(table, roots):
    """Roots bind PID plus observed start time; changed start identities are excluded."""
    selected = {}
    for name, identity in roots.items():
        row = table.get(identity['pid'])
        if row and row['started'] == identity['started']:
            selected[row['pid']] = name
    changed = True
    while changed:
        changed = False
        for pid, row in table.items():
            if pid not in selected and row['ppid'] in selected:
                selected[pid] = selected[row['ppid']]
                changed = True
    return [dict(table[pid], root=selected[pid]) for pid in sorted(selected)]


class ProcessSampler:
    def __init__(self, path, interval=1.0, max_samples=7200, max_processes=256):
        if interval < 0.05 or max_samples < 1 or max_processes < 1:
            raise ValueError('invalid resource sampling bounds')
        self.interval = interval
        self.max_samples = max_samples
        self.max_processes = max_processes
        self.roots = {}
        self.lock = threading.Lock()
        self.done = threading.Event()
        self.file = open(path, 'x')
        self.samples = 0
        self.error = None
        self.started = time.monotonic()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def register(self, name, pid):
        row = process_table().get(pid)
        if row is None:
            raise RuntimeError('proof process exited before resource registration')
        with self.lock:
            if name in self.roots:
                raise ValueError('resource label must identify one process incarnation')
            if len(self.roots) >= self.max_processes:
                raise ValueError('resource root capacity exhausted')
            self.roots[name] = {'pid': pid, 'started': row['started']}

    def _run(self):
        try:
            while not self.done.is_set():
                if self.samples >= self.max_samples:
                    raise RuntimeError('resource sample capacity exhausted')
                with self.lock:
                    roots = dict(self.roots)
                rows = owned_rows(process_table(), roots)
                if len(rows) > self.max_processes:
                    raise RuntimeError('resource process capacity exhausted')
                self.file.write(json.dumps({
                    'kind': 'host-process-sample', 'elapsed_seconds': time.monotonic() - self.started,
                    'unix_seconds': time.time(), 'processes': rows,
                }) + '\n')
                self.file.flush()
                self.samples += 1
                self.done.wait(self.interval)
        except Exception as error:
            self.error = type(error).__name__ + ': ' + str(error)

    def stop(self):
        self.done.set()
        self.thread.join(timeout=6)
        if self.thread.is_alive():
            raise RuntimeError('resource sampler did not stop')
        if self.file.closed:
            return self.summary
        self.summary = {
            'kind': 'host-process-summary', 'samples': self.samples,
            'interval_seconds': self.interval, 'max_samples': self.max_samples,
            'max_processes': self.max_processes, 'complete': self.error is None,
            'error': self.error, 'scope': 'host process trees; sampled RSS and cumulative CPU time',
            'limitations': ['Short-lived processes and between-sample memory peaks may be missed.',
                            'Container CPU and memory are not included.',
                            'ps start identity has second precision.'],
        }
        self.file.write(json.dumps(self.summary) + '\n')
        self.file.close()
        return self.summary
