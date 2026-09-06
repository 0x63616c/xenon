#!/usr/bin/env python3
"""Install the hash-pinned UI Node runtime in this checkout's disposable directory."""
import hashlib
import json
from pathlib import Path
import platform
import tempfile
import tarfile
import urllib.request
ROOT=Path(__file__).resolve().parents[1]
def install(archive, destination, name):
    # Replacing signed executable bytes in place can leave macOS executing a
    # stale cached inode. Extract fresh inodes, including on repeated installs.
    with tempfile.TemporaryDirectory(prefix='node-install-', dir=destination) as temporary:
        staging=Path(temporary)
        with tarfile.open(archive) as source:source.extractall(staging,filter='data')
        candidate=staging/name
        if not (candidate/'bin/node').is_file():raise ValueError('Node executable missing')
        target=destination/name
        retired=staging/'previous'
        if target.exists():target.rename(retired)
        try:candidate.rename(target)
        except BaseException:
            if retired.exists():retired.rename(target)
            raise
    return target/'bin'

def prepare():
    pins=json.loads((ROOT/'test/scenarios/ministack/pins.json').read_text())['runtime_tools']
    os_name={'Darwin':'darwin','Linux':'linux'}[platform.system()]
    arch={'arm64':'arm64','aarch64':'arm64','x86_64':'x64','AMD64':'x64'}[platform.machine()]
    name='node-'+pins['node']+'-'+os_name+'-'+arch
    archive=ROOT/'.local/node-downloads'/(name+'.tar.gz');archive.parent.mkdir(parents=True,exist_ok=True)
    if not archive.exists():
        with urllib.request.urlopen('https://nodejs.org/dist/'+pins['node']+'/'+archive.name,timeout=60) as response:
            archive.write_bytes(response.read())
    if hashlib.sha256(archive.read_bytes()).hexdigest()!=pins['node_archive_sha256'][os_name+'-'+arch]:
        raise ValueError('Node archive SHA256 mismatch')
    return install(archive, ROOT/'.local', name)
if __name__=='__main__':print(prepare())
