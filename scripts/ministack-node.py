#!/usr/bin/env python3
"""Install the hash-pinned UI Node runtime in this checkout's disposable directory."""
import hashlib
import json
from pathlib import Path
import platform
import tarfile
import urllib.request
ROOT=Path(__file__).resolve().parents[1]
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
    with tarfile.open(archive) as source:source.extractall(ROOT/'.local',filter='data')
    return ROOT/'.local'/name/'bin'
if __name__=='__main__':print(prepare())
