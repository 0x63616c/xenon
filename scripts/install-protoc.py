#!/usr/bin/env python3
"""Install the checksum-pinned official compiler into the ignored local tools directory."""
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[1]

def main():
    pins = json.loads((ROOT / "tools/protoc.json").read_text())
    target = platform.system() + "-" + platform.machine()
    if target not in pins["assets"]:
        raise SystemExit(f"no verified protoc asset for {target}")
    asset = pins["assets"][target]
    destination = ROOT / ".local/protoc"
    destination.mkdir(parents=True, exist_ok=True)
    archive = destination / (target + ".zip")
    if not archive.exists():
        with urllib.request.urlopen(asset["url"], timeout=60) as response:
            data = response.read()
        if hashlib.sha256(data).hexdigest() != asset["sha256"]:
            raise SystemExit("downloaded protoc archive checksum mismatch")
        archive.write_bytes(data)
    if hashlib.sha256(archive.read_bytes()).hexdigest() != asset["sha256"]:
        raise SystemExit("cached protoc archive checksum mismatch")
    with zipfile.ZipFile(archive) as source:
        for member in source.infolist():
            if not (destination / member.filename).resolve().is_relative_to(destination.resolve()):
                raise SystemExit("invalid compiler archive path")
        source.extractall(destination)
    binary = destination / "bin/protoc"
    binary.chmod(0o755)
    version = subprocess.check_output([str(binary), "--version"], text=True).strip()
    if version != "libprotoc " + pins["version"]:
        raise SystemExit("compiler version mismatch")
    print(binary.parent)

if __name__ == "__main__":
    main()
