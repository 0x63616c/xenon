"""Repeated installation replaces executable inodes and preserves old readers."""
import importlib.util
import io
from pathlib import Path
import tarfile
import tempfile
import unittest

spec=importlib.util.spec_from_file_location('ministack_node',Path(__file__).with_name('ministack-node.py'))
node=importlib.util.module_from_spec(spec)
spec.loader.exec_module(node)

class Installation(unittest.TestCase):
    def test_repeat_install_replaces_inode(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); archive=root/'node.tar.gz'
            with tarfile.open(archive,'w:gz') as output:
                entry=tarfile.TarInfo('node-test/bin/node'); entry.size=3; entry.mode=0o755
                output.addfile(entry,io.BytesIO(b'new'))
            binary=node.install(archive,root,'node-test')/'node'
            binary.write_bytes(b'old')
            with binary.open('rb') as existing:
                previous=binary.stat().st_ino
                replacement=node.install(archive,root,'node-test')/'node'
                self.assertNotEqual(previous,replacement.stat().st_ino)
                self.assertEqual(replacement.read_bytes(),b'new')
                self.assertEqual(existing.read(),b'old')
            self.assertFalse(list(root.glob('node-install-*')))

if __name__=='__main__':unittest.main()
