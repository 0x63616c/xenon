import json
from pathlib import Path
import subprocess
import tempfile
import unittest
import temporal_upgrade as upgrade

class UpgradeTests(unittest.TestCase):
    def test_changed_contracts_and_unmapped_paths(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); repo=root/'temporal';repo.mkdir()
            def git(*args):
                return subprocess.check_output(['git','-C',str(repo),*args],stderr=subprocess.PIPE).decode().strip()
            git('init');git('config','user.name','Fixture');git('config','user.email','fixture@example.invalid')
            (repo/'README').write_text('old');git('add','.');git('commit','-m','old');old=git('rev-parse','HEAD')
            for path in ['common/persistence/data_interfaces.go','schema/postgresql/v12/temporal/versioned/v2/new.sql','api/persistence/v1/state.pb.go','unclassified/behavior.go']:
                p=repo/path;p.parent.mkdir(parents=True,exist_ok=True);p.write_text('changed')
            git('add','.');git('commit','-m','new');new=git('rev-parse','HEAD')
            report=upgrade.impact(repo,old,new,root/'report')
            areas={i['area'] for c in report['changes'] for i in c['impacts']}
            self.assertTrue({'persistence','schema','serialization'}<=areas)
            self.assertTrue(next(c for c in report['changes'] if c['path']=='unclassified/behavior.go')['unmapped'])
            self.assertEqual(report['fresh_install'],'NOT_TESTED');self.assertEqual(report['existing_state_upgrade'],'NOT_TESTED')
            self.assertEqual(report['patch_sha256'],upgrade.sha((root/'report/temporal.diff').read_bytes()))
            with self.assertRaises(FileExistsError):upgrade.impact(repo,old,new,root/'report')
            with self.assertRaises(ValueError):upgrade.impact(repo,old,new,repo/'output')
            with self.assertRaises(subprocess.CalledProcessError):upgrade.impact(repo,old,'missing-ref',root/'invalid-report')
            (repo/'README').write_text('tracked dirty')
            with self.assertRaises(ValueError):upgrade.impact(repo,old,new,root/'tracked-dirty-report')
            git('checkout','--','README')
            (repo/'untracked').write_text('dirty')
            with self.assertRaises(ValueError):upgrade.impact(repo,old,new,root/'dirty-report')
            self.assertFalse((root/'dirty-report').exists())

if __name__=='__main__':unittest.main()
