"""A second process may not mark another live supervisor's action uncertain."""
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from control_supervisor import Supervisor


CHILD = r'''
import json, sys
from pathlib import Path
from control_supervisor import Supervisor
root=Path(sys.argv[1])
try:
    candidate=Supervisor(root/'second.sock',root/'recovery.sqlite3',root/'config.json')
except RuntimeError as exc:
    print(json.dumps({'rejected':True,'reason':str(exc)}))
else:
    print(json.dumps({'rejected':False,'reconciled':candidate.interrupted_actions}))
'''


class InstanceOwnershipAcceptance(unittest.TestCase):
    def test_same_database_with_different_socket_cannot_steal_active_claim(self):
        with tempfile.TemporaryDirectory(prefix='control-instance-ownership-') as raw:
            root=Path(raw); (root/'config.json').write_text('{}')
            owner=Supervisor(root/'first.sock',root/'recovery.sqlite3',root/'config.json')
            action=owner.db.claim('broker','restart','isolated fixture')
            try:
                proc=subprocess.run([sys.executable,'-c',CHILD,raw],
                                    cwd=Path(__file__).resolve().parent,
                                    capture_output=True,text=True,timeout=10)
                self.assertEqual(proc.returncode,0,proc.stderr)
                result=json.loads(proc.stdout)
                self.assertTrue(result['rejected'],result)
                rows=owner.db.status()['recent_outcomes']
                same=next(x for x in rows if x['action_id']==action['action_id'])
                self.assertIn(same['state'],{'claimed','recovering'})
            finally:
                owner.db.close() if hasattr(owner.db,'close') else owner.db.conn.close()
                import os
                os.close(owner._lock_fd)


if __name__ == '__main__': unittest.main()
