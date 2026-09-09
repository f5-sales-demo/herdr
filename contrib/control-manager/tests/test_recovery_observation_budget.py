"""Observation must not spend durable recovery claims before live activation."""
import json
import tempfile
import unittest
from pathlib import Path
from control_supervisor import Supervisor


class ObservationBudgetAcceptance(unittest.IsolatedAsyncioTestCase):
    async def test_inactive_modes_do_not_admit_automatic_recovery(self):
        for config in ({}, {'supervisor_mode':'observation_only'},
                       {'supervisor_mode':'guarded_live','recovery_live_enabled':False}):
            with self.subTest(config=config),tempfile.TemporaryDirectory(prefix='control-observation-budget-') as raw:
                root=Path(raw); path=root/'machine.json'; path.write_text(json.dumps(config))
                supervisor=Supervisor(root/'recovery.sock',root/'state.db',path)
                try:
                    result=await supervisor.automatic_recover({'components':[{
                        'component':'broker','status':'unavailable'}]})
                    self.assertIsNone(result)
                    self.assertEqual(supervisor.db.status()['recent_outcomes'],[])
                finally: supervisor.db.conn.close()


if __name__=='__main__': unittest.main()
