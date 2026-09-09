import json,tempfile,unittest
from pathlib import Path
from unittest.mock import AsyncMock,patch
import appserver_manager as manager
from control_supervisor import Supervisor

class RepairOrder(unittest.IsolatedAsyncioTestCase):
 async def test_failed_early_observer_reconnect_can_be_repaired_by_appserver_restart(self):
  with tempfile.TemporaryDirectory() as raw:
   root=Path(raw);cfg=root/'cfg';cfg.write_text(json.dumps({'supervisor_mode':'isolated_active'}));s=Supervisor(root/'s',root/'db',cfg)
   before={'state':'unavailable','components':[{'component':c,'status':'unavailable','reason':'FileNotFoundError: socket missing'} for c in ['observer','app_server']],'authoritative':{}}
   after={**before,'components':[dict(v,status='healthy') for v in before['components']]}
   async def action(c,a,name):return {'state':'failed' if name=='observer_reconnect' else 'completed'}
   try:
    with patch.object(s,'check',AsyncMock(side_effect=[before,before,after])),patch.object(s,'_run_action',action):r=await s.recover('all')
    self.assertEqual(r['state'],'completed');self.assertEqual(r['outcome']['observer_reconnect']['state'],'failed');self.assertEqual(r['outcome']['restart:app_server']['state'],'completed')
   finally:s.db.conn.close()
 def test_observer_cannot_publish_canonical_identity_into_foreign_or_unbound_pane(self):
  with tempfile.TemporaryDirectory() as raw:
   cfg=Path(raw)/'cfg';cfg.write_text(json.dumps({'manager_pane_id':'p','manager_thread_id':'canonical'}))
   for identity in ['foreign',None,'canonical']:
    calls=[]
    def rpc(method,params):
     calls.append(method)
     if method=='agent.get':return {'agent':{'agent':'codex','agent_session':{'value':identity}}}
     return {}
    with patch.object(manager,'CONFIG_PATH',cfg),patch.object(manager,'herdr_request',rpc):manager.report_manager_lifecycle('p','canonical','idle','fixture',1)
    self.assertEqual('pane.report_agent' in calls,identity=='canonical')
if __name__=='__main__':unittest.main()
