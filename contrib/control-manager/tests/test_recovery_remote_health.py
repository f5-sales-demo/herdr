import json,tempfile,unittest
from pathlib import Path
from unittest.mock import AsyncMock,patch
from appserver_manager import probe
from control_supervisor import Supervisor,REQUIRED_TOOLS

class RemoteHealth(unittest.IsolatedAsyncioTestCase):
 async def test_remote_outage_never_causes_local_restart(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);cfg=root/'config.json';cfg.write_text(json.dumps({'supervisor_mode':'isolated_active','observer_command':['fixture']}));s=Supervisor(root/'s',root/'db',cfg)
   s.db.observe_state('manager_observer','degraded','old optional row')
   try:
    for state in ['connected','connecting','errored','disabled','unknown']:
     app={'thread':{'id':'same','status':'idle'},'turn':{'status':'completed'},'control_broker':{'runtime_status':'connected','tools':list(REQUIRED_TOOLS)},'remote_control':{'status':state}}
     with patch('control_supervisor.unix_probe',AsyncMock(return_value=(True,'responsive'))),patch('control_supervisor.appserver_probe',AsyncMock(return_value=(True,'responsive',app))),patch.object(s,'_observer_status',AsyncMock(return_value=(True,'ready'))),patch.object(s,'_run_action',AsyncMock()) as action:
      h=await s.check();by={v['component']:v for v in h['components']};self.assertEqual(by['remote_control']['status'],'healthy' if state=='connected' else 'degraded');self.assertEqual(by['app_server']['status'],'healthy');self.assertNotIn('manager_observer',s.status()['health']);await s.automatic_recover(h);action.assert_not_called()
   finally:s.db.conn.close()
 def test_status_rpc_failure_preserves_probe_and_discards_identifiers(self):
  class Server:
   def request(self,method,params):
    if method=='thread/read':return {'thread':{'id':'same'}}
    if method=='mcpServerStatus/list':return {'data':[]}
    if method=='remoteControl/status/read':raise RuntimeError('unsupported method')
  result=probe(Server(),'same');self.assertEqual(result['thread']['id'],'same');self.assertEqual(result['remote_control']['status'],'unknown')

if __name__=='__main__':unittest.main()
