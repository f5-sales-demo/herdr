import json,tempfile,time,unittest
from pathlib import Path
from unittest.mock import AsyncMock,patch
from control_supervisor import Supervisor

class LiveCooldown(unittest.IsolatedAsyncioTestCase):
 async def test_old_aggregate_cooldown_cannot_crash_or_block_another_component(self):
  with tempfile.TemporaryDirectory() as raw:
   root=Path(raw);cfg=root/'cfg';cfg.write_text(json.dumps({'supervisor_mode':'isolated_active'}));s=Supervisor(root/'sock',root/'db',cfg)
   s.db.conn.execute('INSERT INTO recovery_settings VALUES(?,?)',('cooldown:all',str(time.time()+900)));s.db.conn.commit()
   before={'state':'unavailable','components':[{'component':'app_server','status':'unavailable','reason':'FileNotFoundError: socket missing'}],'authoritative':{}}
   healthy={'state':'healthy','components':[{'component':'app_server','status':'healthy','reason':'RPC verified'}],'authoritative':{}}
   calls=[]
   async def action(c,a,name):
    calls.append(name)
    if name=='restart_app_server':s.db.reserve_restart('app_server',a['action_id'])
    return {'state':'completed'}
   try:
    with patch.object(s,'check',AsyncMock(side_effect=[before,before,healthy])),patch.object(s,'_run_action',action):
     result=await s.automatic_recover(before)
    self.assertEqual(result['state'],'completed');self.assertIn('restart_app_server',calls)
    self.assertEqual(s.db.conn.execute('SELECT count(*) FROM recovery_restart_attempts WHERE component=?',('app_server',)).fetchone()[0],1)
    self.assertTrue(s.status())
   finally:s.db.conn.close()
 async def test_exhausted_component_restart_budget_remains_blocked_without_exception(self):
  with tempfile.TemporaryDirectory() as raw:
   root=Path(raw);cfg=root/'cfg';cfg.write_text(json.dumps({'supervisor_mode':'isolated_active','recovery_commands':{'restart_broker':['/bin/true']}}));s=Supervisor(root/'sock',root/'db',cfg)
   try:
    for i in range(3):s.db.reserve_restart('broker',str(i))
    result=await s._run_action(s.bindings(),{'action_id':'fourth'},'restart_broker')
    self.assertEqual(result['state'],'blocked');self.assertIn('cooldown',result['reason'])
    s.db.reserve_restart('app_server','independent')
    self.assertTrue(s.status())
   finally:s.db.conn.close()
if __name__=='__main__':unittest.main()
