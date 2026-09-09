import asyncio,json,os,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import AsyncMock,patch
from control_supervisor import Supervisor,bounded_process

class HelperFailures(unittest.IsolatedAsyncioTestCase):
 async def test_descendant_holding_stdout_cannot_escape_whole_helper_deadline(self):
  # Leader exits, descendant retains both output pipes. Bound the entire
  # exchange, including EOF, and reap our own process group on expiry.
  code='import os,time; p=os.fork(); os._exit(0) if p else time.sleep(30)'
  start=asyncio.get_running_loop().time()
  with self.assertRaisesRegex(RuntimeError,'bounded deadline'):
   await asyncio.wait_for(bounded_process([sys.executable,'-c',code],os.environ.copy(),timeout=.15),4)
  self.assertLess(asyncio.get_running_loop().time()-start,3)
 async def test_actual_timed_out_restart_keeps_supervisor_responsive_and_records_uncertainty(self):
  with tempfile.TemporaryDirectory() as raw:
   root=Path(raw);cfg=root/'cfg';cfg.write_text(json.dumps({'supervisor_mode':'isolated_active','recovery_action_timeout':.1,'recovery_commands':{'restart_broker':[sys.executable,'-c','import time; time.sleep(30)']}}));s=Supervisor(root/'s',root/'db',cfg)
   check={'state':'unavailable','components':[{'component':'broker','status':'unavailable','reason':'ConnectionRefusedError: socket unavailable'}],'authoritative':{}}
   original=s._run_action
   async def action(c,a,name):
    if name=='observer_reconnect':return {'state':'completed'}
    return await original(c,a,name)
   try:
    with patch.object(s,'check',AsyncMock(return_value=check)),patch.object(s,'_run_action',action):r=await s.automatic_recover(check)
    self.assertEqual(r['state'],'uncertain');self.assertIn('bounded deadline',r['outcome']['reason']);self.assertFalse(s.status()['active_actions']);self.assertEqual(s.status()['history'][0]['state'],'uncertain')
   finally:s.db.conn.close()
if __name__=='__main__':unittest.main()
