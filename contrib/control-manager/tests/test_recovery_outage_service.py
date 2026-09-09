"""A sustained component outage must not crash the owner or its control socket."""
import asyncio,json,tempfile,unittest
from pathlib import Path
from unittest.mock import AsyncMock,patch
from control_supervisor import Supervisor,REQUIRED_TOOLS

class OutageService(unittest.IsolatedAsyncioTestCase):
 async def test_control_socket_survives_exhausted_component_restart_budget(self):
  with tempfile.TemporaryDirectory() as raw:
   root=Path(raw);cfg=root/'cfg';cfg.write_text(json.dumps({'supervisor_mode':'isolated_active','broker_socket':str(root/'missing-broker'),'herdr_socket':str(root/'missing-herdr'),'recovery_commands':{'restart_broker':['/bin/false'],'restart_herdr':['/bin/false']}}));s=Supervisor(root/'sock',root/'db',cfg)
   app={'thread':{'id':'fixture','status':'idle'},'turn':{'status':'completed'},'control_broker':{'runtime_status':'connected','tools':list(REQUIRED_TOOLS)}}
   # RPC transport is independently exercised elsewhere. Here use a healthy
   # app-server adapter and real missing Unix sockets and subprocess failures.
   with patch('control_supervisor.INTERVAL',.02),patch('control_supervisor.appserver_probe',AsyncMock(return_value=(True,'read succeeded',app))):
    service=asyncio.create_task(s.serve())
    try:
     deadline=asyncio.get_running_loop().time()+10
     while asyncio.get_running_loop().time()<deadline:
      if service.done():await service
      cooldown=s.db.conn.execute("SELECT value FROM recovery_settings WHERE key='restart_cooldown:broker'").fetchone()
      if cooldown:break
      await asyncio.sleep(.05)
     else:self.fail('component budget never exhausted')
     reader,writer=await asyncio.open_unix_connection(str(root/'sock'),limit=1_000_000);writer.write(b'{"method":"status"}\n');await writer.drain();reply=json.loads(await asyncio.wait_for(reader.readline(),1));writer.close();await writer.wait_closed()
     self.assertTrue(reply['ok']);self.assertFalse(service.done());self.assertEqual(s.db.conn.execute("SELECT count(*) FROM recovery_restart_attempts WHERE component='broker'").fetchone()[0],3)
    finally:
     s.stop.set();await asyncio.wait_for(service,3);s.db.conn.close()
if __name__=='__main__':unittest.main()
