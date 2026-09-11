import json,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import appserver_manager as manager

class MetadataAdmission(unittest.TestCase):
 def test_only_exact_idle_remote_client_can_request_identity_repair(self):
  with tempfile.TemporaryDirectory() as raw:
   root=Path(raw);cfg=root/'cfg';config={'manager_thread_id':'canonical','manager_pane_id':'p1','codex_binary':sys.executable,'app_server_remote':'unix://fixture','profile':'control-manager','manager_cwd':str(root)};cfg.write_text(json.dumps(config));argv=[str(Path(sys.executable).resolve()),'--disable','hooks','--disable','goals','--remote','unix://fixture','-C',str(root),'resume','canonical']
   for status,tail,expected in [('idle','canonical','unavailable'),('idle','foreign','degraded'),('working','canonical','waiting_user')]:
    calls=[]
    def rpc(method,params):
     calls.append(method)
     if method=='agent.get':return {'agent':{'agent':'codex','agent_status':status}}
     if method=='pane.process_info':return {'process_info':{'foreground_processes':[{'name':'codex','argv':argv[:-1]+[tail]}]}}
     self.fail('health probe attempted mutation: '+method)
    with self.subTest(status=status,tail=tail),patch.object(manager,'CONFIG_PATH',cfg),patch.object(manager,'herdr_request',rpc):
     self.assertEqual(manager.native_pane_health('canonical')['state'],expected)
     self.assertTrue(set(calls)<={'agent.get','pane.process_info'})
if __name__=='__main__':unittest.main()
