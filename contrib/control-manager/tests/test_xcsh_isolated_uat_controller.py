import hashlib, os, subprocess, tarfile, tempfile, unittest
from pathlib import Path
from xcsh_isolated_uat_controller import ControllerError, DisposableHerdrController, IsolatedController

class ControllerTests(unittest.TestCase):
 def test_owned_actions_are_idempotent_and_refuse_default_targets(self):
  with tempfile.TemporaryDirectory() as raw:
   c=IsolatedController(Path(raw)/'actions.sqlite',{'session_id':'xcsh-uat-a','service_id':'xcsh-uat-a','workspace_id':'w1','pane_ids':['p1']},'token')
   c.register_execution({'id':'execution-1','workspace_id':'w1','tab_id':'t1','pane_id':'p1'})
   calls=[]
   def hook(kind,target): calls.append((kind,target)); return {'kind':kind,'session_id':target['session_id'],'pane_id':target['pane_id'],'effect':'isolated'}
   a=c.act('restart_loss','p1','k','token',hook); b=c.act('restart_loss','p1','k','token',hook)
   self.assertEqual(a,b); self.assertEqual(len(calls),1)
   with self.assertRaises(ControllerError): c.act('cleanup','p2','x','token',hook)
   with self.assertRaises(ControllerError): IsolatedController(Path(raw)/'x',{'session_id':'default','service_id':'xcsh-uat-a','pane_ids':['p1']},'token')
   c.close()

 def test_claim_is_written_before_hook_and_conflicting_replay_is_refused(self):
  with tempfile.TemporaryDirectory() as raw:
   db=Path(raw)/'actions.sqlite'; own={'session_id':'xcsh-uat-a','service_id':'xcsh-uat-a','workspace_id':'w1','pane_ids':['p1']}
   c=IsolatedController(db,own,'token'); c.register_execution({'id':'e1','workspace_id':'w1','tab_id':'t1','pane_id':'p1'})
   calls=[]
   def uncertain(kind,target):
    calls.append(kind); raise RuntimeError('simulated post-side-effect crash')
   with self.assertRaisesRegex(RuntimeError,'post-side-effect'): c.act('restart_loss','p1','same','token',uncertain)
   with self.assertRaisesRegex(ControllerError,'uncertain'): c.act('restart_loss','p1','same','token',lambda *_: {})
   self.assertEqual(calls,['restart_loss'])
   with self.assertRaisesRegex(ControllerError,'conflicts'): c.act('reconnect_replay','p1','same','token',lambda *_: {})
   c.close()

 def test_second_controller_cannot_repeat_completed_key(self):
  with tempfile.TemporaryDirectory() as raw:
   db=Path(raw)/'actions.sqlite'; own={'session_id':'xcsh-uat-a','service_id':'xcsh-uat-a','workspace_id':'w1','pane_ids':['p1']}
   first=IsolatedController(db,own,'token'); first.register_execution({'id':'e1','workspace_id':'w1','tab_id':'t1','pane_id':'p1'})
   calls=[]
   def hook(kind,target): calls.append(kind); return {'kind':kind,'session_id':target['session_id'],'pane_id':target['pane_id']}
   one=first.act('reconnect_replay','p1','same','token',hook)
   second=IsolatedController(db,own,'token'); second.register_execution({'id':'e1','workspace_id':'w1','tab_id':'t1','pane_id':'p1'})
   self.assertEqual(second.act('reconnect_replay','p1','same','token',hook),one)
   self.assertEqual(calls,['reconnect_replay']); first.close(); second.close()

 def test_syntactically_valid_wrong_execution_receipt_is_rejected(self):
  with tempfile.TemporaryDirectory() as raw:
   own={'session_id':'xcsh-uat-a','service_id':'xcsh-uat-a','workspace_id':'w1','pane_ids':['p1']}
   c=IsolatedController(Path(raw)/'actions.sqlite',own,'token')
   c.register_execution({'id':'expected','workspace_id':'w1','tab_id':'t1','pane_id':'p1'})
   def wrong(kind,target):
    return {'kind':kind,'session_id':target['session_id'],'service_id':target['service_id'],'workspace_id':'w1','execution_id':'other','tab_id':'t2','pane_id':'p1'}
   with self.assertRaisesRegex(ControllerError,'immutable admitted execution provenance'):
    c.act('restart_loss','p1','wrong-execution','token',wrong)
   c.close()

 def test_real_v21190_binary_probes_documented_json_session_header(self):
  """Real released-binary smoke; it is source evidence, not installed UAT."""
  archive=Path('/data/robin-GIT/xcsh-codex-login/xcsh/.local/native-lifecycle-artifacts/v21.19.0/xcsh-linux-x64.tar.gz')
  if not archive.is_file(): self.skipTest('retained immutable v21.19.0 archive absent')
  with tempfile.TemporaryDirectory() as raw:
   root=Path(raw)
   with tarfile.open(archive,'r:gz') as payload:
    member=payload.getmember('xcsh')
    binary=root/'xcsh'; binary.write_bytes(payload.extractfile(member).read())
   binary.chmod(0o700)
   controller=object.__new__(DisposableHerdrController)
   receipt=controller.probe_xcsh_json_session(binary,hashlib.sha256(binary.read_bytes()).hexdigest(),root,root/'sessions')
   self.assertRegex(receipt['session_id'],r'^[0-9a-f]{16}$')
   self.assertTrue(receipt['json_mode_session_header'])
   # v21.19.0 exposes no prompt-free durable session creation. This is the
   # concrete producer dependency, deliberately not forged by a test shim.
   self.assertFalse(receipt['resume_ready'])
   with self.assertRaisesRegex(ControllerError,'prompt-free durable'):
    controller.create_xcsh_session(binary,hashlib.sha256(binary.read_bytes()).hexdigest(),root,root/'second-session')

 def test_real_disposable_binary_actions_when_explicitly_enabled(self):
  raw_binary=os.environ.get('HERDR_DISPOSABLE_BINARY')
  expected=os.environ.get('HERDR_DISPOSABLE_SHA256')
  if not raw_binary or not expected: self.skipTest('set HERDR_DISPOSABLE_BINARY and HERDR_DISPOSABLE_SHA256 for owned subprocess check')
  with tempfile.TemporaryDirectory(prefix='xcsh-uat-controller-') as raw:
   c=DisposableHerdrController.launch(Path(raw)/'actions.sqlite',Path(raw_binary),Path.cwd(),expected_sha256=expected,expected_version='0.10.1')
   execution_tab=c._owned_call('tab','create','--workspace',c.ownership['workspace_id'],'--cwd',str(Path.cwd()),'--label','xcsh-uat-admitted-fixture','--no-focus')
   tab,root=execution_tab['tab'],execution_tab['root_pane']; pane=root['pane_id']
   c.register_execution({'id':'real-execution','workspace_id':c.ownership['workspace_id'],'tab_id':tab['tab_id'],'pane_id':pane})
   try:
    receipt_path=Path(raw)/'controller-receipt.json'
    saved=c.save_receipt(receipt_path)
    reopened=DisposableHerdrController.open_receipt(Path(raw)/'actions.sqlite',receipt_path)
    self.assertEqual(saved['workspace_id'],reopened.ownership['workspace_id'])
    reopened.close()
    reconnect=c.real_action('reconnect_replay',pane,'real-reconnect',c.token)
    restart=c.real_action('restart_loss',pane,'real-restart',c.token)
    self.assertEqual(c.real_action('restart_loss',pane,'real-restart',c.token),restart)
    self.assertTrue(reconnect['effect']['reconnected'])
    with self.assertRaises(ControllerError): c.real_action('cleanup',pane,'real-cleanup',c.token)
    self.assertEqual(restart['effect']['server_version'],'0.10.1')
   finally:
    subprocess.run([raw_binary,'--session',c.ownership['session_id'],'server','stop'],check=False,capture_output=True,text=True)
    c.close()
