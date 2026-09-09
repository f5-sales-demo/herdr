import hashlib, os, subprocess, tempfile, unittest
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
   with self.assertRaisesRegex(ControllerError,'conflicts'): c.act('cleanup','p1','same','token',lambda *_: {})
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

 def test_measured_xcsh_session_and_capability_receipts_are_required(self):
  """Synthetic controller fixture; it labels command receipts, never UAT proof."""
  with tempfile.TemporaryDirectory() as raw:
   binary=Path(raw)/'xcsh'
   binary.write_text('#!/bin/sh\ncase "$1" in --session-json) echo \'{"session_id":"123e4567-e89b-12d3-a456-426614174000"}\' ;; --caps-json) echo \'{"capabilities":["transport_replay"]}\' ;; *) exit 2 ;; esac\n')
   binary.chmod(0o700)
   digest=hashlib.sha256(binary.read_bytes()).hexdigest()
   controller=object.__new__(DisposableHerdrController)
   self.assertEqual(controller.create_xcsh_session(binary,digest,[str(binary),'--session-json'],Path(raw)),'123e4567-e89b-12d3-a456-426614174000')
   self.assertEqual(controller.probe_xcsh_capabilities(binary,digest,[str(binary),'--caps-json'],Path(raw)),{'transport_replay'})
   with self.assertRaisesRegex(ControllerError,'measured'):
    controller.probe_xcsh_capabilities(binary,'0'*64,[str(binary),'--caps-json'],Path(raw))

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
    cleanup=c.real_action('cleanup',pane,'real-cleanup',c.token)
    self.assertTrue(reconnect['effect']['reconnected'])
    self.assertEqual(cleanup['effect']['closed_execution_id'],'real-execution')
    self.assertEqual(restart['effect']['server_version'],'0.10.1')
   finally:
    subprocess.run([raw_binary,'--session',c.ownership['session_id'],'server','stop'],check=False,capture_output=True,text=True)
    c.close()
