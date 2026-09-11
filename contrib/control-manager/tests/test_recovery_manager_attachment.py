"""Durable replacement of a proven-lost Control Manager terminal binding."""
from __future__ import annotations

import json
import tempfile
import unittest
import uuid
from pathlib import Path

from control_supervisor import RecoveryDB
from test_control_broker import TestBroker


THREAD="01a07cad-d970-7393-82a4-14ae9a1c16ee"


class ManagerAttachmentRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix="control-manager-attachment-")
        self.root=Path(self.tmp.name)
        self.broker=TestBroker(self.root)
        self.supervisor_db=RecoveryDB(self.root/"supervisor.sqlite3")
        created=await self.broker.herdr.request("workspace.create",{"cwd":str(self.root),"label":"control"})
        self.old_workspace=created["workspace"]["workspace_id"]
        self.old_pane=created["root_pane"]["pane_id"]
        self.broker.config_path.write_text(json.dumps({
            "manager_thread_id":THREAD,"manager_cwd":str(self.root),
            "manager_workspace_id":self.old_workspace,"manager_tab_id":created["tab"]["tab_id"],
            "manager_pane_id":self.old_pane,"manager_binding_generation":7,
            "app_server_remote":"unix://","profile":"control-manager","codex_binary":"/bin/true",
            "supervisor_owns_recovery":True,"supervisor_database":str(self.root/"supervisor.sqlite3"),
        }))

    async def asyncTearDown(self):
        if self.broker.manager_reconnect_task:
            self.broker.manager_reconnect_task.cancel()
            await __import__("asyncio").gather(self.broker.manager_reconnect_task,return_exceptions=True)
        self.broker.db.close(); self.supervisor_db.conn.close(); self.tmp.cleanup()

    def claim(self):
        return self.supervisor_db.claim("manager","recover_manager_binding","fixture",admission_budget=False)

    async def call(self, action):
        return await self.broker.handle("reconcile_topology",{
            "recovery_action_id":action["action_id"],"recovery_claim_key":action["claim_key"],
            "recovery_owner_generation":action["owner_generation"],
        })

    def lose_workspace_and_pane(self):
        self.broker.herdr.workspaces.pop(self.old_workspace)
        self.broker.herdr.panes.pop(self.old_pane)
        old_tab=next(tab_id for tab_id,tab in self.broker.herdr.tabs.items() if tab["workspace_id"] == self.old_workspace)
        self.broker.herdr.tabs.pop(old_tab)

    async def test_proven_workspace_loss_rebinds_same_thread_once_without_prompt_replay(self):
        self.lose_workspace_and_pane(); action=self.claim()
        result=await self.call(action)
        self.assertEqual(result["topology_reattach"],"verified")
        config=json.loads(self.broker.config_path.read_text())
        self.assertEqual(config["manager_thread_id"],THREAD)
        self.assertNotEqual(config["manager_workspace_id"],self.old_workspace)
        self.assertEqual(config["manager_binding_generation"],8)
        self.assertEqual(sum(name=="workspace.create" for name,_ in self.broker.herdr.calls),2)
        self.assertFalse(any(name=="agent.prompt" for name,_ in self.broker.herdr.calls))
        launch=next(params for name,params in self.broker.herdr.calls if name=="execution.start")
        self.assertIn(["--disable","hooks","--disable","goals"],
                      [launch["argv"][i:i+4] for i in range(len(launch["argv"])-3)])
        await self.call(action)
        self.assertEqual(sum(name=="workspace.create" for name,_ in self.broker.herdr.calls),2)

    async def test_pane_only_loss_reuses_live_workspace_with_one_new_tab(self):
        foreign=await self.broker.herdr.request("tab.create",{
            "workspace_id":self.old_workspace,"cwd":str(self.root),"label":"foreign-work",
        })
        foreign_id=foreign["tab"]["tab_id"]
        self.broker.herdr.panes.pop(self.old_pane)
        old_tab=next(tab_id for tab_id,tab in self.broker.herdr.tabs.items() if tab["workspace_id"] == self.old_workspace)
        self.broker.herdr.tabs.pop(old_tab)
        action=self.claim(); await self.call(action)
        config=json.loads(self.broker.config_path.read_text())
        self.assertEqual(config["manager_workspace_id"],self.old_workspace)
        self.assertNotEqual(config["manager_pane_id"],self.old_pane)
        self.assertEqual(sum(name=="workspace.create" for name,_ in self.broker.herdr.calls),1)
        self.assertEqual(sum(name=="execution.start" for name,_ in self.broker.herdr.calls),1)
        self.assertEqual(sum(name=="tab.create" for name,_ in self.broker.herdr.calls),1)
        self.assertIn(foreign_id,self.broker.herdr.tabs)
        self.assertEqual(self.broker.herdr.tabs[foreign_id]["label"],"foreign-work")
        self.assertFalse(any(name=="tab.move" and params["tab_id"]==foreign_id
                             for name,params in self.broker.herdr.calls))

    async def test_transient_inventory_absence_does_not_allocate_when_direct_pane_exists(self):
        action=self.claim(); original=self.broker.herdr.request
        async def stale(method,params=None,timeout=65):
            if method=="session.snapshot": return {"snapshot":{"workspaces":[],"tabs":[],"panes":[],"agents":[]}}
            return await original(method,params,timeout)
        self.broker.herdr.request=stale
        with self.assertRaisesRegex(RuntimeError,"topology is unverified"):
            await self.call(action)
        self.assertEqual(sum(name=="workspace.create" for name,_ in self.broker.herdr.calls),1)
        self.assertEqual(sum(name=="tab.create" for name,_ in self.broker.herdr.calls),0)

    async def test_layout_revision_change_is_not_treated_as_server_incarnation(self):
        self.lose_workspace_and_pane(); action=self.claim(); original=self.broker.herdr.request; revision=0
        async def changing_layout_revision(method,params=None,timeout=65):
            nonlocal revision
            value=await original(method,params,timeout)
            if method=="session.snapshot":
                revision += 1
                value["snapshot"]["revision"]=revision
            return value
        self.broker.herdr.request=changing_layout_revision
        result=await self.call(action)
        self.assertEqual(result["topology_reattach"],"verified")
        self.assertEqual(sum(name=="workspace.create" for name,_ in self.broker.herdr.calls),2)

    async def test_config_commit_crash_replays_immutable_intent_without_second_create(self):
        self.lose_workspace_and_pane(); action=self.claim(); original=self.broker.db.advance_manager_attachment
        tripped=False
        def crash(action_id,*,states,state,**values):
            nonlocal tripped
            if state=="binding_persisted" and not tripped:
                tripped=True; raise RuntimeError("crash after config replace")
            return original(action_id,states=states,state=state,**values)
        self.broker.db.advance_manager_attachment=crash
        with self.assertRaisesRegex(RuntimeError,"crash after config replace"):
            await self.call(action)
        self.assertEqual(self.broker.db.manager_attachment(action["action_id"])["state"],"binding_intent")
        self.broker.db.advance_manager_attachment=original
        result=await self.call(action)
        self.assertEqual(result["topology_reattach"],"verified")
        self.assertEqual(sum(name=="workspace.create" for name,_ in self.broker.herdr.calls),2)

    async def test_new_claim_cannot_allocate_after_prior_create_effect_is_uncertain(self):
        self.lose_workspace_and_pane(); first=self.claim(); original=self.broker.herdr.request
        async def lost_create_response(method,params=None,timeout=65):
            value=await original(method,params,timeout)
            if method=="workspace.create" and params.get("label")=="control":
                raise ConnectionError("response lost after workspace create")
            return value
        self.broker.herdr.request=lost_create_response
        with self.assertRaises(ConnectionError): await self.call(first)
        self.assertEqual(self.broker.db.manager_attachment(first["action_id"])["state"],"uncertain")
        # Model a restarted supervisor after its interrupted action was marked
        # uncertain and its short duplicate-admission interval elapsed.
        self.supervisor_db.finish(first,{"state":"uncertain"},"uncertain")
        second={"action_id":"recovery-"+uuid.uuid4().hex,"claim_key":uuid.uuid4().hex+uuid.uuid4().hex,
                "owner_generation":self.supervisor_db.owner_generation}
        self.supervisor_db.conn.execute(
            """INSERT INTO recovery_actions(action_id,component,kind,state,claim_key,reason,created_at,updated_at,outcome_json,owner_generation,lease_expires_at)
               VALUES(?,?,?,?,?,?,?,?,NULL,?,?)""",
            (second["action_id"],"manager","recover_manager_binding","claimed",second["claim_key"],"later retry",1,1,
             self.supervisor_db.owner_generation,9999999999),
        ); self.supervisor_db.conn.commit()
        self.broker.herdr.request=original
        with self.assertRaisesRegex(RuntimeError,"already uncertain"):
            await self.call(second)
        self.assertEqual(sum(name=="workspace.create" for name,_ in self.broker.herdr.calls),2)

    async def test_new_claim_cannot_bypass_unresolved_prior_intent(self):
        self.lose_workspace_and_pane(); first=self.claim()
        self.broker.db.claim_manager_attachment(
            action_id=first["action_id"],claim_sha256=__import__("hashlib").sha256(first["claim_key"].encode()).hexdigest(),
            logical_thread_id=THREAD,expected_binding_generation=7,
            old_workspace_id=self.old_workspace,old_pane_id=self.old_pane,
        )
        self.supervisor_db.finish(first,{"state":"uncertain"},"uncertain")
        second={"action_id":"recovery-"+uuid.uuid4().hex,"claim_key":uuid.uuid4().hex+uuid.uuid4().hex,
                "owner_generation":self.supervisor_db.owner_generation}
        self.supervisor_db.conn.execute(
            """INSERT INTO recovery_actions(action_id,component,kind,state,claim_key,reason,created_at,updated_at,outcome_json,owner_generation,lease_expires_at)
               VALUES(?,?,?,?,?,?,?,?,NULL,?,?)""",
            (second["action_id"],"manager","recover_manager_binding","claimed",second["claim_key"],"later retry",1,1,
             self.supervisor_db.owner_generation,9999999999),
        ); self.supervisor_db.conn.commit()
        with self.assertRaisesRegex(RuntimeError,"already uncertain"):
            await self.call(second)
        self.assertEqual(sum(name=="workspace.create" for name,_ in self.broker.herdr.calls),1)

    async def test_foreign_claim_capability_is_rejected_before_any_effect(self):
        self.lose_workspace_and_pane(); action=self.claim()
        with self.assertRaisesRegex(PermissionError,"capability"):
            await self.broker.handle("reconcile_topology",{
                "recovery_action_id":action["action_id"],"recovery_claim_key":"f"*64,
                "recovery_owner_generation":action["owner_generation"],
            })
        self.assertEqual(sum(name=="workspace.create" for name,_ in self.broker.herdr.calls),1)

    async def test_cancelled_execution_response_replays_one_durable_resume(self):
        self.lose_workspace_and_pane(); action=self.claim(); original=self.broker.herdr.request
        async def cancelled_after_admission(method,params=None,timeout=65):
            value=await original(method,params,timeout)
            if method=="execution.start":
                raise __import__("asyncio").CancelledError()
            return value
        self.broker.herdr.request=cancelled_after_admission
        with self.assertRaises(__import__("asyncio").CancelledError): await self.call(action)
        self.broker.herdr.request=original
        await self.call(action)
        execution_id="manager-resume-"+action["action_id"]
        self.assertEqual(len(self.broker.herdr.executions),1)
        self.assertIn(execution_id,self.broker.herdr.executions)
        self.assertEqual(sum(name=="execution.start" for name,_ in self.broker.herdr.calls),2)
        self.assertFalse(any(name=="pane.send_text" for name,_ in self.broker.herdr.calls))

    async def test_concurrent_ordinary_reconcile_cannot_create_a_second_binding(self):
        self.lose_workspace_and_pane(); action=self.claim()
        ordinary,recovery=await __import__("asyncio").gather(self.broker.reconcile(),self.call(action))
        self.assertFalse(ordinary.get("reconciled"),ordinary)
        self.assertEqual(recovery["topology_reattach"],"verified")
        self.assertEqual(sum(name=="workspace.create" for name,_ in self.broker.herdr.calls),2)

    async def test_pause_after_workspace_creation_fences_execution_start(self):
        self.lose_workspace_and_pane(); action=self.claim(); original=self.broker.herdr.request
        async def pause_after_create(method,params=None,timeout=65):
            value=await original(method,params,timeout)
            if method=="workspace.create" and params.get("label")=="control":
                self.supervisor_db.set_paused(True)
            return value
        self.broker.herdr.request=pause_after_create
        with self.assertRaisesRegex(RuntimeError,"topology is unverified"):
            await self.call(action)
        self.assertEqual(sum(name=="workspace.create" for name,_ in self.broker.herdr.calls),2)
        self.assertFalse(any(name=="execution.start" for name,_ in self.broker.herdr.calls))

    async def test_owner_generation_change_after_workspace_creation_fences_execution_start(self):
        self.lose_workspace_and_pane(); action=self.claim(); original=self.broker.herdr.request
        async def takeover_after_create(method,params=None,timeout=65):
            value=await original(method,params,timeout)
            if method=="workspace.create" and params.get("label")=="control":
                self.supervisor_db.conn.execute("UPDATE recovery_settings SET value=? WHERE key='owner_generation'",("f"*32,))
                self.supervisor_db.conn.commit()
            return value
        self.broker.herdr.request=takeover_after_create
        with self.assertRaisesRegex(RuntimeError,"topology is unverified"):
            await self.call(action)
        self.assertFalse(any(name=="execution.start" for name,_ in self.broker.herdr.calls))


if __name__=="__main__": unittest.main()
