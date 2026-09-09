"""Recovery must not restart healthy infrastructure for a manager capability gap."""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from control_supervisor import Supervisor, REQUIRED_TOOLS


class ComponentIsolationAcceptance(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="recovery-component-isolation-")
        root = Path(self.tmp.name)
        # A real bounded helper returns successful app-server RPC evidence,
        # while its canonical manager lacks the required broker tools.
        value = {"thread": {"id": "fixture-manager", "status": "idle"},
                 "turn": {"id": "fixture-turn", "status": "completed", "error": None},
                 "control_broker": {"inventory_verified": True, "runtime_status": "connected", "tools": []}}
        config = root / "machine.json"
        config.write_text(json.dumps({
            "manager_thread_id": "fixture-manager", "app_server_socket": str(root / "app.sock"),
            "appserver_probe_command": [sys.executable, "-c", "print(" + repr(json.dumps(value)) + ")"],
            "supervisor_mode": "isolated_active",
            **{key: str(root / key) for key in (
                "herdr_socket", "broker_socket", "manager_observer_socket", "remote_control_socket")},
        }))
        self.supervisor = Supervisor(root / "recovery.sock", root / "recovery.db", config)
        self.config_path = config
        self.probe_value = value

    async def asyncTearDown(self):
        self.supervisor.db.conn.close()
        self.tmp.cleanup()

    async def test_missing_tools_does_not_mark_app_server_unhealthy(self):
        with patch("control_supervisor.unix_probe", AsyncMock(return_value=(True, "fixture RPC healthy"))):
            result = await self.supervisor.check()
        components = {c["component"]: c for c in result["components"]}
        self.assertEqual(components["app_server"]["status"], "healthy")
        self.assertNotEqual(components["broker_tools"]["status"], "healthy")

    async def test_missing_tools_never_restart_app_server_or_continue_completed_turn(self):
        actions = []

        async def action(cfg, claim, name):
            actions.append(name)
            return {"state": "completed"}

        with patch("control_supervisor.unix_probe", AsyncMock(return_value=(True, "fixture RPC healthy"))), \
             patch.object(self.supervisor, "_run_action", action):
            for _ in range(3):
                await self.supervisor.check()
            await self.supervisor.recover("all", idempotency_key="missing-tools-case")
        self.assertNotIn("restart_app_server", actions)
        self.assertNotIn("manager_continuation", actions)

    async def test_structured_transport_error_survives_complete_health_check(self):
        self.probe_value['turn'].update(status='failed',error={
            'message':'stream disconnected',
            'codexErrorInfo':{'responseStreamDisconnected':{'httpStatusCode':None}}})
        self.probe_value['control_broker']['tools']=sorted(REQUIRED_TOOLS)
        config=json.loads(self.config_path.read_text())
        config['appserver_probe_command']=[sys.executable,'-c','print('+repr(json.dumps(self.probe_value))+')']
        self.config_path.write_text(json.dumps(config))
        with patch('control_supervisor.unix_probe',AsyncMock(return_value=(True,'fixture RPC healthy'))):
            result=await self.supervisor.check()
        components={c['component']:c for c in result['components']}
        self.assertEqual(components['app_server']['status'],'healthy')
        self.assertNotEqual(components['manager_turn']['status'],'healthy')
        self.assertEqual(result['authoritative']['turn']['error'],self.probe_value['turn']['error'])

    async def test_auth_quota_and_interruption_never_trigger_observer_recovery(self):
        for status,info in [('failed','unauthorized'),('failed','usageLimitExceeded'),
                            ('failed','serverOverloaded'),('interrupted',None)]:
            with self.subTest(status=status,info=info):
                self.probe_value['turn'].update(status=status,error={
                    'message':'fixture terminal result','codexErrorInfo':info})
                self.probe_value['control_broker']['tools']=sorted(REQUIRED_TOOLS)
                config=json.loads(self.config_path.read_text())
                config['appserver_probe_command']=[sys.executable,'-c','print('+repr(json.dumps(self.probe_value))+')']
                self.config_path.write_text(json.dumps(config))
                with patch('control_supervisor.unix_probe',AsyncMock(return_value=(True,'fixture RPC healthy'))), \
                     patch.object(self.supervisor,'_run_action',AsyncMock()) as action:
                    for _ in range(4):
                        check=await self.supervisor.check()
                        self.assertIsNone(await self.supervisor.automatic_recover(check))
                    action.assert_not_awaited()

    async def test_failed_appserver_probe_never_licenses_capability_candidate(self):
        config=json.loads(self.config_path.read_text())
        config['appserver_probe_command']=[sys.executable,'-c','raise SystemExit(7)']
        self.config_path.write_text(json.dumps(config))
        for _ in range(3): await self.supervisor.check()
        from unittest.mock import AsyncMock, patch
        with patch.object(self.supervisor,'_run_action',AsyncMock(return_value={'state':'completed'})) as action:
            await self.supervisor.recover('manager',idempotency_key='probe-failed')
        self.assertNotIn('refresh_capabilities',[call.args[2] for call in action.await_args_list])

    async def test_disconnected_mcp_inventory_is_not_healthy(self):
        self.probe_value["control_broker"] = {
            "inventory_verified": True, "runtime_status": "disconnected", "tools": sorted(REQUIRED_TOOLS)
        }
        config = json.loads(self.config_path.read_text())
        config["appserver_probe_command"] = [
            sys.executable, "-c", "print(" + repr(json.dumps(self.probe_value)) + ")"
        ]
        self.config_path.write_text(json.dumps(config))
        with patch("control_supervisor.unix_probe", AsyncMock(return_value=(True, "fixture RPC healthy"))):
            result = await self.supervisor.check()
        components = {c["component"]: c for c in result["components"]}
        self.assertEqual(components["app_server"]["status"], "healthy")
        self.assertNotEqual(components["broker_tools"]["status"], "healthy")
        self.assertIn("not connected", components["broker_tools"]["reason"])


if __name__ == "__main__":
    unittest.main()
