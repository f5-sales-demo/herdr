import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from control_supervisor import Supervisor


class NativeReadiness(unittest.IsolatedAsyncioTestCase):
    def supervisor(self, root):
        config = root / "config.json"
        config.write_text(json.dumps({"supervisor_mode": "isolated_active", "manager_thread_id": "canonical-thread",
                                      "manager_workspace_id": "wE", "manager_pane_id": "wE:p1"}))
        return Supervisor(root / "supervisor.sock", root / "recovery.db", config)

    async def test_healthy_rpc_with_missing_native_pane_is_not_healthy(self):
        with tempfile.TemporaryDirectory() as raw:
            s = self.supervisor(Path(raw))
            app = {"thread": {"id": "canonical-thread", "status": "idle"}, "turn": {"status": "completed"},
                   "control_broker": {"inventory_verified": True, "runtime_status": "connected", "tools": []}}
            try:
                with patch("control_supervisor.unix_probe", AsyncMock(return_value=(True, "healthy"))), \
                     patch("control_supervisor.appserver_probe", AsyncMock(return_value=(True, "thread/read healthy", app))), \
                     patch("control_supervisor.native_manager_probe", AsyncMock(return_value=("unavailable", "agent_not_found", {}))):
                    result = await s.check()
                by = {item["component"]: item for item in result["components"]}
                self.assertEqual(by["app_server"]["status"], "healthy")
                self.assertEqual(by["manager_native"]["status"], "unavailable")
            finally:
                s.db.conn.close()

    async def test_missing_native_pane_reconciles_once_then_requires_exact_healthy_identity(self):
        with tempfile.TemporaryDirectory() as raw:
            s = self.supervisor(Path(raw))
            missing = {"state": "unavailable", "components": [
                {"component": "herdr", "status": "healthy"}, {"component": "app_server", "status": "healthy"},
                {"component": "manager_thread", "status": "healthy"}, {"component": "manager_turn", "status": "healthy"},
                {"component": "manager_native", "status": "unavailable", "reason": "agent_not_found"},
                {"component": "broker_tools", "status": "healthy"}],
                "authoritative": {"turn": {}, "control_broker": {"inventory_verified": True}}}
            healthy = {**missing, "state": "healthy", "components": [dict(item, status="healthy") if item["component"] == "manager_native" else item for item in missing["components"]]}
            actions = []
            async def action(_cfg, _claim, name):
                actions.append(name)
                return {"state": "completed"}
            try:
                with patch.object(s, "check", AsyncMock(side_effect=[missing, missing, missing, healthy])), \
                     patch.object(s, "_run_action", action):
                    result = await s.recover("manager")
                self.assertEqual(result["state"], "completed")
                self.assertEqual(actions.count("reconcile_manager_topology"), 1)
                self.assertFalse(any(name.startswith("restart_") for name in actions))
            finally:
                s.db.conn.close()

    async def test_foreign_or_busy_native_pane_is_noninterrupting(self):
        with tempfile.TemporaryDirectory() as raw:
            s = self.supervisor(Path(raw))
            check = {"state": "waiting_user", "components": [
                {"component": "herdr", "status": "healthy"}, {"component": "app_server", "status": "healthy"},
                {"component": "manager_thread", "status": "healthy"}, {"component": "manager_turn", "status": "healthy"},
                {"component": "manager_native", "status": "waiting_user", "reason": "foreign busy pane"},
                {"component": "broker_tools", "status": "healthy"}],
                "authoritative": {"turn": {}, "control_broker": {"inventory_verified": True}}}
            try:
                with patch.object(s, "check", AsyncMock(return_value=check)), \
                     patch.object(s, "_run_action", AsyncMock(return_value={"state": "waiting_rollout"})) as action:
                    self.assertIsNone(await s.automatic_recover(check))
                    result = await s.recover("manager")
                self.assertEqual(result["state"], "blocked")
                self.assertNotIn("reconcile_manager_topology", [call.args[2] for call in action.await_args_list])
                self.assertFalse(any(call.args[2].startswith("restart_") for call in action.await_args_list))
            finally:
                s.db.conn.close()

    async def test_inventory_lookup_failure_never_refreshes_or_restarts_appserver(self):
        with tempfile.TemporaryDirectory() as raw:
            s = self.supervisor(Path(raw))
            check = {"state": "degraded", "components": [
                {"component": "app_server", "status": "healthy"}, {"component": "manager_thread", "status": "healthy"},
                {"component": "manager_turn", "status": "healthy"}, {"component": "manager_native", "status": "healthy"},
                {"component": "broker_tools", "status": "degraded", "reason": "inventory unavailable"}],
                "authoritative": {"turn": {}, "control_broker": {"inventory_verified": False, "inventory_error": "thread_not_found"}}}
            try:
                with patch.object(s, "check", AsyncMock(return_value=check)), \
                     patch.object(s, "_run_action", AsyncMock(return_value={"state": "completed"})) as action:
                    await s.recover("manager")
                names = [call.args[2] for call in action.await_args_list]
                self.assertNotIn("refresh_capabilities", names)
                self.assertNotIn("restart_app_server", names)
            finally:
                s.db.conn.close()


if __name__ == "__main__":
    unittest.main()
