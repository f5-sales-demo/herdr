from __future__ import annotations

import json
import tempfile
import tomllib
import unittest
import sys
from pathlib import Path
from unittest.mock import patch

from appserver_manager import MANAGER_CWD, MANAGER_EFFORT, MANAGER_MODEL, REQUIRED_CONTROL_TOOLS, activate_refreshed_tools, clear_goal, ensure, hold, manager_config, native_pane_health, probe, refresh_tools, refresh_tools_in_place, resume_once


class FakeRefreshServer:
    def __init__(self, tools=None):
        self.tools = set(REQUIRED_CONTROL_TOOLS if tools is None else tools)
        self.calls = []

    def request(self, method, params=None):
        self.calls.append((method, params))
        if method == "thread/read":
            return {"thread": {"id": "old-thread", "name": "Control Manager", "cwd": MANAGER_CWD}}
        if method == "thread/turns/list":
            return {"data": [{"id": "failed-turn", "status": "failed", "error": "transient transport"}]}
        if method == "thread/resume":
            return {"thread": {"id": params["threadId"], "name": "Control Manager", "cwd": MANAGER_CWD}}
        if method == "thread/fork":
            return {"thread": {"id": "new-thread"}}
        if method == "thread/goal/get":
            return {"goal": None}
        if method == "mcpServerStatus/list":
            return {"data": [{"name": "control_broker", "runtimeStatus": "connected",
                              "tools": {name: {} for name in self.tools}}]}
        if method == "turn/start":
            return {"turn": {"id": "recovery-turn", "status": "inProgress"}}
        return {}

    def wait_notification(self, method):
        self.calls.append(("wait_notification", {"method":method}))
        return {"params":{"turn":{"id":"recovery-turn","status":"completed"}}}


class ManagerRealtimeConfigTests(unittest.TestCase):
    def test_appserver_manager_enables_realtime(self):
        config = manager_config()

        self.assertEqual(config["model"], MANAGER_MODEL)
        self.assertEqual(config["model_reasoning_effort"], MANAGER_EFFORT)
        self.assertIs(config["features"]["realtime_conversation"], True)
        self.assertIs(config["features"]["goals"], False)
        tools = config["mcp_servers"]["control_broker"]["enabled_tools"]
        self.assertIn("completion_inbox", tools)
        self.assertIn("ack_completion", tools)

    def test_generated_manager_config_enables_realtime(self):
        profile = manager_config()
        self.assertIs(profile["features"]["realtime_conversation"], True)

    def test_manager_config_binds_remote_shell_tools_to_saved_herdr_pane(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "config.json"
            path.write_text(json.dumps({
                "manager_workspace_id": "wP",
                "manager_tab_id": "wP:t2",
                "manager_pane_id": "wP:p2",
            }))
            with patch("appserver_manager.CONFIG_PATH", path), \
                 patch("appserver_manager.HERDR_SOCKET", "/tmp/herdr.sock"):
                config = manager_config()
        environment = config["shell_environment_policy"]["set"]
        self.assertEqual(environment, {
            "HERDR_ENV": "1",
            "HERDR_SOCKET_PATH": "/tmp/herdr.sock",
            "HERDR_WORKSPACE_ID": "wP",
            "HERDR_TAB_ID": "wP:t2",
            "HERDR_PANE_ID": "wP:p2",
        })

    def test_refresh_tools_verifies_candidate_without_replacing_canonical(self):
        server = FakeRefreshServer()
        with patch("appserver_manager.persist_config"):
            result = refresh_tools(server, "old-thread")
        self.assertEqual(result["candidate_thread_id"], "new-thread")
        self.assertTrue(result["canonical_unchanged"])
        methods = [method for method, _params in server.calls]
        self.assertIn("mcpServerStatus/list", methods)
        self.assertNotIn("thread/archive", methods)

    def test_refresh_tools_archives_only_failed_candidate(self):
        server = FakeRefreshServer({"status"})
        with self.assertRaisesRegex(RuntimeError, "inventory is incomplete"):
            refresh_tools(server, "old-thread")
        archives = [params["threadId"] for method, params in server.calls if method == "thread/archive"]
        self.assertEqual(archives, ["new-thread"])

    def test_automatic_refresh_surface_preserves_canonical_identity_in_place(self):
        server = FakeRefreshServer()
        with patch("appserver_manager.persist_config") as persist:
            result = refresh_tools_in_place(server, "old-thread")
        self.assertTrue(result["refreshed_in_place"])
        self.assertEqual(result["canonical_thread_id"], "old-thread")
        methods = [method for method, _params in server.calls]
        self.assertIn("thread/resume", methods)
        self.assertIn("config/mcpServer/reload", methods)
        self.assertNotIn("thread/fork", methods)
        self.assertNotIn("thread/archive", methods)
        self.assertNotIn("thread/name/set", methods)
        persist.assert_not_called()

    def test_guarded_activation_promotes_only_verified_full_history_candidate(self):
        server=FakeRefreshServer()
        with patch("appserver_manager.persist_config") as persist:
            result=activate_refreshed_tools(server,"old-thread")
        self.assertTrue(result["activated"])
        self.assertTrue(result["full_history_forked"])
        fork=next(params for method,params in server.calls if method == "thread/fork")
        self.assertFalse(fork["excludeTurns"])
        self.assertIs(fork["copyGoal"],False)
        self.assertIs(fork["deferGoalContinuation"],False)
        persist.assert_called_once_with("new-thread")
        self.assertIn(("thread/archive",{"threadId":"old-thread"}),server.calls)

    def test_probe_reports_exact_thread_turn_and_actual_tool_inventory(self):
        result = probe(FakeRefreshServer(), "old-thread")
        self.assertEqual(result["thread"]["id"], "old-thread")
        self.assertEqual(result["control_broker"]["runtime_status"], "connected")
        self.assertEqual(set(result["control_broker"]["tools"]), REQUIRED_CONTROL_TOOLS)
        self.assertEqual(result["goal"],{"presence_verified":True,"present":False,"status":None})

    def test_probe_redacts_goal_objective_and_reports_presence_status_only(self):
        server=FakeRefreshServer()
        original=server.request
        def request(method,params=None):
            if method == "thread/goal/get":
                return {"goal":{"objective":"never expose me","status":"blocked","tokensUsed":7}}
            return original(method,params)
        server.request=request
        result=probe(server,"old-thread")
        self.assertEqual(result["goal"],{"presence_verified":True,"present":True,"status":"blocked"})
        self.assertNotIn("never expose me",json.dumps(result))

    def test_probe_degrades_goal_health_when_api_is_unsupported(self):
        server=FakeRefreshServer()
        original=server.request
        def request(method,params=None):
            if method == "thread/goal/get": raise RuntimeError("method not found")
            return original(method,params)
        server.request=request
        result=probe(server,"old-thread")
        self.assertFalse(result["goal"]["presence_verified"])
        self.assertIsNone(result["goal"]["present"])

    def test_probe_keeps_exact_thread_healthy_when_inventory_lookup_fails(self):
        server = FakeRefreshServer()
        original = server.request
        def request(method, params=None):
            if method == "mcpServerStatus/list":
                raise RuntimeError("thread_not_found while inventory is loading")
            return original(method, params)
        server.request = request
        result = probe(server, "old-thread")
        self.assertEqual(result["thread"]["id"], "old-thread")
        self.assertFalse(result["control_broker"]["inventory_verified"])
        self.assertEqual(result["control_broker"]["tools"], [])

    def test_native_pane_health_requires_exact_session_and_does_not_touch_foreign_busy_agent(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "config.json"
            path.write_text(json.dumps({"manager_thread_id": "canonical-thread", "manager_pane_id": "wE:p1"}))
            def request(method, _params):
                if method == "agent.get":
                    return {"agent": {"agent_status": "working", "agent_session": {"value": "foreign-thread"}}}
                if method == "pane.process_info":
                    return {"process_info": {"foreground_processes": [{"name": "codex", "argv": ["foreign"]}]}}
                self.fail(method)
            with patch("appserver_manager.CONFIG_PATH", path), \
                 patch("appserver_manager.herdr_request", side_effect=request) as mocked:
                result = native_pane_health("canonical-thread")
            self.assertEqual(result["state"], "waiting_user")
            self.assertEqual(mocked.call_args_list[0].args, ("agent.get", {"target": "wE:p1"}))

    def test_native_pane_health_rejects_saved_exact_metadata_without_a_runtime(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "config.json"
            path.write_text(json.dumps({"manager_thread_id": "canonical-thread", "manager_pane_id": "wE:p1"}))
            responses = iter(({"agent": {"agent": "codex", "agent_status": "idle",
                                         "agent_session": {"value": "canonical-thread"}}},
                              {"process_info": {"foreground_processes": []}}))
            with patch("appserver_manager.CONFIG_PATH", path), patch("appserver_manager.herdr_request", side_effect=responses):
                result = native_pane_health("canonical-thread")
            self.assertEqual(result["state"], "unavailable")
            self.assertIn("no foreground runtime", result["reason"])

    def test_native_pane_health_rejects_terminally_disconnected_exact_client(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            path = root / "config.json"
            thread = "canonical-thread"
            argv = [str(Path("/bin/true").resolve()), "--disable", "hooks", "--disable", "goals", "--remote", "unix:///owned/appserver.sock",
                    "-C", str(root), "resume", thread]
            path.write_text(json.dumps({
                "manager_thread_id": thread, "manager_pane_id": "wE:p1", "manager_cwd": str(root),
                "codex_binary": "/bin/true", "app_server_remote": "unix://",
                "app_server_socket": "/owned/appserver.sock", "profile": "control-manager",
            }))
            def request(method, _params):
                if method == "agent.get":
                    # The real reconnect spinner is classified as working even
                    # after Codex has reached its terminal failure footer.
                    return {"agent": {"agent": "codex", "agent_status": "working",
                                      "agent_session": {"value": thread}}}
                if method == "pane.process_info":
                    return {"process_info": {"foreground_processes": [{"name": "codex", "argv": argv}]}}
                if method == "agent.read":
                    return {"read": {"text": "Automatic reconnect could not restore this session.\n"
                                             "app-server session could not be restored\n"
                                             "Reconnect failed — check the endpoint, then relaunch\n"
                                             "Ask Codex to do anything\nctrl+c quit"}}
                self.fail(method)
            with patch("appserver_manager.CONFIG_PATH", path), patch("appserver_manager.herdr_request", side_effect=request):
                result = native_pane_health(thread)
            self.assertEqual(result["state"], "unavailable")
            self.assertIs(result["terminal_disconnect_proven"], True)
            self.assertEqual(result["terminal_source"], "visible")

    def test_native_pane_health_keeps_exact_interactive_client_healthy(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            path = root / "config.json"
            thread = "canonical-thread"
            argv = [str(Path("/bin/true").resolve()), "--disable", "hooks", "--disable", "goals", "--remote", "unix://",
                    "-C", str(root), "resume", thread]
            path.write_text(json.dumps({
                "manager_thread_id": thread, "manager_pane_id": "wE:p1", "manager_cwd": str(root),
                "codex_binary": "/bin/true", "app_server_remote": "unix://", "profile": "control-manager",
            }))
            responses = {
                "agent.get": {"agent": {"agent": "codex", "agent_status": "idle",
                                          "agent_session": {"value": thread}}},
                "pane.process_info": {"process_info": {"foreground_processes": [{"name": "codex", "argv": argv}]}},
                "agent.read": {"read": {"text": "Ask Codex to do anything"}},
            }
            with patch("appserver_manager.CONFIG_PATH", path), \
                 patch("appserver_manager.herdr_request", side_effect=lambda method, _params: responses[method]):
                result = native_pane_health(thread)
            self.assertEqual(result["state"], "healthy")

    def test_observer_diagnostics_are_sent_to_stderr_not_ndjson_stdout(self):
        server = FakeRefreshServer()
        original = server.request
        def request(method, params=None):
            if method == "thread/resume":
                return {"thread": {"id": "old-thread", "cwd": MANAGER_CWD}}
            if method == "thread/read":
                return {"thread": {"id": "old-thread", "cwd": MANAGER_CWD, "turns": []}}
            return original(method, params)
        server.request = request
        messages = iter([
            {"method": "item/completed", "params": {"turnId": "turn", "item": {"id": "item", "type": "agentMessage", "text": "ok"}}},
            RuntimeError("stop observer fixture"),
        ])
        def receive(_timeout):
            value = next(messages)
            if isinstance(value, Exception): raise value
            return value
        server._receive_json = receive
        with patch("appserver_manager.CONFIG_PATH") as config, \
             patch("appserver_manager.report_manager_lifecycle"), \
             patch("appserver_manager.broker_request", return_value={"matched_event_ids": ["event"]}), \
             patch("builtins.print") as printed:
            config.read_text.return_value = "{}"
            with self.assertRaisesRegex(RuntimeError, "stop observer fixture"):
                hold(server, "old-thread")
        diagnostic = next(call for call in printed.call_args_list if str(call.args[0]).startswith("manager response correlated"))
        self.assertIs(diagnostic.kwargs.get("file"), sys.stderr)

    def test_long_history_observer_uses_metadata_heartbeat_and_subscribed_events(self):
        server = FakeRefreshServer()
        original = server.request
        def request(method, params=None):
            if method == "thread/read" and params.get("includeTurns"):
                raise RuntimeError("full history read exceeded fixture deadline")
            return original(method, params)
        server.request = request
        messages = iter((__import__("socket").timeout(), RuntimeError("stop observer fixture")))
        def receive(_timeout):
            value = next(messages)
            if isinstance(value, Exception):
                raise value
            return value
        server._receive_json = receive
        with patch("appserver_manager.CONFIG_PATH") as config, \
             patch("appserver_manager.report_manager_lifecycle"), \
             patch("builtins.print"):
            config.read_text.return_value = "{}"
            with self.assertRaisesRegex(RuntimeError, "stop observer fixture"):
                hold(server, "old-thread")
        reads = [params for method, params in server.calls if method == "thread/read"]
        self.assertGreaterEqual(len(reads), 2)
        self.assertTrue(all(params["includeTurns"] is False for params in reads))
        self.assertNotIn("thread/fork", [method for method, _params in server.calls])

    def test_ensure_persists_nondefault_owned_appserver_remote(self):
        server = FakeRefreshServer()
        with tempfile.TemporaryDirectory() as raw:
            config = Path(raw) / "machine.json"
            config.write_text(json.dumps({"manager_thread_id": "old-thread"}))
            with patch("appserver_manager.CONFIG_PATH", config), \
                 patch("appserver_manager.APP_SERVER_REMOTE", "unix:///owned/appserver.sock"):
                result = ensure(server)
            persisted = json.loads(config.read_text())
        self.assertEqual(result["thread_id"], "old-thread")
        self.assertEqual(persisted["app_server_remote"], "unix:///owned/appserver.sock")

    def test_resume_once_preserves_exact_thread_identity(self):
        server=FakeRefreshServer()
        original=server.request
        def request(method, params=None):
            if method == 'thread/read': return {'thread': {'id':'old-thread','name':'Control Manager','cwd':MANAGER_CWD,'turns':[{'id':'failed-turn','status':'failed'}]}}
            if method == 'thread/resume': return {'thread': {'id': params['threadId'], 'cwd': MANAGER_CWD, 'status':'working'}}
            return original(method,params)
        server.request=request
        result=resume_once(server,'old-thread','failed-turn')
        self.assertTrue(result['history_preserved'])
        self.assertEqual(result['recovery_turn_id'],'recovery-turn')
        self.assertFalse(result['original_request_replayed'])
        self.assertIn('turn/start',[method for method,_ in server.calls])

    def test_resume_once_refuses_changed_failed_turn(self):
        server=FakeRefreshServer()
        with self.assertRaisesRegex(RuntimeError,'failed turn changed'):
            resume_once(server,'old-thread','stale-failed-turn')
        self.assertNotIn('turn/start',[method for method,_ in server.calls])

    def test_clear_goal_validates_exact_configured_thread_before_mutation(self):
        server=FakeRefreshServer()
        with tempfile.TemporaryDirectory() as raw:
            config=Path(raw)/"machine.json"
            config.write_text(json.dumps({
                "manager_thread_id":"old-thread","manager_thread_name":"Control Manager",
                "manager_cwd":MANAGER_CWD,
            }))
            with patch("appserver_manager.CONFIG_PATH",config):
                with self.assertRaisesRegex(RuntimeError,"exact configured"):
                    clear_goal(server,"foreign-thread")
        self.assertNotIn("thread/goal/clear",[method for method,_ in server.calls])

    def test_clear_goal_is_idempotent_when_goal_is_already_absent(self):
        server=FakeRefreshServer()
        with tempfile.TemporaryDirectory() as raw:
            config=Path(raw)/"machine.json"
            config.write_text(json.dumps({
                "manager_thread_id":"old-thread","manager_thread_name":"Control Manager",
                "manager_cwd":MANAGER_CWD,
            }))
            with patch("appserver_manager.CONFIG_PATH",config):
                result=clear_goal(server,"old-thread")
        self.assertTrue(result["already_absent"])
        self.assertFalse(result["clear_requested"])
        self.assertNotIn("thread/goal/clear",[method for method,_ in server.calls])

    def test_clear_goal_redacts_objective_and_verifies_absence(self):
        class GoalServer(FakeRefreshServer):
            def __init__(self):
                super().__init__(); self.goal={"objective":"secret objective","status":"blocked",
                    "tokenBudget":100,"tokensUsed":8,"timeUsedSeconds":3}
            def request(self,method,params=None):
                if method == "thread/goal/get":
                    self.calls.append((method,params)); return {"goal":self.goal}
                if method == "thread/goal/clear":
                    self.calls.append((method,params)); self.goal=None; return {"cleared":True}
                return super().request(method,params)
        server=GoalServer()
        with tempfile.TemporaryDirectory() as raw:
            config=Path(raw)/"machine.json"
            config.write_text(json.dumps({
                "manager_thread_id":"old-thread","manager_thread_name":"Control Manager",
                "manager_cwd":MANAGER_CWD,
            }))
            with patch("appserver_manager.CONFIG_PATH",config): result=clear_goal(server,"old-thread")
        self.assertTrue(result["cleared"])
        self.assertFalse(result["goal_present"])
        self.assertEqual(result["prior_goal_accounting"]["status"],"blocked")
        self.assertNotIn("secret objective",json.dumps(result))

    def test_clear_goal_reconciles_a_lost_success_response_by_rereading(self):
        class LostResponseServer(FakeRefreshServer):
            def __init__(self): super().__init__(); self.goal={"objective":"hidden","status":"active"}
            def request(self,method,params=None):
                if method == "thread/goal/get":
                    self.calls.append((method,params)); return {"goal":self.goal}
                if method == "thread/goal/clear":
                    self.calls.append((method,params)); self.goal=None; raise RuntimeError("response lost")
                return super().request(method,params)
        server=LostResponseServer()
        with tempfile.TemporaryDirectory() as raw:
            config=Path(raw)/"machine.json"
            config.write_text(json.dumps({
                "manager_thread_id":"old-thread","manager_thread_name":"Control Manager",
                "manager_cwd":MANAGER_CWD,
            }))
            with patch("appserver_manager.CONFIG_PATH",config): result=clear_goal(server,"old-thread")
        self.assertTrue(result["reconciled_after_uncertain_response"])
        self.assertFalse(result["goal_present"])


if __name__ == "__main__":
    unittest.main()
