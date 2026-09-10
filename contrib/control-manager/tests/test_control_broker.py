from __future__ import annotations

import asyncio
import shlex
import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from control_broker import Broker, OUTBOX_LEASE_SECONDS, StateDB, completion_presentation


class FakeHerdr:
    def __init__(self):
        self.workspaces: dict[str, dict] = {}
        self.tabs: dict[str, dict] = {}
        self.panes: dict[str, dict] = {}
        self.calls: list[tuple[str, dict]] = []
        self.executions: dict[str, dict] = {}
        self.agent_turns: list[dict] = []
        self.active_workspace_id: str | None = None
        self.seq = 0

    async def request(self, method, params=None, timeout=65):
        params = params or {}
        self.calls.append((method, params))
        if method == "execution.resume":
            semantic, generation = params["execution_id"], params["generation"]
            launch = params["native_launch"]
            existing = next((value for value in self.executions.values()
                             if value.get("semantic_execution_id") == semantic and value.get("generation") == generation), None)
            if existing:
                return {"execution": existing, "admitted": False}
            self.seq += 1
            wid = params.get("workspace_id", self.active_workspace_id)
            if not isinstance(wid, str) or wid not in self.workspaces:
                raise RuntimeError("workspace_not_found")
            tid, pid = f"{wid}:t{self.seq}", f"{wid}:p{self.seq}"
            backend = f"backend-{semantic}-{generation}"
            self.tabs[tid] = {"tab_id": tid, "workspace_id": wid, "label": params.get("label"), "number": 1, "pane_count": 1}
            self.panes[pid] = {"pane_id": pid, "workspace_id": wid, "tab_id": tid, "agent_status": "working", "cwd": params.get("cwd")}
            execution = {"execution_id": backend, "backend_execution_id": backend,
                         "semantic_execution_id": semantic, "generation": generation,
                         "native_producer": "xcsh", "producer_session_id": launch["session_header"]["id"], "workspace_id": wid,
                         "cwd": params["cwd"], "command": {"mode": "argv", "argv": [launch["xcsh_executable"]] + ([] if launch["interactive"] else ["--mode", "json"]) + ["--session-dir", launch["session_dir"], "--resume", launch["session_path"], "--model", launch["model"], "--tools", "read", "--no-mcp", "--no-lsp", "--no-memories", "--no-skills", "--no-rules", "--no-pty"] + ([] if launch["interactive"] else ["--print"]) + [params["text"]]},
                         "native_executable": {"canonical_path": launch["xcsh_executable"],
                                               "sha256": hashlib.sha256(Path(launch["xcsh_executable"]).read_bytes()).hexdigest()},
                         "native_launch": launch,
                         "injected_env": {"HERDR_EXECUTION_ID": semantic, "HERDR_EXECUTION_GENERATION": str(generation)},
                         "state": "running", "pane_id": pid, "tab_id": tid,
                         "exit_code": None, "signal_name": None, "stdout_tail": "", "output_complete": False}
            self.executions[backend] = execution
            return {"execution": execution, "admitted": True}
        if method == "execution.start":
            existing = self.executions.get(params["execution_id"])
            if existing:
                return {"execution": existing, "admitted": False}
            self.seq += 1
            wid = params["workspace_id"]
            tid = f"{wid}:t{self.seq}"
            pid = f"{wid}:p{self.seq}"
            tab = {"tab_id": tid, "workspace_id": wid, "label": params.get("label"), "number": len([t for t in self.tabs.values() if t["workspace_id"] == wid]) + 1, "pane_count": 1}
            pane = {"pane_id": pid, "workspace_id": wid, "tab_id": tid, "agent_status": "unknown", "cwd": params.get("cwd")}
            if params.get("mode") == "argv":
                process_argv = list(params.get("argv") or [])
                if process_argv[:1] == ["/usr/bin/env"]:
                    process_argv = process_argv[2:]
                pane.update({"process_name":"codex","process_argv":process_argv,
                             "agent_status":"working"})
            execution = {"execution_id": params["execution_id"], "state": "running", "pane_id": pid, "tab_id": tid, "exit_code": None, "signal_name": None, "stdout_tail": "", "output_complete": False}
            self.tabs[tid] = tab
            self.panes[pid] = pane
            self.executions[params["execution_id"]] = execution
            return {"execution": execution, "admitted": True}
        if method == "execution.get":
            return {"execution": self.executions[params["execution_id"]]}
        if method == "execution.cancel":
            execution = self.executions[params["execution_id"]]
            # Protocol-22 native cancellation is cooperative: this request
            # persists intent, while only a later authenticated semantic turn
            # can settle the task as cancelled.
            if execution.get("native_launch") is not None:
                execution.update(cancel_requested=True)
            else:
                execution.update(state="cancelled", signal_name="Interrupt", output_complete=True)
            return {"execution": execution, "admitted": False}
        if method == "ping":
            return {"protocol": 23, "capabilities": {"tracked_executions": True, "agent_turn_journal": True}}
        if method == "agent.turn.list":
            since = params.get("since_revision", 0)
            # Synthetic component model of PR49's cross-ledger settlement:
            # an authenticated cancelled turn changes the tracked execution
            # only after its cooperative cancellation request.
            for turn in self.agent_turns:
                report = turn.get("report", turn)
                if report.get("state") != "cancelled":
                    continue
                for execution in self.executions.values():
                    if (execution.get("semantic_execution_id") == report.get("execution_id")
                            and execution.get("generation") == report.get("generation")
                            and execution.get("cancel_requested") is True):
                        execution["state"] = "cancelled"
            return {"turns": [turn for turn in self.agent_turns if turn["revision"] > since]}
        if method == "workspace.get":
            workspace = self.workspaces.get(params["workspace_id"])
            if not workspace:
                raise RuntimeError("workspace_not_found")
            return {"workspace": workspace}
        if method == "workspace.create":
            self.seq += 1
            wid = f"w{self.seq}"
            tid = f"{wid}:t1"
            pid = f"{wid}:p1"
            workspace = {"workspace_id": wid, "label": params["label"], "number": len(self.workspaces) + 1}
            tab = {"tab_id": tid, "workspace_id": wid, "label": params.get("label", "1"), "number": 1, "pane_count": 1}
            pane = {"pane_id": pid, "workspace_id": wid, "tab_id": tid, "agent_status": "idle", "cwd": params.get("cwd")}
            self.workspaces[wid] = workspace
            self.tabs[tid] = tab
            self.panes[pid] = pane
            self.active_workspace_id = wid
            return {"workspace": workspace, "tab": tab, "root_pane": pane}
        if method == "tab.create":
            self.seq += 1
            wid = params["workspace_id"]
            tid = f"{wid}:t{self.seq}"
            pid = f"{wid}:p{self.seq}"
            tab = {"tab_id": tid, "workspace_id": wid, "label": params.get("label"), "number": len([t for t in self.tabs.values() if t["workspace_id"] == wid]) + 1, "pane_count": 1}
            pane = {"pane_id": pid, "workspace_id": wid, "tab_id": tid, "agent_status": "idle", "cwd": params.get("cwd")}
            self.tabs[tid] = tab
            self.panes[pid] = pane
            return {"tab": tab, "root_pane": pane}
        if method == "workspace.rename":
            self.workspaces[params["workspace_id"]]["label"] = params["label"]
            return {}
        if method == "workspace.move":
            moved = self.workspaces[params["workspace_id"]]
            for workspace in self.workspaces.values():
                if workspace is not moved:
                    workspace["number"] += 1
            moved["number"] = params["insert_index"] + 1
            return {}
        if method == "tab.rename":
            self.tabs[params["tab_id"]]["label"] = params["label"]
            return {}
        if method == "tab.move":
            self.tabs[params["tab_id"]]["number"] = params["insert_index"] + 1
            return {}
        if method == "tab.get":
            tab = self.tabs.get(params["tab_id"])
            if not tab:
                raise RuntimeError("tab_not_found")
            return {"tab": tab}
        if method == "pane.get":
            pane = self.panes.get(params["pane_id"])
            if not pane:
                raise RuntimeError("pane_not_found")
            return {"pane": pane}
        if method == "tab.close":
            tab = self.tabs.pop(params["tab_id"], None)
            if not tab:
                raise RuntimeError("tab_not_found")
            for pane_id in [p for p, value in self.panes.items() if value["tab_id"] == params["tab_id"]]:
                self.panes.pop(pane_id)
            return {}
        if method == "pane.read":
            return {"text": self.panes[params["pane_id"]].get("output", "bounded output")}
        if method == "agent.start":
            pane = self.panes[params["pane_id"]]
            self.seq += 1
            pane["agent_name"] = params["name"]
            pane["agent_status"] = "idle"
            pane["state_change_seq"] = self.seq
            args = list(params.get("args") or [])
            pane["process_name"] = "codex"
            pane["process_argv"] = ["codex", *args]
            if len(args) >= 2 and args[-2] == "resume":
                pane["agent_session"] = {"value": args[-1]}
            return {"agent": {"agent_session": {"value": f"session-{params['name']}"}}}
        if method == "agent.get":
            pane = next(
                (
                    value
                    for value in self.panes.values()
                    if value.get("agent_name") == params["target"]
                    or value.get("pane_id") == params["target"]
                ),
                None,
            )
            if pane is None:
                raise RuntimeError("agent_not_found")
            return {
                "agent": {
                    "agent": "codex",
                    "name": pane.get("agent_name", params["target"]),
                    "pane_id": pane["pane_id"],
                    "agent_status": pane.get("agent_status", "idle"),
                    "interactive_ready": True,
                    "state_change_seq": pane.get("state_change_seq", 0),
                    "agent_session": pane.get("agent_session", {}),
                }
            }
        if method == "agent.read":
            pane = next(value for value in self.panes.values() if value.get("pane_id") == params["target"])
            return {"read": {"text": pane.get("output", "")}}
        if method == "pane.process_info":
            pane = self.panes[params["pane_id"]]
            return {
                "process_info": {
                    "foreground_processes": [
                        {"name": pane.get("process_name", "zsh"), "argv": pane.get("process_argv", [pane.get("process_name", "zsh")])}
                    ]
                }
            }
        if method == "pane.send_text":
            self.panes[params["pane_id"]]["sent_text"] = params["text"]
            return {}
        if method == "pane.send_keys":
            pane = self.panes[params["pane_id"]]
            if "enter" in params.get("keys", []) and pane.get("sent_text"):
                self.seq += 1
                pane["agent_status"] = "working"
                pane["state_change_seq"] = self.seq
                pane["process_name"] = "codex"
                process_argv = shlex.split(pane.get("sent_text", "codex"))
                if process_argv[:1] == ["/usr/bin/env"]:
                    process_argv = process_argv[2:]
                pane["process_argv"] = process_argv
            return {}
        if method == "pane.close":
            pane = self.panes.pop(params["pane_id"], None)
            if pane is None:
                raise RuntimeError("pane_not_found")
            return {}
        if method == "pane.report_agent":
            pane = self.panes[params["pane_id"]]
            pane["agent_status"] = params["state"]
            self.seq += 1
            pane["state_change_seq"] = self.seq
            return {"pane": pane}
        if method == "pane.report_agent_session":
            pane = self.panes[params["pane_id"]]
            # Match Herdr session_ref_from_report: custom lifecycle source
            # names do not authorize resumable Codex session metadata.
            if params.get("source") == "herdr:codex" and params.get("agent") == "codex":
                pane["agent_session"] = {"value": params.get("agent_session_id")}
            return {"pane": pane}
        if method == "agent.prompt":
            pane = next(
                value
                for value in self.panes.values()
                if value.get("agent_name") == params["target"]
                or value.get("pane_id") == params["target"]
            )
            self.seq += 1
            pane["agent_status"] = "working"
            pane["state_change_seq"] = self.seq
            pane["prompt_text"] = params["text"]
            return {"agent": pane}
        if method == "agent.rename":
            pane = next(value for value in self.panes.values() if value["pane_id"] == params["target"])
            pane["agent_name"] = params["name"]
            pane["process_name"] = "codex"
            return {"agent": {"name": params["name"]}}
        if method == "session.snapshot":
            return {
                "snapshot": {
                    "workspaces": list(self.workspaces.values()),
                    "tabs": list(self.tabs.values()),
                    "panes": list(self.panes.values()),
                    "agents": [],
                }
            }
        return {}


class TestBroker(Broker):
    __test__ = False

    def __init__(self, root: Path, clock=None, cleanup_delay=0.01):
        super().__init__(
            root / "control.sock",
            root / "state.sqlite3",
            root / "herdr.sock",
            root / "config.json",
            clock=clock or __import__("time").time,
            cleanup_delay=cleanup_delay,
        )
        self.herdr = FakeHerdr()
        self.alerts = []
        self.native_turn_status: dict[str, str] = {}
        self.appserver_calls: list[tuple[list[str], dict[str, str] | None]] = []

    async def emit_attention(self, row):
        self.alerts.append((row["id"], row["state"], row["summary"]))

    async def _create_native_worker(self, row, text):
        return "01a07cad-d970-7393-82a4-14ae9a1c16ee", "fake-initial-turn"

    async def _start_native_turn(self, row, text, *, client_user_message_id=None):
        await self.herdr.request(
            "agent.prompt", {"target": row["agent_name"] or row["pane_id"], "text": text}
        )
        return "fake-turn"

    async def _run_json_required(self, argv, timeout=30, env=None):
        self.appserver_calls.append((list(argv), env))
        if "steer" in argv:
            return {
                "thread_id": argv[argv.index("--thread-id") + 1],
                "turn_id": argv[argv.index("--expected-turn-id") + 1],
                "client_user_message_id": argv[argv.index("--client-user-message-id") + 1],
                "delivery": "accepted",
            }
        if "queue-list" in argv:
            return {"thread_id": argv[argv.index("--thread-id") + 1], "submissions": []}
        if "queue-delete" in argv:
            return {
                "thread_id": argv[argv.index("--thread-id") + 1],
                "queued_submission_id": argv[argv.index("--queued-submission-id") + 1],
                "deleted": True,
            }
        if "status" in argv:
            turn_id = argv[argv.index("--turn-id") + 1]
            return {"turn_id": turn_id, "turn_status": self.native_turn_status.get(turn_id, "inProgress")}
        raise AssertionError(f"unexpected app-server invocation: {argv}")


class StateTests(unittest.TestCase):
    def test_terminal_result_is_transactionally_journaled_and_delivered(self):
        with tempfile.TemporaryDirectory() as raw:
            db = StateDB(Path(raw) / "state.sqlite3")
            task = db.add_task({"id": "t1", "target": "t1", "cwd": raw, "prompt": "x",
                                "summary": "queued", "parent_id": None, "priority": "routine"})
            done = db.update(task["id"], event="control_report_completed", state="completed",
                             summary="durable result", terminal_reported_at=1, finished_at=1)
            self.assertIsNotNone(done["terminal_event_id"])
            events = db.pending_completions()
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["payload"]["summary"], "durable result")
            delivered = db.deliver_inbox()
            self.assertEqual(delivered[0]["delivery_state"], "delivered")
            db.ack_completion(events[0]["event_id"], "consumed", "manager_mcp", "turn-1", "turn-1")
            row = db.conn.execute("SELECT * FROM completion_outbox").fetchone()
            self.assertIsNotNone(row["consumed_at"])
            self.assertIsNone(row["response_produced_at"])
            self.assertIsNone(row["client_delivered_at"])
            db.close()

    def test_outbox_lease_replays_and_continuation_supersedes(self):
        with tempfile.TemporaryDirectory() as raw:
            current = [1000.0]
            db = StateDB(Path(raw) / "state.sqlite3", clock=lambda: current[0])
            task = db.add_task({"id": "t2", "target": "t2", "cwd": raw, "prompt": "x",
                                "summary": "queued", "parent_id": None, "priority": "routine"})
            done = db.update(task["id"], state="completed", summary="first", finished_at=current[0])
            first = db.lease_outbox()
            self.assertIsNotNone(first)
            self.assertIsNone(db.lease_outbox())
            current[0] += OUTBOX_LEASE_SECONDS + 1
            replay = db.lease_outbox()
            self.assertEqual(replay["event_id"], first["event_id"])
            db.update(done["id"], event="followup_queued", state="working",
                      run_generation=1, terminal_event_id=None, summary="continued")
            journal = db.conn.execute("SELECT obsolete_reason FROM event_journal").fetchone()
            self.assertEqual(journal["obsolete_reason"], "superseded_by_continuation")
            self.assertEqual(db.pending_completions(include_consumed=True), [])
            db.close()

    def test_duplicate_ack_is_idempotent_and_out_of_order_rejected(self):
        with tempfile.TemporaryDirectory() as raw:
            db = StateDB(Path(raw) / "state.sqlite3")
            task = db.add_task({"id": "t3", "target": "t3", "cwd": raw, "prompt": "x",
                                "summary": "queued", "parent_id": None, "priority": "routine"})
            done = db.update(task["id"], state="completed", summary="done", finished_at=1)
            event_id = done["terminal_event_id"]
            with self.assertRaisesRegex(ValueError, "out of order"):
                db.ack_completion(event_id, "consumed", "manager_mcp", "x", None)
            db.deliver_inbox()
            db.ack_completion(event_id, "consumed", "manager_mcp", "same", None)
            db.ack_completion(event_id, "consumed", "manager_mcp", "same", None)
            count = db.conn.execute("SELECT count(*) n FROM completion_acks WHERE stage='consumed'").fetchone()["n"]
            self.assertEqual(count, 1)
            db.close()

    def test_interrupted_manager_reply_and_cleaned_pane_survive_restart(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "state.sqlite3"
            db = StateDB(path)
            task = db.add_task({"id": "t4", "target": "t4", "cwd": raw, "prompt": "x",
                                "summary": "queued", "parent_id": None, "priority": "routine"})
            done = db.update(task["id"], state="completed", summary="survives cleanup", finished_at=1)
            event_id = done["terminal_event_id"]
            db.deliver_inbox()
            db.update(task["id"], herdr_state="closed", pane_id=None, tab_id=None,
                      session_state="dormant", event="owned_tab_cleaned")
            db.close()
            reopened = StateDB(path)
            pending = reopened.pending_completions(include_consumed=True)
            self.assertEqual([item["event_id"] for item in pending], [event_id])
            self.assertEqual(pending[0]["delivery_state"], "delivered")
            self.assertEqual(pending[0]["payload"]["summary"], "survives cleanup")
            reopened.ack_completion(event_id, "consumed", "manager_mcp", "after-reconnect", None)
            state = reopened.conn.execute("SELECT * FROM completion_outbox").fetchone()
            self.assertIsNotNone(state["consumed_at"])
            self.assertIsNone(state["client_delivered_at"])
            reopened.close()

    def test_response_production_requires_result_identity_and_is_not_client_delivery(self):
        with tempfile.TemporaryDirectory() as raw:
            db = StateDB(Path(raw) / "state.sqlite3")
            task = db.add_task({"id": "t5", "target": "t5", "cwd": raw, "prompt": "x",
                                "summary": "queued", "parent_id": None, "priority": "routine"})
            done = db.update(task["id"], state="completed", summary="answer", finished_at=1)
            self.assertEqual(db.record_manager_response("turn-a", "msg-a", "unrelated"), [])
            matched = db.record_manager_response("turn-b", "msg-b", "Task t5 completed: answer")
            self.assertEqual(matched, [done["terminal_event_id"]])
            outbox = db.conn.execute("SELECT * FROM completion_outbox").fetchone()
            self.assertIsNotNone(outbox["response_produced_at"])
            self.assertIsNone(outbox["client_delivered_at"])
            db.ack_completion(done["terminal_event_id"], "consumed", "delegated_manager_receipt", "receipt", None)
            after = db.conn.execute("SELECT state FROM completion_outbox").fetchone()
            self.assertEqual(after["state"], "response_produced")
            db.close()

    def test_voice_presentation_is_id_safe_and_semantic_duplicates_are_consumed(self):
        with tempfile.TemporaryDirectory() as raw:
            db = StateDB(Path(raw) / "state.sqlite3")
            for ident in ("t6a", "t6b"):
                task = db.add_task({"id": ident, "target": ident, "cwd": raw, "prompt": "x",
                                    "summary": "queued", "parent_id": None, "priority": "normal"})
                db.update(task["id"], state="completed",
                          summary="Built /secret/path for evt-deadbeef-1234-5678-9999-deadbeefdead abcdef0123456789",
                          finished_at=1)
            first = db.deliver_inbox(limit=2)
            self.assertTrue(first[0]["presentation"]["material"])
            spoken = " ".join(str(value) for key, value in first[0]["presentation"].items()
                              if key != "semantic_key")
            self.assertNotRegex(spoken, r"evt-|[0-9a-f]{16,}|/secret/path")
            db.ack_completion(first[0]["event_id"], "consumed", "manager_mcp", "turn-a", "turn-a")
            replay = db.deliver_inbox(limit=2)
            duplicate = next(item for item in replay if item["event_id"] != first[0]["event_id"])
            self.assertFalse(duplicate["presentation"]["material"])
            db.ack_completion(duplicate["event_id"], "consumed", "manager_mcp", "turn-b", "turn-b")
            self.assertEqual(len(db.pending_completions(include_consumed=True)), 2)
            db.close()

    def test_voice_presentation_distinguishes_validation_from_merge(self):
        validation = completion_presentation({"state": "completed", "summary": "Focused test suite passed."}, "normal", stage="tests")
        merged = completion_presentation({"state": "completed", "summary": "Authorized change merged successfully."}, "normal", stage="merge", evidence_kind="merge")
        self.assertEqual(validation["kind"], "validation_work")
        self.assertEqual(merged["kind"], "merge")
        self.assertNotEqual(validation["outcome"], merged["outcome"])
        self.assertNotEqual(validation["semantic_key"], merged["semantic_key"])

    def test_voice_presentation_treats_recovery_summary_as_unverified_without_typed_evidence(self):
        presentation = completion_presentation({"state": "completed", "summary": "Recovery harness verified reconnect/re-observe without shared restart evt-12345678-abcd."}, "normal")
        self.assertEqual(presentation["kind"], "unverified_completion")
        self.assertIn("not independently verified", presentation["outcome"])
        self.assertNotIn("evt-", " ".join(str(value) for value in presentation.values()))

    def test_voice_presentation_treats_command_summary_as_unverified_without_typed_evidence(self):
        presentation = completion_presentation({"state": "completed", "summary": "Command completed successfully (native exit 0)."}, "normal")
        self.assertEqual(presentation["kind"], "unverified_completion")

    def test_command_presentation_requires_structural_command_exit_not_quoted_example(self):
        quoted = completion_presentation({"state": "completed", "summary": "Example: Command completed successfully (native exit 0)."}, "normal", work_kind="codex")
        structural = completion_presentation({"state": "completed", "summary": "unrelated"}, "normal", work_kind="command", command_exit_status=0)
        self.assertEqual(quoted["kind"], "unverified_completion")
        self.assertEqual(structural["kind"], "command_exit")

    def test_voice_presentation_does_not_trust_dependency_repair_summary_without_typed_context(self):
        presentation = completion_presentation({
            "state": "completed",
            "summary": "Dependency deadlock fixed and tests stage admitted for the native delivery.",
        }, "attention")
        spoken = " ".join(str(value) for key, value in presentation.items() if key != "semantic_key")
        self.assertEqual(presentation["kind"], "unverified_completion")
        self.assertIn("not independently verified", spoken)
        self.assertNotRegex(spoken, r"evt-|[0-9a-f]{16,}|/")

    def test_typed_stage_prevents_false_install_and_uat_from_negative_summary(self):
        summary = ("Independent tests stage completed; no installs, deployments, or UAT were performed. "
                   "Historical merged PR checks include test installation methods; current Herdr target is unmerged and release remains pending.")
        tests = completion_presentation({"state": "completed", "summary": summary}, "attention", stage="tests")
        install = completion_presentation({"state": "completed", "summary": "No install was performed."}, "attention", stage="install")
        uat = completion_presentation({"state": "completed", "summary": "No install/UAT yet."}, "attention", stage="live_uat")
        self.assertEqual(tests["kind"], "validation_work")
        self.assertEqual(install["kind"], "install_pending")
        self.assertEqual(uat["kind"], "uat_pending")
        self.assertNotIn("installed and verified", install["outcome"].lower())
        self.assertNotIn("acceptance testing completed", uat["outcome"].lower())

    def test_wrong_target_review_cannot_be_promoted_by_summary_keywords(self):
        review = completion_presentation({"state": "completed", "summary": "Reviewed unrelated merged PR and found install/UAT gaps."}, "attention", stage="review")
        self.assertEqual(review["kind"], "review_pending")
        self.assertNotIn("review completed", review["outcome"].lower())

    def test_installed_shared_policy_requires_voice_safe_presentations(self):
        policy = json.loads((Path(__file__).parents[1] / "control-policy.json").read_text())
        presentation = policy["presentation_policy"]
        self.assertTrue(presentation["voice_summary_only"])
        self.assertTrue(presentation["deduplicate_semantic_completions"])

    def test_structured_response_correlation_needs_no_spoken_identifier(self):
        with tempfile.TemporaryDirectory() as raw:
            db = StateDB(Path(raw) / "state.sqlite3")
            task = db.add_task({"id": "t7", "target": "t7", "cwd": raw, "prompt": "x",
                                "summary": "queued", "parent_id": None, "priority": "routine"})
            done = db.update(task["id"], state="completed", summary="done", finished_at=1)
            matched = db.record_manager_response("turn", "item", "The requested work completed.",
                                                 [done["terminal_event_id"]])
            self.assertEqual(matched, [done["terminal_event_id"]])
            self.assertIsNotNone(db.conn.execute("SELECT response_produced_at FROM completion_outbox").fetchone()[0])
            db.close()

    def test_priority_queue_and_dedupe(self):
        with tempfile.TemporaryDirectory() as raw:
            db = StateDB(Path(raw) / "state.sqlite3")
            base = {"cwd": raw, "prompt": "x", "summary": "queued", "parent_id": None}
            for ident, priority in (("r", "routine"), ("n", "normal"), ("c", "critical")):
                db.add_task({**base, "id": ident, "target": ident, "priority": priority})
            self.assertEqual(db.next_queued_codex()["id"], "c")
            self.assertTrue(db.should_notify("c", "blocked", "summary", "question"))
            self.assertFalse(db.should_notify("c", "blocked", "summary", "question"))
            db.record_manager_queue("c", "blocked", "summary", "question")
            note = db.conn.execute(
                "SELECT repeat_count,manager_queue_count FROM notifications WHERE task_id='c'"
            ).fetchone()
            self.assertEqual((note["repeat_count"], note["manager_queue_count"]), (2, 1))
            db.close()

    def test_thirty_day_session_expiry_uses_injected_clock(self):
        with tempfile.TemporaryDirectory() as raw:
            current = [1_000_000.0]
            db = StateDB(Path(raw) / "state.sqlite3", clock=lambda: current[0])
            db.add_task(
                {
                    "id": "retained",
                    "target": "retained",
                    "cwd": raw,
                    "prompt": "x",
                    "summary": "queued",
                    "parent_id": None,
                    "priority": "normal",
                    "work_kind": "codex",
                }
            )
            db.update(
                "retained", state="completed", agent_session_id="session-value", finished_at=current[0]
            )
            current[0] += 30 * 86400 + 1
            db.prune()
            row = db.task("retained")
            self.assertEqual(row["session_state"], "expired")
            self.assertIsNone(row["agent_session_id"])
            db.close()

    def test_feature_claim_survives_restart_and_never_double_dispatches(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "state.sqlite3"
            db = StateDB(path)
            db.create_feature({"feature_id": "restart-safe", "target": "restart-safe", "cwd": raw,
                               "title": "restart", "scope": "end_to_end", "required_stages": ["merge"],
                               "actions": {"merge": {"prompt": "merge once"}}})
            first = db.claim_next_feature_action("restart-safe")
            self.assertEqual(first["stage"], "merge")
            db.close()
            reopened = StateDB(path)
            self.assertIsNone(reopened.claim_next_feature_action("restart-safe"))
            stage = next(s for s in reopened.feature("restart-safe")["stages"] if s["stage"] == "merge")
            self.assertEqual(stage["state"], "claimed")
            self.assertEqual(stage["attempts"], 1)
            reopened.close()

    def test_portable_bootstrap_uses_relocated_root_and_state_without_home(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw) / "different-checkout"
            source = Path(__file__).parents[1]
            state = Path(raw) / "relocated-state"
            config = state / "machine.json"
            appserver_socket = Path(raw) / "owned-appserver.sock"
            env = os.environ | {
                "CODEX_CONTROL_ROOT": str(root),
                "CODEX_CONTROL_STATE_DIR": str(state),
                "CODEX_APP_SERVER_SOCKET": str(appserver_socket),
            }
            installed = subprocess.run(["python3", str(source / "runtime/control_portable.py"), "install", "--source", str(source), "--target", str(root)], env=env, text=True, capture_output=True, check=True)
            self.assertEqual(json.loads(installed.stdout)["installed_root"], str(root))
            result = subprocess.run(["python3", str(root / "runtime/control_portable.py"), "bootstrap", "--root", str(root), "--state-dir", str(state)], env=env, text=True, capture_output=True, check=True)
            saved = json.loads(result.stdout)
            self.assertEqual(saved["control_root"], str(root))
            self.assertEqual(saved["state_dir"], str(state))
            self.assertEqual(saved["policy_file"], str(root / "POLICY.md"))
            self.assertEqual(saved["app_server_socket"], str(appserver_socket))
            self.assertEqual(saved["app_server_remote"], f"unix://{appserver_socket}")
            self.assertEqual(saved["state_schema_version"], 12)
            self.assertTrue(config.exists())
            self.assertNotIn(str(Path.home()), config.read_text())

    def test_portable_upgrade_does_not_replace_existing_manager_config(self):
        with tempfile.TemporaryDirectory() as raw:
            source = Path(__file__).parents[1]
            root = Path(raw) / "control-manager-new"
            state = Path(raw) / "new-state"
            config = Path(raw) / "existing-machine.json"
            original = {
                "manager_cwd": "/durable/original-manager-cwd",
                "control_root": "/durable/original-manager-root",
                "operator_setting": "preserve-me",
            }
            config.write_text(json.dumps(original, sort_keys=True) + "\n")
            before = config.read_bytes()
            env = os.environ | {"CODEX_CONTROL_ROOT": str(root), "CODEX_CONTROL_STATE_DIR": str(state)}
            subprocess.run([
                "python3", str(source / "runtime/control_portable.py"), "install",
                "--source", str(source), "--target", str(root),
            ], env=env, text=True, capture_output=True, check=True)
            result = subprocess.run([
                "python3", str(root / "runtime/control_portable.py"), "bootstrap",
                "--root", str(root), "--state-dir", str(state), "--config", str(config),
            ], env=env, text=True, capture_output=True, check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("refusing to overwrite existing machine config", result.stderr)
            self.assertEqual(config.read_bytes(), before)
            self.assertEqual(json.loads(config.read_text())["manager_cwd"], original["manager_cwd"])

    def test_newer_state_schema_fails_closed(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "state.sqlite3"
            db = StateDB(path)
            db.conn.execute("UPDATE metadata SET value='999' WHERE key='schema_version'")
            db.conn.commit()
            db.close()
            with self.assertRaisesRegex(RuntimeError, "newer"):
                StateDB(path)

    def test_xcsh_semantic_turn_requires_identity_digest_and_global_revision(self):
        with tempfile.TemporaryDirectory() as raw:
            db = StateDB(Path(raw) / "state.sqlite3")
            task = db.add_task({"id": "xcsh-turn", "target": "xcsh-turn", "cwd": raw, "prompt": "x", "summary": "queued", "parent_id": None, "priority": "normal", "work_kind": "xcsh"})
            db.update(task["id"], state="working", pane_id="pane-1", agent_session_id="session-1", native_turn_id="turn-1")
            # Synthetic component fixture: it builds the same immutable
            # admission ledger as execution.resume, rather than injecting a
            # fabricated semantic journal record as acceptance evidence.
            executable = str(Path("/bin/true").resolve())
            executable_sha256 = hashlib.sha256(Path(executable).read_bytes()).hexdigest()
            session_dir = Path(raw) / "sessions"; session_dir.mkdir()
            session_path = session_dir / "0123abcd4567ef89.jsonl"
            first_line = b'{"type":"session","id":"0123abcd4567ef89"}\n'; session_path.write_bytes(first_line)
            launch = {"version":3,"xcsh_executable":executable,"session_dir":str(session_dir),"session_path":str(session_path),"session_header":{"id":"0123abcd4567ef89","sha256":hashlib.sha256(first_line).hexdigest()},"model":"test/model","discovery":"reduced-v1","tools":"read","interactive":False,"lifecycle_mode":"managed_turn_v1"}
            request = {"execution_id": task["id"], "generation": 0, "native_launch": launch, "workspace_id": "w1", "cwd": raw, "text": "x", "xcsh_executable_sha256": executable_sha256}
            encoded = json.dumps(request, sort_keys=True, separators=(",", ":"))
            db.claim_native_generation(task["id"], generation=0, session_id=launch["session_header"]["id"], workspace_id="w1", request_sha256=hashlib.sha256(encoded.encode()).hexdigest(), request_json=encoded)
            argv = [executable,"--mode","json","--session-dir",str(session_dir),"--resume",str(session_path),"--model","test/model","--tools","read","--no-mcp","--no-lsp","--no-memories","--no-skills","--no-rules","--no-pty","--print","x"]
            receipt = {"execution_id": "backend-1", "backend_execution_id": "backend-1", "semantic_execution_id": task["id"], "generation": 0, "native_producer": "xcsh", "producer_session_id": launch["session_header"]["id"], "workspace_id": "w1", "cwd": raw, "native_launch": launch, "command": {"mode": "argv", "argv": argv}, "native_executable": {"canonical_path": executable, "sha256": executable_sha256}, "injected_env": {"HERDR_EXECUTION_ID": task["id"], "HERDR_EXECUTION_GENERATION": "0"}, "tab_id": "tab-1", "pane_id": "pane-1"}
            # Released protocol 22 omitted workspace_id even though the
            # manager had already claimed it. Keep that precise receipt shape
            # rejected; protocol 23 must carry the field rather than weaken
            # the immutable workspace comparison.
            released_receipt = receipt.copy()
            del released_receipt["workspace_id"]
            with self.assertRaisesRegex(ValueError, "immutable native generation provenance"):
                db.admit_native_generation(task["id"], 0, released_receipt)
            admitted = db.admit_native_generation(task["id"], 0, receipt)
            self.assertEqual(admitted["backend_execution_id"], "backend-1")
            # Reconciliation of the same response remains idempotent and
            # cannot allocate a duplicate native generation or child.
            replay = db.admit_native_generation(task["id"], 0, receipt)
            self.assertEqual(replay["backend_execution_id"], "backend-1")
            self.assertEqual(db.conn.execute("SELECT COUNT(*) FROM native_execution_generations WHERE task_id=?", (task["id"],)).fetchone()[0], 1)
            base = {"execution_id": "xcsh-turn", "pane_id": "pane-1", "producer": "xcsh", "session_id": launch["session_header"]["id"], "turn_id": "turn-1", "generation": 0}
            self.assertEqual(db.apply_native_turn({"revision": 1, "report": base | {"event_revision": 1, "state": "starting"}}), ("xcsh-turn", "starting"))
            result = "accepted semantic result"
            self.assertEqual(db.apply_native_turn({"revision": 2, "report": base | {"event_revision": 2, "state": "completed", "result": result, "result_digest": hashlib.sha256(result.encode()).hexdigest()}}), ("xcsh-turn", "completed"))
            self.assertIsNone(db.apply_native_turn({"revision": 2, "report": base | {"event_revision": 2, "state": "completed", "result": result, "result_digest": hashlib.sha256(result.encode()).hexdigest()}}))
            self.assertEqual(db.task("xcsh-turn")["state"], "completed")
            db.close()

    def test_feature_dependency_and_waiting_child_block_without_duplicate_action(self):
        with tempfile.TemporaryDirectory() as raw:
            db = StateDB(Path(raw) / "state.sqlite3")
            child = db.add_task({"id": "native-child", "target": "native-child", "cwd": raw, "prompt": "x",
                                 "summary": "needs governance", "question": "approve message", "parent_id": None,
                                 "priority": "attention"})
            db.update(child["id"], state="waiting_human", summary="needs governance", question="approve message")
            db.create_feature({"feature_id": "native-feature", "target": "native-feature", "cwd": raw,
                               "title": "Native", "scope": "end_to_end", "required_stages": ["implementation", "tests"],
                               "children": {"implementation": child["id"]},
                               "actions": {"tests": {"prompt": "must not dispatch"}}})
            db.sync_feature_child_state(child["id"])
            feature = db.ensure_feature_dependency("native-feature", "native_consumer", before_stage="tests",
                                                   blocker="consumer disabled pending protocol-20 deployment")
            feature = db.ensure_feature_dependency("native-feature", "native_consumer", before_stage="tests",
                                                   blocker="consumer disabled pending protocol-20 deployment")
            stages = {stage["stage"]: stage for stage in feature["stages"]}
            self.assertEqual(stages["implementation"]["state"], "blocked")
            self.assertIn("approve message", stages["implementation"]["blocker"])
            self.assertEqual(stages["native_consumer"]["state"], "blocked")
            self.assertEqual(len([stage for stage in feature["stages"] if stage["stage"] == "native_consumer"]), 1)
            self.assertIsNone(db.claim_next_feature_action("native-feature"))
            db.close()

    def test_existing_installed_dependency_moves_before_live_uat_without_duplication(self):
        with tempfile.TemporaryDirectory() as raw:
            db = StateDB(Path(raw) / "state.sqlite3")
            db.create_feature({"feature_id": "graph", "target": "g", "cwd": raw,
                               "title": "graph", "scope": "end_to_end"})
            db.ensure_feature_dependency("graph", "native_consumer", before_stage="tests",
                                         blocker="await installed capability")
            feature = db.ensure_feature_dependency("graph", "native_consumer", before_stage="live_uat",
                                                   blocker="await installed capability")
            names = [stage["stage"] for stage in feature["stages"]]
            self.assertEqual(names.count("native_consumer"), 1)
            self.assertEqual(names.index("native_consumer"), names.index("live_uat") - 1)
            self.assertEqual(next(stage for stage in feature["stages"] if stage["stage"] == "native_consumer")["state"], "blocked")
            db.close()

    def test_feature_observation_is_durable_but_does_not_pass_gate(self):
        with tempfile.TemporaryDirectory() as raw:
            db = StateDB(Path(raw) / "state.sqlite3")
            db.create_feature({"feature_id": "obs", "target": "o", "cwd": raw,
                               "title": "obs", "scope": "end_to_end"})
            db.record_feature_observation("obs", "ci", "github_ci", "run-1", "successful PR checks")
            feature = db.feature("obs")
            self.assertEqual(feature["observations"][0]["evidence_id"], "run-1")
            self.assertEqual(next(x for x in feature["stages"] if x["stage"] == "ci")["state"], "pending")
            db.close()

    def test_feature_prompt_binding_retains_authoritative_artifact_identity(self):
        with tempfile.TemporaryDirectory() as raw:
            db = StateDB(Path(raw) / "state.sqlite3")
            db.create_feature({"feature_id": "bound", "target": "target", "cwd": raw,
                               "title": "Semantic turns", "scope": "end_to_end"})
            db.record_feature_observation("bound", "tests", "github_pr", "xcsh-immutable-head", "merged source")
            binding = db.feature_prompt_binding("bound")
            self.assertEqual(binding["feature_id"], "bound")
            self.assertEqual(binding["artifact_identities"][0]["evidence_id"], "xcsh-immutable-head")
            db.close()

    def test_unsatisfied_gate_can_retry_once_after_scope_mismatch(self):
        with tempfile.TemporaryDirectory() as raw:
            db = StateDB(Path(raw) / "state.sqlite3")
            child = db.add_task({"id": "wrong-review", "target": "other", "cwd": raw, "prompt": "x",
                                 "summary": "queued", "parent_id": None, "priority": "normal"})
            db.create_feature({"feature_id": "retry", "target": "r", "cwd": raw, "title": "retry",
                               "scope": "end_to_end", "children": {"review": child["id"]}})
            db.update(child["id"], state="completed", terminal_reported_at=db.clock())
            db.feature_task_terminal(child["id"], "completed", "reviewed wrong target")
            db.retry_feature_stage("retry", "review", "review result addressed a different target")
            stage = next(x for x in db.feature("retry")["stages"] if x["stage"] == "review")
            self.assertEqual(stage["state"], "pending")
            self.assertIsNone(stage["task_id"])
            self.assertEqual(stage["attempts"], 0)
            self.assertEqual(db.feature("retry")["observations"][0]["kind"], "scope_mismatch")
            db.close()

    def test_retry_rejects_resumed_active_child_atomically(self):
        with tempfile.TemporaryDirectory() as raw:
            db = StateDB(Path(raw) / "state.sqlite3")
            child = db.add_task({"id": "resumed-review", "target": "target", "cwd": raw, "prompt": "x",
                                 "summary": "queued", "parent_id": None, "priority": "normal"})
            db.create_feature({"feature_id": "active-retry", "target": "target", "cwd": raw,
                               "title": "active", "scope": "end_to_end", "children": {"review": child["id"]}})
            db.feature_task_terminal(child["id"], "completed", "first review")
            db.conn.execute("UPDATE tasks SET state='working',run_generation=1,terminal_reported_at=NULL WHERE id=?", (child["id"],))
            db.conn.commit()
            with self.assertRaisesRegex(ValueError, "active or resumed"):
                db.retry_feature_stage("active-retry", "review", "scope changed")
            stage = next(x for x in db.feature("active-retry")["stages"] if x["stage"] == "review")
            self.assertEqual(stage["task_id"], child["id"])
            self.assertEqual(stage["state"], "awaiting_evidence")
            db.close()


class BrokerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.broker = TestBroker(self.root)
        self.xcsh_executable = str(Path("/bin/true").resolve())
        self.xcsh_executable_sha256 = hashlib.sha256(Path(self.xcsh_executable).read_bytes()).hexdigest()
        self.session_dir = self.root / "sessions"
        self.session_dir.mkdir()
        self.session_path = self.session_dir / "0123abcd4567ef89.jsonl"
        self.session_path.write_bytes(b'{"type":"session","id":"0123abcd4567ef89"}\n')
        self.native_launch = {"version": 3, "xcsh_executable": self.xcsh_executable,
            "session_dir": str(self.session_dir.resolve()), "session_path": str(self.session_path.resolve()),
            "session_header": {"id": "0123abcd4567ef89", "sha256": hashlib.sha256(self.session_path.read_bytes()).hexdigest()},
            "model": "test/model", "discovery": "reduced-v1", "tools": "read", "interactive": False,
            "lifecycle_mode": "managed_turn_v1"}

    async def asyncTearDown(self):
        for task in self.broker.settle_timers.values():
            task.cancel()
        for task in self.broker.native_turn_timers.values():
            task.cancel()
        self.broker.db.close()
        self.tmp.cleanup()

    async def dispatch(self, target, priority="normal", parent_id=None):
        return await self.broker.dispatch(
            {
                "target": target,
                "cwd": str(self.root),
                "priority": priority,
                "prompt": f"do {target}",
                "parent_id": parent_id,
            }
        )

    async def admit_native_xcsh(self, params):
        params = params | {"runtime_identity": (params.get("runtime_identity", {}) | {"xcsh_model": self.native_launch["model"]})}
        return await self.broker.native_xcsh_admit(params | {
            "session_id": self.native_launch["session_header"]["id"], "native_launch": self.native_launch,
            "xcsh_executable_sha256": self.xcsh_executable_sha256,
        })

    async def test_routine_completion_is_automatically_dispatched_to_manager(self):
        self.broker.config_path.write_text(json.dumps({
            "manager_thread_id": "01a07cad-d970-7393-82a4-14ae9a1c16ee",
            "app_server_remote": "unix://",
            "codex_binary": "/bin/true",
        }))
        task = self.broker.db.add_task({"id": "routine", "target": "routine",
            "cwd": str(self.root), "prompt": "x", "summary": "queued",
            "parent_id": None, "priority": "routine"})
        done = self.broker.db.update(task["id"], event="control_report_completed",
            state="completed", summary="automatic durable result", finished_at=self.broker.clock())
        calls = []

        async def accepted(argv, timeout=30):
            calls.append(argv)
            self.broker.stopping.set()

        self.broker._run_required = accepted
        await asyncio.wait_for(self.broker._outbox_loop(), timeout=2)
        self.assertEqual(len(calls), 1)
        # The durable inbox is authoritative.  Queue prompts deliberately do
        # not expose opaque event IDs to a manager's user-facing context.
        self.assertIn("completion_inbox", calls[0][-1])
        self.assertNotIn(done["terminal_event_id"], calls[0][-1])
        outbox = self.broker.db.conn.execute(
            "SELECT state FROM completion_outbox WHERE event_id=?", (done["terminal_event_id"],)
        ).fetchone()
        self.assertEqual(outbox["state"], "dispatched")

    async def test_missing_historical_unknown_does_not_repeat_attention(self):
        task = self.broker.db.add_task({"id": "old-command", "target": "control",
            "cwd": str(self.root), "prompt": None, "summary": "queued",
            "parent_id": None, "priority": "normal", "work_kind": "command"})
        self.broker.db.update(task["id"], state="unknown", herdr_state="missing",
            pane_id="gone-pane", summary="Worker pane is absent after reconciliation.")
        await self.broker.reconcile()
        await self.broker.reconcile()
        self.assertEqual(self.broker.alerts, [])

    async def configure_control_workspace(self):
        created = await self.broker.herdr.request(
            "workspace.create", {"cwd": str(self.root), "label": "control", "focus": False}
        )
        self.broker.config_path.write_text(
            json.dumps(
                {
                    "control_root": str(self.root),
                    "manager_cwd": str(self.root),
                    "manager_workspace_id": created["workspace"]["workspace_id"],
                    "manager_tab_id": created["tab"]["tab_id"],
                    "manager_pane_id": created["root_pane"]["pane_id"],
                }
            )
        )
        return created

    async def test_validation_and_hierarchy(self):
        parent = await self.dispatch("parent")
        self.assertEqual(parent["model"], "gpt-5.6-sol")
        self.assertEqual(parent["reasoning_effort"], "low")
        child = await self.dispatch("child", parent_id=parent["id"])
        self.assertEqual(child["parent_id"], parent["id"])
        terra = await self.broker.dispatch(
            {
                "target": "terra",
                "cwd": str(self.root),
                "priority": "normal",
                "prompt": "balanced implementation",
                "model": "gpt-5.6-terra",
            }
        )
        self.assertEqual(terra["model"], "gpt-5.6-terra")
        self.assertEqual(terra["reasoning_effort"], "medium")
        luna = await self.broker.dispatch(
            {
                "target": "luna",
                "cwd": str(self.root),
                "priority": "routine",
                "prompt": "simple review",
                "model": "gpt-5.6-luna",
                "reasoning_effort": "low",
            }
        )
        self.assertEqual(luna["reasoning_effort"], "low")
        with self.assertRaisesRegex(ValueError, "does not exist"):
            await self.dispatch("orphan", parent_id="missing")
        with self.assertRaisesRegex(ValueError, "delegated model"):
            await self.broker.dispatch(
                {
                    "target": "astra-worker",
                    "cwd": str(self.root),
                    "priority": "normal",
                    "prompt": "x",
                    "model": "gpt-6-astra",
                }
            )
        with self.assertRaisesRegex(ValueError, "target must match"):
            await self.broker.dispatch(
                {"target": "bad target", "cwd": str(self.root), "priority": "normal", "prompt": "x"}
            )
        with self.assertRaisesRegex(ValueError, "absolute"):
            await self.broker.dispatch(
                {"target": "relative", "cwd": ".", "priority": "normal", "prompt": "x"}
            )

    async def test_unlimited_workers_start_without_capacity_queue(self):
        routine = await self.dispatch("routine", "routine")
        critical = await self.dispatch("critical", "critical")
        normal = await self.dispatch("normal", "normal")
        fourth = await self.dispatch("fourth", "routine")
        fifth = await self.dispatch("fifth", "routine")
        sixth = await self.dispatch("sixth", "routine")
        scheduler = asyncio.create_task(self.broker._scheduler())
        self.broker.scheduler_event.set()
        for _ in range(100):
            await asyncio.sleep(0.01)
            if self.broker.db.running_count() == 6:
                break
        states = {row["id"]: row["state"] for row in self.broker.db.list_tasks()}
        for task in (routine, critical, normal, fourth, fifth, sixth):
            self.assertEqual(states[task["id"]], "working")
        scheduler.cancel()
        await asyncio.gather(scheduler, return_exceptions=True)

    async def test_scheduler_interleaving_never_admits_real_command_as_codex(self):
        """A command row may exist while the Codex scheduler is already awake."""
        scheduler = asyncio.create_task(self.broker._scheduler())
        codex = await self.dispatch("parallel-codex")
        command_task = asyncio.create_task(
            self.broker.run_command(
                {"label": "interactive xcsh", "cwd": str(self.root), "shell": "zsh", "command": "xcsh"}
            )
        )
        self.broker.scheduler_event.set()
        command = await command_task
        for _ in range(100):
            await asyncio.sleep(0.01)
            if self.broker.db.task(codex["id"])["state"] == "working":
                break
        command_row = self.broker.db.task(command["id"])
        self.assertEqual(command_row["work_kind"], "command")
        self.assertEqual(command_row["state"], "working")
        self.assertIsNone(command_row["agent_kind"])
        self.assertIsNone(command_row["agent_session_id"])
        self.assertIsNone(command_row["native_turn_id"])
        events = [event["event"] for event in self.broker.db.transitions(command["id"])]
        self.assertIn("command_launch_claimed", events)
        self.assertNotIn("codex_dispatch_admitted", events)
        self.assertNotIn("native_turn_started", events)
        starts = [call for call in self.broker.herdr.calls if call[0] == "agent.start"]
        self.assertEqual(len(starts), 1)
        self.assertIn(["--remote", "unix://"], [starts[0][1]["args"][i:i + 2]
                                                for i in range(len(starts[0][1]["args"]) - 1)])
        self.assertIn(
            f'shell_environment_policy.set.HERDR_PANE_ID="{self.broker.db.task(codex["id"])["pane_id"]}"',
            starts[0][1]["args"],
        )
        codex_panes = [params for method, params in self.broker.herdr.calls
                       if method in {"workspace.create", "tab.create"}
                       and (params.get("env") or {}).get("CONTROL_TASK_ID") == codex["id"]]
        self.assertEqual(
            codex_panes[-1]["env"]["CODEX_HOME"],
            str(self.root / "remote-client-codex-home"),
        )
        self.assertEqual((self.root / "remote-client-codex-home" / "config.toml").read_text(), "")
        native_starts = [call for call in self.broker.herdr.calls if call[0] == "execution.start"]
        self.assertEqual(len(native_starts), 1)
        self.assertEqual(native_starts[0][1]["execution_id"], command["id"])
        scheduler.cancel()
        await asyncio.gather(scheduler, return_exceptions=True)

    async def test_native_launch_and_monitor_refuse_command_ownership(self):
        command = await self.broker.run_command(
            {"label": "owned command", "cwd": str(self.root), "shell": "bash", "command": "xcsh"}
        )
        row = self.broker.db.task(command["id"])
        with self.assertRaisesRegex(RuntimeError, "not Codex-owned"):
            await self.broker._start_task(row)
        await self.broker._monitor_native_turn(command["id"], "forged-turn")
        retained = self.broker.db.task(command["id"])
        self.assertEqual(retained["state"], "failed")
        self.assertIn("ownership boundary", retained["summary"])

    async def test_native_xcsh_admission_is_idempotent_and_semantic_only(self):
        workspace = await self.configure_control_workspace()
        params = {
            "target": "xcsh-uat", "cwd": str(self.root), "priority": "routine",
            "prompt": "safe synthesized prompt", "text": "safe synthesized prompt", "session_id": "session-1", "workspace_id": workspace["workspace"]["workspace_id"],
            "runtime_identity": {"xcsh_artifact": "xcsh@1#aaa", "herdr_artifact": "herdr@1#bbb", "manager_artifact": "manager@1#ccc"},
            "idempotency_key": "native-xcsh-admit-1",
        }
        admitted = await self.admit_native_xcsh(params)
        replay = await self.admit_native_xcsh(params)
        self.assertEqual(admitted["id"], replay["id"])
        self.assertTrue(replay["idempotency_replayed"])
        starts = [call for call in self.broker.herdr.calls if call[0] == "execution.resume"]
        self.assertEqual(len(starts), 1)
        self.assertEqual(starts[0][1]["generation"], 0)
        row = self.broker.db.task(admitted["id"])
        self.assertEqual(row["work_kind"], "xcsh")
        self.assertIn("xcsh@1#aaa", row["native_runtime_json"])
        base = {"execution_id": admitted["id"], "pane_id": row["pane_id"], "producer": "xcsh", "session_id": self.native_launch["session_header"]["id"], "turn_id": "turn-1", "generation": 0}
        self.assertEqual(self.broker.db.apply_native_turn({"revision": 1, "report": base | {"event_revision": 1, "state": "starting"}}), (admitted["id"], "starting"))
        self.assertEqual(self.broker.db.apply_native_turn({"revision": 2, "report": base | {"event_revision": 2, "state": "waiting_input", "reason": "need local label"}}), (admitted["id"], "waiting_human"))
        continued = await self.broker.continue_task({"task_id": admitted["id"], "text": "amber"})
        retained = self.broker.db.task(admitted["id"])
        self.assertEqual(continued["id"], admitted["id"])
        self.assertEqual(retained["state"], "starting")
        self.assertEqual(retained["run_generation"], 1)
        self.assertIsNone(retained["native_turn_id"])
        self.assertFalse(any(call[0] == "pane.send_text" for call in self.broker.herdr.calls))

    async def test_native_xcsh_uncertain_admission_replay_never_launches_twice(self):
        workspace = await self.configure_control_workspace()
        original = self.broker.herdr.request
        uncertain_once = True
        async def uncertain(method, params=None, timeout=65):
            nonlocal uncertain_once
            if method == "execution.resume" and uncertain_once:
                uncertain_once = False
                await original(method, params, timeout)
                raise RuntimeError("lost response after possible native launch")
            return await original(method, params, timeout)
        self.broker.herdr.request = uncertain
        params = {"target": "xcsh-uncertain", "cwd": str(self.root), "priority": "routine", "prompt": "safe",
                  "workspace_id": workspace["workspace"]["workspace_id"], "session_id": "session-uncertain", "text": "safe",
                  "runtime_identity": {"xcsh_artifact": "x", "herdr_artifact": "h", "manager_artifact": "m"}, "idempotency_key": "native-xcsh-uncertain"}
        first = await self.admit_native_xcsh(params)
        replay = await self.admit_native_xcsh(params)
        self.assertEqual(first["state"], "unknown")
        self.assertEqual(replay["id"], first["id"])
        self.assertTrue(replay["idempotency_replayed"])
        self.assertEqual(len(self.broker.herdr.executions), 1)
        resumes = [item for method, item in self.broker.herdr.calls if method == "execution.resume"]
        self.assertGreaterEqual(len(resumes), 2)
        self.assertTrue(all(item["workspace_id"] == workspace["workspace"]["workspace_id"] for item in resumes))

    async def test_native_xcsh_atomic_gen0_claim_requires_key_and_replays_one_child(self):
        """Synthetic component fixture for the task/key/generation transaction."""
        workspace = await self.configure_control_workspace()
        base = {"target": "xcsh-atomic", "cwd": str(self.root), "priority": "routine", "prompt": "safe",
                "text": "safe", "session_id": "session-atomic", "workspace_id": workspace["workspace"]["workspace_id"],
                "runtime_identity": {"xcsh_artifact": "x", "herdr_artifact": "h", "manager_artifact": "m"}}
        with self.assertRaisesRegex(ValueError, "idempotency_key"):
            await self.broker.native_xcsh_admit(base)
        task = await self.admit_native_xcsh(base | {"idempotency_key": "atomic-gen0"})
        binding = self.broker.db.native_generation(task["id"], 0)
        self.assertEqual(binding["state"], "admitted")
        key = self.broker.db.conn.execute("SELECT task_id FROM admission_idempotency WHERE idempotency_key='atomic-gen0'").fetchone()
        self.assertEqual(key["task_id"], task["id"])
        replay = await self.admit_native_xcsh(base | {"idempotency_key": "atomic-gen0"})
        self.assertEqual(replay["id"], task["id"])
        self.assertEqual(len(self.broker.herdr.executions), 1)

    async def test_native_xcsh_requires_measured_absolute_controller_executable(self):
        """Synthetic component fixture for the manager admission boundary."""
        workspace = await self.configure_control_workspace()
        base = {"target": "xcsh-binding", "cwd": str(self.root), "priority": "routine", "prompt": "safe",
                "text": "safe", "session_id": self.native_launch["session_header"]["id"], "workspace_id": workspace["workspace"]["workspace_id"],
                "runtime_identity": {"xcsh_artifact": "x", "herdr_artifact": "h", "manager_artifact": "m", "xcsh_model": "test/model"},
                "native_launch": self.native_launch, "idempotency_key": "binding-required"}
        with self.assertRaisesRegex(ValueError, "xcsh_executable"):
            await self.broker.native_xcsh_admit(base)
        with self.assertRaisesRegex(ValueError, "measurement differs"):
            await self.broker.native_xcsh_admit(base | {
                "xcsh_executable_sha256": "0" * 64,
            })
        with self.assertRaisesRegex(ValueError, "absolute"):
            await self.broker.native_xcsh_admit(base | {
                "native_launch": self.native_launch | {"xcsh_executable": "xcsh"}, "xcsh_executable_sha256": self.xcsh_executable_sha256,
            })

    async def test_native_launch_v3_rejects_changed_or_noncanonical_header_provenance(self):
        """Synthetic component fixture for the v3 session receipt boundary."""
        workspace = await self.configure_control_workspace()
        base = {"target": "xcsh-v3-header", "cwd": str(self.root), "priority": "routine", "prompt": "safe",
                "text": "safe", "session_id": self.native_launch["session_header"]["id"],
                "workspace_id": workspace["workspace"]["workspace_id"],
                "runtime_identity": {"xcsh_artifact": "x", "herdr_artifact": "h", "manager_artifact": "m", "xcsh_model": "test/model"},
                "xcsh_executable_sha256": self.xcsh_executable_sha256, "idempotency_key": "v3-header"}
        with self.assertRaisesRegex(ValueError, "session_header"):
            await self.broker.native_xcsh_admit(base | {"native_launch": self.native_launch | {
                "session_header": self.native_launch["session_header"] | {"sha256": "0" * 64}}})
        with self.assertRaisesRegex(ValueError, "already be canonical"):
            await self.broker.native_xcsh_admit(base | {"idempotency_key": "v3-path", "native_launch": self.native_launch | {
                "session_path": str(self.session_path.parent / "../sessions" / self.session_path.name)}})
        self.assertFalse(any(row["work_kind"] == "xcsh" for row in self.broker.db.list_tasks()))

    async def test_native_launch_v3_sends_only_typed_request_and_exact_backend_argv(self):
        """Synthetic component fixture for protocol-23 request/receipt equivalence."""
        workspace = await self.configure_control_workspace()
        task = await self.admit_native_xcsh({
            "target": "xcsh-v3-argv", "cwd": str(self.root), "priority": "routine", "prompt": "safe",
            "text": "semantic text", "session_id": self.native_launch["session_header"]["id"],
            "workspace_id": workspace["workspace"]["workspace_id"],
            "runtime_identity": {"xcsh_artifact": "x", "herdr_artifact": "h", "manager_artifact": "m", "xcsh_model": "test/model"},
            "idempotency_key": "v3-argv",
        })
        request = next(params for method, params in self.broker.herdr.calls if method == "execution.resume")
        self.assertEqual(set(request), {"execution_id", "generation", "native_launch", "text", "cwd", "workspace_id"})
        self.assertEqual(request["native_launch"], self.native_launch)
        self.assertEqual(request["workspace_id"], workspace["workspace"]["workspace_id"])
        execution = self.broker.herdr.executions[task["backend_execution_id"]]
        self.assertEqual(execution["command"]["argv"], [self.xcsh_executable, "--mode", "json", "--session-dir",
            self.native_launch["session_dir"], "--resume", self.native_launch["session_path"], "--model", "test/model",
            "--tools", "read", "--no-mcp", "--no-lsp", "--no-memories", "--no-skills", "--no-rules", "--no-pty", "--print", "semantic text"])
        interactive_launch = self.native_launch | {"interactive": True}
        interactive = await self.broker.native_xcsh_admit({
            "target": "xcsh-v3-interactive", "cwd": str(self.root), "priority": "routine", "prompt": "safe",
            "text": "interactive semantic text", "session_id": interactive_launch["session_header"]["id"],
            "workspace_id": workspace["workspace"]["workspace_id"], "native_launch": interactive_launch,
            "xcsh_executable_sha256": self.xcsh_executable_sha256,
            "runtime_identity": {"xcsh_artifact": "x", "herdr_artifact": "h", "manager_artifact": "m", "xcsh_model": "test/model"},
            "idempotency_key": "v3-interactive-argv",
        })
        interactive_argv = self.broker.herdr.executions[interactive["backend_execution_id"]]["command"]["argv"]
        self.assertEqual(interactive_argv, [self.xcsh_executable, "--session-dir", self.native_launch["session_dir"],
            "--resume", self.native_launch["session_path"], "--model", "test/model", "--tools", "read",
            "--no-mcp", "--no-lsp", "--no-memories", "--no-skills", "--no-rules", "--no-pty",
            "interactive semantic text"])
        self.assertNotIn("--mode", interactive_argv)
        self.assertNotIn("--print", interactive_argv)

    async def test_native_xcsh_resume_targets_durable_workspace_not_active_fallback(self):
        """Synthetic component fixture for immutable workspace routing."""
        admitted_workspace = await self.configure_control_workspace()
        foreign_workspace = await self.broker.herdr.request("workspace.create", {
            "cwd": str(self.root), "label": "active-but-foreign",
        })
        admitted_id = admitted_workspace["workspace"]["workspace_id"]
        foreign_id = foreign_workspace["workspace"]["workspace_id"]
        foreign_tabs_before = {tab_id for tab_id, tab in self.broker.herdr.tabs.items()
                               if tab["workspace_id"] == foreign_id}
        task = await self.admit_native_xcsh({
            "target": "xcsh-workspace-routing", "cwd": str(self.root), "priority": "routine", "prompt": "safe",
            "text": "semantic text", "session_id": self.native_launch["session_header"]["id"],
            "workspace_id": admitted_id,
            "runtime_identity": {"xcsh_artifact": "x", "herdr_artifact": "h", "manager_artifact": "m", "xcsh_model": "test/model"},
            "idempotency_key": "workspace-routing",
        })
        requests = [params for method, params in self.broker.herdr.calls if method == "execution.resume"]
        self.assertEqual([request["workspace_id"] for request in requests], [admitted_id])
        self.assertEqual(task["workspace_id"], admitted_id)
        self.assertEqual(self.broker.herdr.executions[task["backend_execution_id"]]["workspace_id"], admitted_id)
        self.assertEqual({tab_id for tab_id, tab in self.broker.herdr.tabs.items()
                          if tab["workspace_id"] == foreign_id}, foreign_tabs_before)

    async def test_native_launch_model_matches_backend_utf8_and_control_boundary_preclaim(self):
        """Synthetic component fixture for the protocol-23 model contract."""
        workspace = await self.configure_control_workspace()
        base = {"target": "xcsh-model", "cwd": str(self.root), "priority": "routine", "prompt": "safe",
                "text": "safe", "session_id": self.native_launch["session_header"]["id"],
                "workspace_id": workspace["workspace"]["workspace_id"],
                "xcsh_executable_sha256": self.xcsh_executable_sha256}

        async def admit(model: str, key: str, *, runtime_model: str | None = None):
            return await self.broker.native_xcsh_admit(base | {
                "native_launch": self.native_launch | {"model": model},
                "runtime_identity": {"xcsh_artifact": "x", "herdr_artifact": "h", "manager_artifact": "m",
                                     "xcsh_model": runtime_model if runtime_model is not None else model},
                "idempotency_key": key,
            })

        accepted = await admit(" model ", "model-whitespace")
        self.assertEqual(accepted["state"], "starting")
        self.assertEqual(accepted["native_launch"]["model"], " model ")
        accepted_boundary = await admit("m" * 256, "model-256")
        self.assertEqual(accepted_boundary["native_launch"]["model"], "m" * 256)
        for model, key, field in (("m" * 257, "model-257", "native_launch.model"),
                                  ("é" * 129, "model-utf8", "native_launch.model"),
                                  ("safe\u0085selector", "model-control", "native_launch.model")):
            with self.assertRaisesRegex(ValueError, field):
                await admit(model, key, runtime_model="test/model")
        # Python ``strip`` would silently remove these C0/C1 controls.  The
        # manager must reject the raw launch and runtime identity exactly as
        # Rust's ``char::is_control`` check does, before claiming a task.
        for control, name in (("\u001c", "c0"), ("\u0085", "c1")):
            for model, suffix in ((f"{control}selector", "leading"),
                                  (f"selector{control}", "trailing")):
                with self.assertRaisesRegex(ValueError, "native_launch.model"):
                    await admit(model, f"model-{name}-{suffix}", runtime_model="test/model")
                with self.assertRaisesRegex(ValueError, "runtime_identity.xcsh_model"):
                    await admit("test/model", f"runtime-{name}-{suffix}", runtime_model=model)
        self.assertEqual(len([row for row in self.broker.db.list_tasks() if row["work_kind"] == "xcsh"]), 2)

    async def test_native_xcsh_rejects_protocol20_before_durable_generation_claim(self):
        workspace = await self.configure_control_workspace()
        original = self.broker.herdr.request

        async def protocol20(method, params=None, timeout=65):
            if method == "ping":
                return {"protocol": 20, "capabilities": {"tracked_executions": True, "agent_turn_journal": True}}
            return await original(method, params, timeout)

        self.broker.herdr.request = protocol20
        try:
            with self.assertRaisesRegex(ValueError, "protocol-23 workspace-bound"):
                await self.admit_native_xcsh({
                    "target": "xcsh-protocol", "cwd": str(self.root), "priority": "routine", "prompt": "safe",
                    "text": "safe", "session_id": "session-protocol", "workspace_id": workspace["workspace"]["workspace_id"],
                    "runtime_identity": {"xcsh_artifact": "x", "herdr_artifact": "h", "manager_artifact": "m"},
                    "idempotency_key": "protocol-21-required",
                })
        finally:
            self.broker.herdr.request = original
        self.assertFalse(any(row["work_kind"] == "xcsh" for row in self.broker.db.list_tasks()))

    async def test_native_xcsh_rejects_wrong_returned_executable_binding(self):
        """A protocol-23 receipt may not substitute an executable after claim."""
        workspace = await self.configure_control_workspace()
        original = self.broker.herdr.request

        async def wrong_binding(method, params=None, timeout=65):
            result = await original(method, params, timeout)
            if method == "execution.resume":
                result["execution"]["native_executable"] = {
                    "canonical_path": "/bin/false", "sha256": "0" * 64,
                }
            return result

        self.broker.herdr.request = wrong_binding
        try:
            result = await self.admit_native_xcsh({
                "target": "xcsh-wrong-binding", "cwd": str(self.root), "priority": "routine", "prompt": "safe",
                "text": "safe", "session_id": "session-wrong-binding", "workspace_id": workspace["workspace"]["workspace_id"],
                "runtime_identity": {"xcsh_artifact": "x", "herdr_artifact": "h", "manager_artifact": "m"},
                "idempotency_key": "wrong-binding",
            })
        finally:
            self.broker.herdr.request = original
        self.assertTrue(result["admission_uncertain"])
        binding = self.broker.db.native_generation(result["id"], 0)
        self.assertIsNone(binding["backend_execution_id"])

    async def test_native_xcsh_rejects_wrong_returned_launch_receipt(self):
        """Synthetic component fixture: a receipt cannot substitute v3 provenance."""
        workspace = await self.configure_control_workspace()
        original = self.broker.herdr.request

        async def wrong_launch(method, params=None, timeout=65):
            result = await original(method, params, timeout)
            if method == "execution.resume":
                result["execution"]["native_launch"] = params["native_launch"] | {"model": "other/model"}
            return result

        self.broker.herdr.request = wrong_launch
        try:
            result = await self.admit_native_xcsh({
                "target": "xcsh-wrong-launch", "cwd": str(self.root), "priority": "routine", "prompt": "safe",
                "text": "safe", "session_id": "session-wrong-launch", "workspace_id": workspace["workspace"]["workspace_id"],
                "runtime_identity": {"xcsh_artifact": "x", "herdr_artifact": "h", "manager_artifact": "m"},
                "idempotency_key": "wrong-launch",
            })
        finally:
            self.broker.herdr.request = original
        self.assertTrue(result["admission_uncertain"])
        binding = self.broker.db.native_generation(result["id"], 0)
        self.assertIsNone(binding["backend_execution_id"])

    async def test_native_xcsh_continuation_remeasures_persisted_executable(self):
        """A replaced executable fails before a continuation can claim a child."""
        workspace = await self.configure_control_workspace()
        executable = self.root / "xcsh"
        executable.write_bytes(Path(self.xcsh_executable).read_bytes())
        executable.chmod(0o755)
        digest = hashlib.sha256(executable.read_bytes()).hexdigest()
        task = await self.broker.native_xcsh_admit({
            "target": "xcsh-remeasure", "cwd": str(self.root), "priority": "routine", "prompt": "safe",
            "text": "safe", "session_id": self.native_launch["session_header"]["id"], "workspace_id": workspace["workspace"]["workspace_id"],
            "xcsh_executable": str(executable), "xcsh_executable_sha256": digest,
            "native_launch": self.native_launch | {"xcsh_executable": str(executable)},
            "runtime_identity": {"xcsh_artifact": "x", "herdr_artifact": "h", "manager_artifact": "m", "xcsh_model": "test/model"},
            "idempotency_key": "remeasure",
        })
        first = self.broker.db.current_native_generation(task["id"])
        report = {"execution_id": task["id"], "pane_id": first["pane_id"], "producer": "xcsh",
                  "session_id": first["session_id"], "turn_id": "turn-remeasure", "generation": 0}
        self.broker.db.apply_native_turn({"revision": 1, "report": report | {"event_revision": 1, "state": "waiting_input", "reason": "need input"}})
        executable.write_bytes(b"replacement")
        executable.chmod(0o755)
        with self.assertRaisesRegex(ValueError, "measurement differs"):
            await self.broker.continue_task({"task_id": task["id"], "text": "continue"})
        self.assertEqual(self.broker.db.current_native_generation(task["id"])["generation"], 0)

    async def test_native_xcsh_concurrent_same_key_reuses_atomic_gen0_claim(self):
        """Synthetic component fixture for the post-preflight admission race."""
        workspace = await self.configure_control_workspace()
        params = {
            "target": "xcsh-atomic-race", "cwd": str(self.root), "priority": "routine", "prompt": "safe",
            "text": "safe", "session_id": "session-atomic-race", "workspace_id": workspace["workspace"]["workspace_id"],
            "runtime_identity": {"xcsh_artifact": "x", "herdr_artifact": "h", "manager_artifact": "m"},
            "idempotency_key": "atomic-race",
        }
        first, second = await asyncio.gather(
            self.admit_native_xcsh(params), self.admit_native_xcsh(dict(params)),
        )
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(len(self.broker.herdr.executions), 1)
        bindings = self.broker.db.conn.execute(
            "SELECT generation,request_json FROM native_execution_generations WHERE task_id=?", (first["id"],)
        ).fetchall()
        self.assertEqual(len(bindings), 1)
        self.assertEqual(bindings[0]["generation"], 0)

    async def test_xcsh_generation_history_rejects_foreign_panes_and_settles_old_child(self):
        """Synthetic component fixture for ledger rules; not UAT evidence."""
        workspace = await self.configure_control_workspace()
        self.broker.config_path.write_text(json.dumps({"agent_turn_consumer_enabled": True, "agent_turn_producer": "xcsh"}))
        task = await self.admit_native_xcsh({
            "target": "xcsh-generations", "cwd": str(self.root), "priority": "routine", "prompt": "safe",
            "text": "safe", "session_id": "session-history", "workspace_id": workspace["workspace"]["workspace_id"],
            "runtime_identity": {"xcsh_artifact": "x", "herdr_artifact": "h", "manager_artifact": "m"}, "idempotency_key": "history",
        })
        first = self.broker.db.current_native_generation(task["id"])
        self.assertIsNotNone(first)
        base = {"execution_id": task["id"], "pane_id": first["pane_id"], "producer": "xcsh", "session_id": first["session_id"], "turn_id": "turn-0", "generation": 0}
        self.broker.db.apply_native_turn({"revision": 1, "report": base | {"event_revision": 1, "state": "starting"}})
        self.broker.db.apply_native_turn({"revision": 2, "report": base | {"event_revision": 2, "state": "waiting_input", "reason": "need input"}})
        await self.broker.continue_task({"task_id": task["id"], "text": "next"})
        current = self.broker.db.current_native_generation(task["id"])
        self.assertEqual(current["generation"], 1)
        replay = await self.broker.continue_task({"task_id": task["id"], "text": "next"})
        self.assertTrue(replay["generation_replayed"])
        with self.assertRaisesRegex(ValueError, "conflicts"):
            await self.broker.continue_task({"task_id": task["id"], "text": "different"})
        # The old child can report its interrupted settlement, but cannot
        # overwrite the current generation's pane/session/task state.
        self.assertIsNone(self.broker.db.apply_native_turn({"revision": 3, "report": base | {"event_revision": 3, "state": "interrupted", "reason": "superseded"}}))
        self.assertEqual(self.broker.db.task(task["id"])["run_generation"], 1)
        self.assertEqual(self.broker.db.native_generation(task["id"], 0)["terminal_state"], "interrupted")
        foreign = {"execution_id": task["id"], "pane_id": "foreign:pane", "producer": "xcsh", "session_id": current["session_id"], "turn_id": "turn-1", "generation": 1}
        with self.assertRaisesRegex(ValueError, "provenance"):
            self.broker.db.apply_native_turn({"revision": 4, "report": foreign | {"event_revision": 1, "state": "starting"}})
        current_report = {"execution_id": task["id"], "pane_id": current["pane_id"], "producer": "xcsh", "session_id": current["session_id"], "turn_id": "turn-1", "generation": 1}
        self.broker.db.apply_native_turn({"revision": 4, "report": current_report | {"event_revision": 1, "state": "starting"}})
        await self.broker.request_stop({"task_id": task["id"]})
        cancelled = [call for call in self.broker.herdr.calls if call[0] == "execution.cancel"]
        self.assertEqual(cancelled[-1][1]["execution_id"], current["backend_execution_id"])
        self.broker.herdr.agent_turns = [
            {"revision": 5, "report": current_report | {"event_revision": 2, "state": "cancelled", "reason": "native cancel"}},
        ]
        await self.broker.consume_native_turns()
        await asyncio.sleep(0.08)
        self.assertNotIn(current["tab_id"], self.broker.herdr.tabs)

    async def test_xcsh_concurrent_continuations_share_one_source_turn_child(self):
        """Synthetic component fixture for the source-turn CAS fence."""
        workspace = await self.configure_control_workspace()
        task = await self.admit_native_xcsh({
            "target": "xcsh-continuation-race", "cwd": str(self.root), "priority": "routine", "prompt": "safe",
            "text": "safe", "session_id": "session-continuation-race", "workspace_id": workspace["workspace"]["workspace_id"],
            "runtime_identity": {"xcsh_artifact": "x", "herdr_artifact": "h", "manager_artifact": "m"}, "idempotency_key": "continuation-race",
        })
        binding = self.broker.db.current_native_generation(task["id"])
        report = {"execution_id": task["id"], "pane_id": binding["pane_id"], "producer": "xcsh",
                  "session_id": binding["session_id"], "turn_id": "turn-race", "generation": 0}
        self.broker.db.apply_native_turn({"revision": 1, "report": report | {"event_revision": 1, "state": "starting"}})
        self.broker.db.apply_native_turn({"revision": 2, "report": report | {"event_revision": 2, "state": "waiting_input", "reason": "need input"}})
        first, second = await asyncio.gather(
            self.broker.continue_task({"task_id": task["id"], "text": "continue exactly once"}),
            self.broker.continue_task({"task_id": task["id"], "text": "continue exactly once"}),
        )
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(self.broker.db.current_native_generation(task["id"])["generation"], 1)
        children = self.broker.db.conn.execute(
            "SELECT * FROM native_execution_generations WHERE task_id=? AND generation=1", (task["id"],)
        ).fetchall()
        self.assertEqual(len(children), 1)
        self.assertEqual(len([item for item in self.broker.herdr.executions.values() if item["generation"] == 1]), 1)

    async def test_xcsh_lost_continuation_response_reconciles_same_generation(self):
        """Synthetic component fixture for an effect that succeeded before its response was lost."""
        workspace = await self.configure_control_workspace()
        task = await self.admit_native_xcsh({
            "target": "xcsh-continuation-lost", "cwd": str(self.root), "priority": "routine", "prompt": "safe",
            "text": "safe", "session_id": "session-continuation-lost", "workspace_id": workspace["workspace"]["workspace_id"],
            "runtime_identity": {"xcsh_artifact": "x", "herdr_artifact": "h", "manager_artifact": "m"}, "idempotency_key": "continuation-lost",
        })
        binding = self.broker.db.current_native_generation(task["id"])
        report = {"execution_id": task["id"], "pane_id": binding["pane_id"], "producer": "xcsh",
                  "session_id": binding["session_id"], "turn_id": "turn-lost", "generation": 0}
        self.broker.db.apply_native_turn({"revision": 1, "report": report | {"event_revision": 1, "state": "starting"}})
        self.broker.db.apply_native_turn({"revision": 2, "report": report | {"event_revision": 2, "state": "waiting_input", "reason": "need input"}})
        original_request, lose_reply = self.broker.herdr.request, True

        async def uncertain_request(method, params=None, timeout=65):
            nonlocal lose_reply
            result = await original_request(method, params, timeout)
            if method == "execution.resume" and params["generation"] == 1 and lose_reply:
                lose_reply = False
                raise RuntimeError("continuation response lost after backend effect")
            return result

        self.broker.herdr.request = uncertain_request
        first = await self.broker.continue_task({"task_id": task["id"], "text": "continue after loss"})
        self.assertTrue(first["admission_uncertain"])
        self.broker.herdr.request = original_request
        replay = await self.broker.continue_task({"task_id": task["id"], "text": "continue after loss"})
        self.assertTrue(replay["generation_replayed"])
        self.assertEqual(self.broker.db.current_native_generation(task["id"])["generation"], 1)
        self.assertEqual(len([item for item in self.broker.herdr.executions.values() if item["generation"] == 1]), 1)
        resumes = [params for method, params in self.broker.herdr.calls
                   if method == "execution.resume" and params["generation"] == 1]
        self.assertGreaterEqual(len(resumes), 2)
        self.assertTrue(all(item["native_launch"]["xcsh_executable"] == self.xcsh_executable for item in resumes))
        self.assertTrue(all(item["workspace_id"] == workspace["workspace"]["workspace_id"] for item in resumes))
        self.assertTrue(all("xcsh_executable_sha256" not in item for item in resumes))

    async def test_xcsh_cancel_claim_recovers_after_ambiguous_backend_response(self):
        """Synthetic component fixture: a persisted cancel claim is replayed exactly."""
        workspace = await self.configure_control_workspace()
        task = await self.admit_native_xcsh({
            "target": "xcsh-cancel-recovery", "cwd": str(self.root), "priority": "routine", "prompt": "safe",
            "text": "safe", "session_id": "session-cancel", "workspace_id": workspace["workspace"]["workspace_id"],
            "runtime_identity": {"xcsh_artifact": "x", "herdr_artifact": "h", "manager_artifact": "m"},
            "idempotency_key": "cancel-recovery",
        })
        binding = self.broker.db.current_native_generation(task["id"])
        original_request = self.broker.herdr.request
        lost_response = True

        async def uncertain_request(method, params=None, timeout=65):
            nonlocal lost_response
            result = await original_request(method, params, timeout)
            if method == "execution.cancel" and lost_response:
                lost_response = False
                raise RuntimeError("response lost after backend cancellation")
            return result

        self.broker.herdr.request = uncertain_request
        with self.assertRaisesRegex(RuntimeError, "response lost"):
            await self.broker.request_stop({"task_id": task["id"]})
        self.broker.herdr.request = original_request
        action = self.broker.db.conn.execute(
            "SELECT * FROM native_execution_actions WHERE task_id=? AND generation=0 AND kind='cancel'",
            (task["id"],),
        ).fetchone()
        self.assertEqual(action["state"], "claimed")
        self.assertEqual(action["backend_execution_id"], binding["backend_execution_id"])
        await self.broker._reconcile_native_xcsh_admissions(task["id"])
        recovered = self.broker.db.conn.execute(
            "SELECT * FROM native_execution_actions WHERE action_key=?", (action["action_key"],)
        ).fetchone()
        self.assertEqual(recovered["state"], "completed")
        cancels = [call for call in self.broker.herdr.calls if call[0] == "execution.cancel"]
        self.assertEqual({call[1]["execution_id"] for call in cancels}, {binding["backend_execution_id"]})

    async def test_xcsh_cancelled_turn_waits_for_settled_backend_execution(self):
        """Synthetic component fixture for PR49's cross-ledger cancel settlement."""
        workspace = await self.configure_control_workspace()
        self.broker.config_path.write_text(json.dumps({"agent_turn_consumer_enabled": True, "agent_turn_producer": "xcsh"}))
        task = await self.admit_native_xcsh({
            "target": "xcsh-cancel-settlement", "cwd": str(self.root), "priority": "routine", "prompt": "safe",
            "text": "safe", "session_id": "session-cancel-settlement", "workspace_id": workspace["workspace"]["workspace_id"],
            "runtime_identity": {"xcsh_artifact": "x", "herdr_artifact": "h", "manager_artifact": "m"},
            "idempotency_key": "cancel-settlement",
        })
        binding = self.broker.db.current_native_generation(task["id"])
        base = {"execution_id": task["id"], "pane_id": binding["pane_id"], "producer": "xcsh",
                "session_id": binding["session_id"], "turn_id": "turn-cancel-settlement", "generation": 0}
        await self.broker.request_stop({"task_id": task["id"]})
        self.broker.herdr.agent_turns = [
            {"revision": 1, "report": base | {"event_revision": 1, "state": "starting"}},
            {"revision": 2, "report": base | {"event_revision": 2, "state": "cancelled", "reason": "native cancel"}},
        ]
        original_request = self.broker.herdr.request

        async def unsettled_get(method, params=None, timeout=65):
            result = await original_request(method, params, timeout)
            if method == "execution.get":
                execution = dict(result["execution"])
                execution["state"] = "running"
                return {"execution": execution}
            return result

        self.broker.herdr.request = unsettled_get
        with self.assertRaisesRegex(RuntimeError, "not yet backed by a settled"):
            await self.broker.consume_native_turns()
        self.broker.herdr.request = original_request
        applied = await self.broker.consume_native_turns()
        self.assertIn({"task_id": task["id"], "state": "cancelled"}, applied["applied"])
        self.assertEqual(self.broker.db.task(task["id"])["state"], "cancelled")

    async def test_xcsh_cancel_receipt_requires_exact_cooperative_generation_binding(self):
        """Synthetic component fixture for protocol-23 cancel receipt validation."""
        workspace = await self.configure_control_workspace()
        task = await self.admit_native_xcsh({
            "target": "xcsh-cancel-binding", "cwd": str(self.root), "priority": "routine", "prompt": "safe",
            "text": "safe", "session_id": "session-cancel-binding", "workspace_id": workspace["workspace"]["workspace_id"],
            "runtime_identity": {"xcsh_artifact": "x", "herdr_artifact": "h", "manager_artifact": "m"},
            "idempotency_key": "cancel-binding",
        })
        binding = self.broker.db.current_native_generation(task["id"])
        original_request = self.broker.herdr.request

        async def foreign_receipt(method, params=None, timeout=65):
            result = await original_request(method, params, timeout)
            if method != "execution.cancel":
                return result
            execution = dict(result["execution"])
            execution["native_executable"] = {"canonical_path": "/foreign/xcsh", "sha256": "0" * 64}
            return {**result, "execution": execution}

        self.broker.herdr.request = foreign_receipt
        with self.assertRaisesRegex(RuntimeError, "immutable native generation"):
            await self.broker.request_stop({"task_id": task["id"]})
        action = self.broker.db.conn.execute(
            "SELECT * FROM native_execution_actions WHERE task_id=? AND generation=0 AND kind='cancel'",
            (task["id"],),
        ).fetchone()
        self.assertEqual(action["state"], "claimed")
        # The backend's real record is still cooperative and nonterminal; a
        # recovery can accept it only after rechecking every durable field.
        self.assertEqual(self.broker.herdr.executions[binding["backend_execution_id"]]["state"], "running")
        self.assertTrue(self.broker.herdr.executions[binding["backend_execution_id"]]["cancel_requested"])
        self.broker.herdr.request = original_request
        await self.broker._reconcile_native_xcsh_admissions(task["id"])
        recovered = self.broker.db.conn.execute(
            "SELECT * FROM native_execution_actions WHERE action_key=?", (action["action_key"],)
        ).fetchone()
        self.assertEqual(recovered["state"], "completed")
        self.assertTrue(json.loads(recovered["receipt_json"])["cancel_requested"])

    async def test_native_xcsh_offline_chain_uses_real_db_outbox_inbox_and_ack(self):
        workspace = await self.configure_control_workspace()
        self.broker.config_path.write_text(json.dumps({"agent_turn_consumer_enabled": True, "agent_turn_producer": "xcsh"}))
        task = await self.admit_native_xcsh({
            "target": "xcsh-e2e", "cwd": str(self.root), "priority": "routine", "prompt": "safe",
            "workspace_id": workspace["workspace"]["workspace_id"], "session_id": "s1", "text": "safe",
            "runtime_identity": {"xcsh_artifact": "x", "herdr_artifact": "h", "manager_artifact": "m"}, "idempotency_key": "xcsh-e2e",
        })
        row = self.broker.db.task(task["id"])
        result = "offline semantic completion"
        base = {"execution_id": task["id"], "pane_id": row["pane_id"], "producer": "xcsh", "session_id": self.native_launch["session_header"]["id"], "turn_id": "t1", "generation": 0}
        self.broker.herdr.agent_turns = [
            {"revision": 1, "report": base | {"event_revision": 1, "state": "starting"}},
            {"revision": 2, "report": base | {"event_revision": 2, "state": "completed", "result": result, "result_digest": hashlib.sha256(result.encode()).hexdigest()}},
        ]
        applied = await self.broker.consume_native_turns()
        self.assertEqual([item["state"] for item in applied["applied"]], ["starting", "completed"])
        # Production consume path, not a private test helper, schedules owned
        # cleanup only after the verified semantic terminal record.
        await asyncio.sleep(0.08)
        self.assertNotIn(row["tab_id"], self.broker.herdr.tabs)
        inbox = self.broker.db.deliver_inbox()
        event = next(item for item in inbox if item["task_id"] == task["id"])
        self.assertEqual(event["delivery_state"], "delivered")
        self.broker.db.ack_completion(event["event_id"], "consumed", "offline_manager", "offline-e2e", "turn-1")
        state = self.broker.db.conn.execute("SELECT state FROM completion_outbox WHERE event_id=?", (event["event_id"],)).fetchone()["state"]
        self.assertEqual(state, "consumed")

    async def test_duplicate_command_launch_is_refused_before_a_second_wrapper(self):
        command = await self.broker.run_command(
            {"label": "exactly once", "cwd": str(self.root), "shell": "zsh", "command": "xcsh"}
        )
        row = self.broker.db.task(command["id"])
        sent_before = len([call for call in self.broker.herdr.calls if call[0] == "pane.send_text"])
        with self.assertRaisesRegex(RuntimeError, "command launch refused from state working"):
            await self.broker._start_command(row, "exactly once", "zsh", "xcsh")
        sent_after = len([call for call in self.broker.herdr.calls if call[0] == "pane.send_text"])
        self.assertEqual(sent_after, sent_before)

    async def test_recovery_fails_unknown_persisted_work_kind_closed(self):
        self.broker.db.conn.execute(
            "INSERT INTO tasks(id,target,cwd,state,priority,summary,created_at,updated_at,work_kind,session_state) "
            "VALUES(?,?,?,?,?,?,?,?,?,?)",
            ("future-kind", "control", str(self.root), "starting", "normal", "bad", 1, 1, "future", "live"),
        )
        self.broker.db.conn.commit()
        await self.broker._fail_unsupported_work_kinds("test recovery")
        retained = self.broker.db.task("future-kind")
        self.assertEqual(retained["state"], "failed")
        self.assertIn("Unsupported persisted work kind", retained["summary"])

    async def test_concurrent_same_target_uses_one_workspace_distinct_tabs(self):
        first = await self.dispatch("shared")
        second = await self.dispatch("shared")
        first_row = self.broker.db.update(first["id"], state="starting")
        second_row = self.broker.db.update(second["id"], state="starting")
        await asyncio.gather(
            self.broker._start_task(first_row),
            self.broker._start_task(second_row),
        )
        first_live = self.broker.db.task(first["id"])
        second_live = self.broker.db.task(second["id"])
        self.assertEqual(first_live["workspace_id"], second_live["workspace_id"])
        self.assertNotEqual(first_live["tab_id"], second_live["tab_id"])
        self.assertEqual(first_live["state"], "working")
        self.assertEqual(second_live["state"], "working")

    async def test_control_root_workers_and_commands_share_control_while_external_stays_separate(self):
        control = await self.configure_control_workspace()
        worker = await self.dispatch("home-robin-codex-control")
        await self.broker._start_task(self.broker.db.update(worker["id"], state="starting"))
        worker_row = self.broker.db.task(worker["id"])
        command = await self.broker.run_command(
            {"label": "control check", "cwd": str(self.root), "shell": "zsh", "command": "true", "priority": "normal"}
        )
        with tempfile.TemporaryDirectory() as external_raw:
            external = Path(external_raw)
            project = await self.broker.dispatch(
                {
                    "target": "xcsh-codex-login",
                    "cwd": str(external),
                    "priority": "normal",
                    "prompt": "external project",
                    "parent_id": None,
                    "model": "gpt-5.6-sol",
                }
            )
            await self.broker._start_task(self.broker.db.update(project["id"], state="starting"))
            project_row = self.broker.db.task(project["id"])

            self.assertEqual(worker_row["workspace_id"], control["workspace"]["workspace_id"])
            self.assertEqual(command["workspace_id"], control["workspace"]["workspace_id"])
            self.assertEqual(worker_row["cwd"], str(self.root))
            self.assertEqual(command["cwd"], str(self.root))
            self.assertNotEqual(project_row["workspace_id"], control["workspace"]["workspace_id"])
            self.assertEqual(project_row["cwd"], str(external))

    async def test_control_worker_cleanup_closes_only_its_tab_not_manager_workspace(self):
        control = await self.configure_control_workspace()
        worker = await self.dispatch("home-robin-codex-control")
        await self.broker._start_task(self.broker.db.update(worker["id"], state="starting"))
        row = self.broker.db.task(worker["id"])
        self.broker.db.update(row["id"], state="completed", cleanup_deadline=0)
        await self.broker._cleanup_after(row["id"], 0)

        self.assertIn(control["workspace"]["workspace_id"], self.broker.herdr.workspaces)
        self.assertIn(control["tab"]["tab_id"], self.broker.herdr.tabs)
        self.assertNotIn(row["tab_id"], self.broker.herdr.tabs)

    async def test_command_native_evidence_and_cleanup(self):
        task = await self.broker.run_command(
            {"label": "fixture check", "cwd": str(self.root), "shell": "zsh", "command": "printf ok", "priority": "normal"}
        )
        self.assertEqual(task["work_kind"], "command")
        execution = self.broker.herdr.executions[task["id"]]
        execution.update(state="exited", exit_code=0, stdout_tail="ok", output_complete=True)
        await self.broker._reconcile_native_commands()
        await asyncio.sleep(0.08)
        row = self.broker.db.task(task["id"])
        self.assertEqual(row["command_exit_status"], 0)
        self.assertEqual(row["session_state"], "closed")
        self.assertNotIn(task["tab_id"], self.broker.herdr.tabs)
        events = [entry["event"] for entry in self.broker.db.transitions(task["id"])]
        self.assertIn("command_native_exited", events)
        self.assertIn("cleanup_scheduled", events)
        self.assertIn("owned_tab_cleaned", events)

    async def test_native_command_cancel_and_reconnect_replay_are_authoritative(self):
        task = await self.broker.run_command(
            {"label": "native sleep", "cwd": str(self.root), "shell": "bash", "command": "sleep 30"}
        )
        started = [call for call in self.broker.herdr.calls if call[0] == "execution.start"]
        self.assertEqual(len(started), 1)
        duplicate = await self.broker.herdr.request("execution.start", started[0][1])
        self.assertFalse(duplicate["admitted"])
        self.assertEqual(len(self.broker.herdr.executions), 1)
        stopped = await self.broker.request_stop({"task_id": task["id"]})
        self.assertIsNotNone(stopped["stop_requested_at"])
        await self.broker._reconcile_native_commands()
        retained = self.broker.db.task(task["id"])
        self.assertEqual(retained["state"], "cancelled")
        self.assertIn("Interrupt", retained["summary"])

    async def test_subscription_ignores_missing_panes_without_dropping_live_ones(self):
        stale = self.broker.db.add_task(
            {
                "id": "stale-pane",
                "target": "stale-pane",
                "cwd": str(self.root),
                "prompt": "x",
                "summary": "working",
                "parent_id": None,
                "priority": "normal",
            }
        )
        live = self.broker.db.add_task(
            {
                "id": "live-pane",
                "target": "live-pane",
                "cwd": str(self.root),
                "prompt": "x",
                "summary": "working",
                "parent_id": None,
                "priority": "normal",
            }
        )
        self.broker.db.update(stale["id"], state="working", pane_id="w9:p9", herdr_state="working")
        self.broker.db.update(live["id"], state="working", pane_id="w1:p1", herdr_state="working")
        self.broker.herdr.panes["w1:p1"] = {
            "pane_id": "w1:p1", "workspace_id": "w1", "tab_id": "w1:t1", "agent_status": "working"
        }

        self.assertEqual(await self.broker._live_subscription_pane_ids(), ["w1:p1"])

    async def test_command_exit_spool_survives_broker_unavailability(self):
        task = await self.broker.run_command(
            {
                "label": "spooled exit",
                "cwd": str(self.root),
                "shell": "bash",
                "command": "exit 7",
                "priority": "normal",
            }
        )
        self.broker.command_event_dir.mkdir(mode=0o700)
        event_path = self.broker.command_event_dir / f"{task['id']}.json"
        event_path.write_text(
            json.dumps(
                {
                    "task_id": task["id"],
                    "phase": "exited",
                    "exit_status": 7,
                    "created_at": 1,
                }
            )
        )
        event_path.chmod(0o600)

        await self.broker._drain_command_events()
        await asyncio.sleep(0.08)

        row = self.broker.db.task(task["id"])
        self.assertEqual(row["state"], "failed")
        self.assertEqual(row["command_exit_status"], 7)
        self.assertFalse(event_path.exists())
        self.assertEqual(row["session_state"], "closed")

    async def test_missing_command_exit_report_fails_closed(self):
        task = await self.broker.run_command(
            {
                "label": "lost report",
                "cwd": str(self.root),
                "shell": "zsh",
                "command": "true",
                "priority": "normal",
            }
        )
        self.broker.db.conn.execute(
            "UPDATE tasks SET updated_at=updated_at-30 WHERE id=?", (task["id"],)
        )
        self.broker.db.conn.commit()

        await self.broker._recover_unreported_command_exits()
        await asyncio.sleep(0.08)

        row = self.broker.db.task(task["id"])
        self.assertEqual(row["state"], "failed")
        self.assertIsNone(row["command_exit_status"])
        self.assertIn("unverified", row["summary"])
        self.assertEqual(row["session_state"], "closed")

    async def test_command_idle_tui_never_requires_control_report_or_adopts_session(self):
        task = await self.broker.run_command(
            {"label": "interactive shell", "cwd": str(self.root), "shell": "zsh", "command": "xcsh"}
        )
        pane = self.broker.herdr.panes[task["pane_id"]]
        pane["agent_status"] = "idle"
        await self.broker._handle_event(
            "pane.updated",
            {"pane_id": task["pane_id"], "agent_status": "idle", "agent_session": {"value": "/tmp/xcsh.jsonl"}},
        )
        retained = self.broker.db.task(task["id"])
        self.assertEqual(retained["state"], "working")
        self.assertEqual(retained["herdr_state"], "idle")
        self.assertIsNone(retained["agent_session_id"])
        self.assertIsNone(retained["terminal_reported_at"])
        with self.assertRaisesRegex(ValueError, "only valid for Codex"):
            await self.broker.report({"task_id": task["id"], "state": "completed", "summary": "not allowed"})

    async def test_legacy_command_idle_unknown_recovers_to_wrapper_wait(self):
        task = await self.broker.run_command(
            {"label": "legacy command", "cwd": str(self.root), "shell": "bash", "command": "xcsh"}
        )
        await self.broker.command_event({"task_id": task["id"], "phase": "started"})
        self.broker.db.update(task["id"], state="unknown", herdr_state="idle")
        await self.broker._apply_lifecycle(task["id"], "idle")
        recovered = self.broker.db.task(task["id"])
        self.assertEqual(recovered["state"], "working")
        self.assertIn("awaiting its structural exit report", recovered["summary"])

    async def test_unreported_unknown_command_is_reconcilable_but_native_unknown_is_not(self):
        command = await self.broker.run_command(
            {"label": "recover command", "cwd": str(self.root), "shell": "bash", "command": "xcsh"}
        )
        native = await self.dispatch("native-unknown")
        self.broker.db.update(command["id"], state="unknown")
        self.broker.db.update(native["id"], state="unknown")
        ids = {row["id"] for row in self.broker.db.reconcilable_tasks()}
        self.assertIn(command["id"], ids)
        self.assertNotIn(native["id"], ids)

    async def test_reconcile_clears_legacy_embedded_tui_session_from_command(self):
        task = await self.broker.run_command(
            {"label": "legacy embedded tui", "cwd": str(self.root), "shell": "bash", "command": "xcsh"}
        )
        self.broker.db.update(task["id"], agent_session_id="/tmp/xcsh-session.jsonl")
        await self.broker.reconcile()
        retained = self.broker.db.task(task["id"])
        self.assertIsNone(retained["agent_session_id"])
        self.assertIn(
            "command_non_native_session_cleared",
            [event["event"] for event in self.broker.db.transitions(task["id"])],
        )

    async def test_immediate_command_stop_waits_for_wrapper_start(self):
        task = await self.broker.run_command(
            {
                "label": "long sentinel",
                "cwd": str(self.root),
                "shell": "zsh",
                "command": "sleep 30",
                "priority": "routine",
            }
        )

        async def acknowledge_start():
            await asyncio.sleep(0.05)
            await self.broker.command_event({"task_id": task["id"], "phase": "started"})

        acknowledgement = asyncio.create_task(acknowledge_start())
        stopped = await self.broker.request_stop({"task_id": task["id"]})
        await acknowledgement
        self.assertIsNotNone(stopped["stop_requested_at"])
        cancelled = await self.broker.command_event(
            {"task_id": task["id"], "phase": "exited", "exit_status": 130}
        )
        self.assertEqual(cancelled["state"], "cancelled")

    async def test_cleanup_cancelled_by_followup(self):
        task = await self.dispatch("resume")
        await self.broker._start_task(self.broker.db.update(task["id"], state="starting"))
        row = self.broker.db.task(task["id"])
        session = row["agent_session_id"]
        # Make the native existence check deterministic without touching real rollout files.
        self.broker._native_session_exists = lambda value: value == session
        await self.broker.report({"task_id": task["id"], "state": "completed", "summary": "done"})
        await self.broker._apply_lifecycle(task["id"], "idle")
        self.broker.herdr.panes[row["pane_id"]]["agent_status"] = "idle"
        continued = await self.broker.continue_task({"task_id": task["id"], "text": "follow up"})
        self.assertEqual(continued["state"], "working")
        self.assertIsNone(self.broker.db.task(task["id"])["cleanup_deadline"])

    async def test_active_followup_steers_exact_turn_without_queue_or_generation_change(self):
        task = await self.dispatch("active-correction")
        await self.broker._start_task(self.broker.db.update(task["id"], state="starting"))
        row = self.broker.db.task(task["id"])
        self.broker._native_session_exists = lambda value: value == row["agent_session_id"]
        self.broker.herdr.panes[row["pane_id"]]["agent_status"] = "working"
        generation = row["run_generation"]
        result = await self.broker.continue_task({
            "task_id": task["id"], "text": "use the corrected release hash",
            "idempotency_key": "active-correction-1",
        })
        self.assertEqual(result["followup"]["state"], "accepted")
        self.assertEqual(result["run_generation"], generation)
        invocations = [argv for argv, _env in self.broker.appserver_calls]
        self.assertTrue(any("steer" in argv for argv in invocations))
        self.assertFalse(any("queue" in argv for argv in invocations))
        self.assertEqual(result["native_turn_id"], row["native_turn_id"])

    async def test_failed_active_turn_rejects_steer_without_releasing_future_work(self):
        task = await self.dispatch("failed-turn-correction")
        await self.broker._start_task(self.broker.db.update(task["id"], state="starting"))
        row = self.broker.db.task(task["id"])
        self.broker._native_session_exists = lambda value: value == row["agent_session_id"]
        self.broker.herdr.panes[row["pane_id"]]["agent_status"] = "working"
        async def rejected(argv, timeout=30, env=None):
            self.assertIn("steer", argv)
            return {"thread_id": row["agent_session_id"], "delivery": "rejected",
                    "authoritative_turn_id": row["native_turn_id"],
                    "authoritative_turn_status": "failed", "reason": "turn failed"}
        self.broker._run_json_required = rejected
        result = await self.broker.continue_task({
            "task_id": task["id"], "text": "do not cancel the valid release",
            "idempotency_key": "failed-turn-correction-1",
        })
        self.assertEqual(result["state"], "unknown")
        self.assertEqual(result["followup"]["state"], "rejected")
        self.assertEqual(result["run_generation"], row["run_generation"])
        self.assertEqual(result["native_turn_id"], row["native_turn_id"])

    async def test_uncertain_active_steer_is_durable_and_never_replayed(self):
        task = await self.dispatch("uncertain-steer")
        await self.broker._start_task(self.broker.db.update(task["id"], state="starting"))
        row = self.broker.db.task(task["id"])
        self.broker._native_session_exists = lambda value: value == row["agent_session_id"]
        self.broker.herdr.panes[row["pane_id"]]["agent_status"] = "working"
        attempts = 0
        async def uncertain(argv, timeout=30, env=None):
            nonlocal attempts
            attempts += 1
            raise RuntimeError("response lost after possible delivery")
        self.broker._run_json_required = uncertain
        params = {"task_id": task["id"], "text": "preserve this exact intent",
                  "idempotency_key": "uncertain-steer-1"}
        first = await self.broker.continue_task(params)
        second = await self.broker.continue_task(params)
        self.assertEqual(attempts, 1)
        self.assertEqual(first["followup"]["state"], "uncertain")
        self.assertEqual(second["followup"]["state"], "uncertain")
        status = self.broker.status(task["id"])["tasks"][0]
        self.assertIn("preserve this exact intent", status["followups"][0]["text"])
        self.assertEqual(status["run_generation"], row["run_generation"])

    async def test_owned_legacy_queue_is_preserved_before_delete_and_foreign_input_isolated(self):
        task = await self.dispatch("legacy-reconcile")
        await self.broker._start_task(self.broker.db.update(task["id"], state="starting"))
        row = self.broker.db.task(task["id"])
        self.broker._native_session_exists = lambda value: value == row["agent_session_id"]
        self.broker.herdr.panes[row["pane_id"]]["agent_status"] = "working"
        owned_text = self.broker._followup_prompt(row, "old stale instruction")
        owned = {"id": "queued-owned", "clientUserMessageId": "legacy-client",
                 "input": [{"type": "text", "text": owned_text}]}
        foreign = {"id": "queued-manual", "clientUserMessageId": "manual-client",
                   "input": [{"type": "text", "text": "manual future request"}]}
        deleted = []
        async def appserver(argv, timeout=30, env=None):
            if "queue-list" in argv:
                return {"thread_id": row["agent_session_id"], "submissions": [owned, foreign]}
            if "queue-delete" in argv:
                preserved = self.broker.db.preserved_legacy_followups(task["id"])
                self.assertEqual(preserved[0]["input"], owned["input"])
                deleted.append(argv[argv.index("--queued-submission-id") + 1])
                return {"thread_id": row["agent_session_id"],
                        "queued_submission_id": deleted[-1], "deleted": True}
            if "steer" in argv:
                return {"thread_id": row["agent_session_id"], "turn_id": row["native_turn_id"],
                        "client_user_message_id": argv[argv.index("--client-user-message-id") + 1],
                        "delivery": "accepted"}
            self.fail(argv)
        self.broker._run_json_required = appserver
        result = await self.broker.continue_task({
            "task_id": task["id"], "text": "current correction",
            "idempotency_key": "legacy-reconcile-1", "supersede_pending": True,
        })
        self.assertEqual(result["followup"]["state"], "accepted")
        self.assertEqual(deleted, ["queued-owned"])
        preserved = self.broker.status(task["id"])["tasks"][0]["preserved_legacy_followups"]
        self.assertEqual(preserved[0]["state"], "superseded")
        self.assertEqual(preserved[0]["input"], owned["input"])

    async def test_legacy_delete_race_is_truthful_and_prevents_new_steer(self):
        task = await self.dispatch("legacy-delete-race")
        await self.broker._start_task(self.broker.db.update(task["id"], state="starting"))
        row = self.broker.db.task(task["id"])
        self.broker._native_session_exists = lambda value: value == row["agent_session_id"]
        self.broker.herdr.panes[row["pane_id"]]["agent_status"] = "working"
        owned = {"id": "queued-race", "clientUserMessageId": "legacy-race",
                 "input": [{"type": "text", "text": self.broker._followup_prompt(row, "stale")}]}
        calls = []
        async def appserver(argv, timeout=30, env=None):
            calls.append(argv)
            if "queue-list" in argv:
                return {"thread_id": row["agent_session_id"], "submissions": [owned]}
            if "queue-delete" in argv:
                return {"thread_id": row["agent_session_id"],
                        "queued_submission_id": "queued-race", "deleted": False}
            self.fail("steer must not run after uncertain delete")
        self.broker._run_json_required = appserver
        result = await self.broker.continue_task({
            "task_id": task["id"], "text": "new correction",
            "idempotency_key": "legacy-race-1", "supersede_pending": True,
        })
        self.assertEqual(result["state"], "unknown")
        self.assertEqual(result["followup"]["state"], "uncertain")
        self.assertFalse(any("steer" in argv for argv in calls))
        preserved = self.broker.status(task["id"])["tasks"][0]["preserved_legacy_followups"]
        self.assertEqual(preserved[0]["state"], "delete_uncertain")

    async def test_same_turn_steer_keeps_generation_and_next_terminal_report_truthful(self):
        task = await self.dispatch("terminal-after-steer")
        await self.broker._start_task(self.broker.db.update(task["id"], state="starting"))
        row = self.broker.db.task(task["id"])
        self.broker._native_session_exists = lambda value: value == row["agent_session_id"]
        self.broker.herdr.panes[row["pane_id"]]["agent_status"] = "working"
        continued = await self.broker.continue_task({
            "task_id": task["id"], "text": "correct current work",
            "idempotency_key": "terminal-after-steer-1",
        })
        failed = await self.broker.report({
            "task_id": task["id"], "state": "failed", "summary": "active turn failed transiently",
        })
        event = self.broker.db.conn.execute(
            "SELECT generation FROM event_journal WHERE event_id=?", (failed["terminal_event_id"],)
        ).fetchone()
        self.assertEqual(continued["run_generation"], row["run_generation"])
        self.assertEqual(event["generation"], row["run_generation"])

    def test_worker_helpers_inherit_nondefault_owned_appserver_endpoint(self):
        self.broker.config_path.write_text(json.dumps({
            "app_server_socket": "/owned/appserver.sock",
            "app_server_remote": "unix:///owned/appserver.sock",
        }))
        env = self.broker._appserver_env()
        self.assertEqual(env["CODEX_APP_SERVER_SOCKET"], "/owned/appserver.sock")
        self.assertEqual(env["CODEX_APP_SERVER_REMOTE"], "unix:///owned/appserver.sock")

    async def test_owned_pane_transport_uncertainty_does_not_replay_resume(self):
        task = await self.dispatch("rebind-retained-session")
        await self.broker._start_task(self.broker.db.update(task["id"], state="starting"))
        row = self.broker.db.task(task["id"])
        self.broker._native_session_exists = lambda value: value == row["agent_session_id"]
        await self.broker.report({"task_id": task["id"], "state": "completed", "summary": "done"})
        old_name, old_tab, old_pane = row["agent_name"], row["tab_id"], row["pane_id"]
        original_request = self.broker.herdr.request

        async def stale_agent_request(method, params=None, timeout=65):
            if method == "agent.get" and (params or {}).get("target") == old_name:
                raise RuntimeError("agent_not_found")
            return await original_request(method, params, timeout)

        self.broker.herdr.request = stale_agent_request
        continued = await self.broker.continue_task({"task_id": task["id"], "text": "resume safely"})
        self.assertEqual(continued["state"], "completed")
        retained = self.broker.db.task(task["id"])
        self.assertEqual(retained["tab_id"], old_tab)
        self.assertEqual(retained["pane_id"], old_pane)
        self.assertEqual(retained["agent_session_id"], row["agent_session_id"])
        self.assertEqual(retained["resume_count"], 0)
        self.assertEqual(retained["herdr_state"], "unknown")
        self.assertIn("no follow-up was sent", retained["summary"])

    async def test_missing_retained_binding_resumes_same_thread_in_new_tab(self):
        task = await self.dispatch("lost-binding")
        await self.broker._start_task(self.broker.db.update(task["id"], state="starting"))
        row = self.broker.db.task(task["id"])
        self.broker._native_session_exists = lambda value: value == row["agent_session_id"]
        await self.broker.report({"task_id": task["id"], "state": "completed", "summary": "done"})
        self.broker.herdr.tabs.pop(row["tab_id"])
        self.broker.herdr.panes.pop(row["pane_id"])

        continued = await self.broker.continue_task({"task_id": task["id"], "text": "resume safely"})
        rebound = self.broker.db.task(task["id"])
        self.assertEqual(continued["state"], "working")
        self.assertNotEqual(rebound["tab_id"], row["tab_id"])
        self.assertNotEqual(rebound["pane_id"], row["pane_id"])
        self.assertEqual(rebound["agent_session_id"], row["agent_session_id"])
        self.assertEqual(rebound["resume_count"], 1)
        started = [params for method, params in self.broker.herdr.calls if method == "agent.start"]
        self.assertIn(["--remote", "unix://"], [started[-1]["args"][i:i + 2]
                                                for i in range(len(started[-1]["args"]) - 1)])
        self.assertIn(
            f'shell_environment_policy.set.HERDR_PANE_ID="{rebound["pane_id"]}"',
            started[-1]["args"],
        )
        rebound_panes = [params for method, params in self.broker.herdr.calls
                         if method in {"workspace.create", "tab.create"}
                         and (params.get("env") or {}).get("CONTROL_TASK_ID") == task["id"]]
        self.assertEqual(
            rebound_panes[-1]["env"]["CODEX_HOME"],
            str(self.root / "remote-client-codex-home"),
        )
        self.assertEqual(started[-1]["args"][-2:], ["resume", row["agent_session_id"]])

    async def test_resume_attach_failure_preserves_completed_result_without_phantom_working(self):
        task = await self.dispatch("bounded-attach-failure")
        await self.broker._start_task(self.broker.db.update(task["id"], state="starting"))
        row = self.broker.db.task(task["id"])
        self.broker._native_session_exists = lambda value: value == row["agent_session_id"]
        await self.broker.report({"task_id": task["id"], "state": "completed", "summary": "durable result"})
        self.broker.herdr.tabs.pop(row["tab_id"])
        self.broker.herdr.panes.pop(row["pane_id"])

        async def refused_attach(**kwargs):
            raise RuntimeError("visible Codex TUI could not attach to pane: bounded timeout")

        self.broker._start_visible_codex = refused_attach
        result = await self.broker.continue_task({"task_id": task["id"], "text": "resume later"})
        retained = self.broker.db.task(task["id"])
        self.assertEqual(result["state"], "completed")
        self.assertEqual(retained["state"], "completed")
        self.assertEqual(retained["resume_count"], 0)
        self.assertEqual(retained["herdr_state"], "unknown")
        self.assertIn("no follow-up was sent", retained["summary"])
        self.assertIn("bounded timeout", retained["summary"])

    async def test_reused_same_cwd_ids_never_attach_or_prompt_foreign_worker(self):
        old = await self.dispatch("old-completed")
        await self.broker._start_task(self.broker.db.update(old["id"], state="starting"))
        old_row = self.broker.db.task(old["id"])
        self.broker._native_session_exists = lambda value: value == old_row["agent_session_id"]
        await self.broker.report({"task_id": old["id"], "state": "completed", "summary": "old result"})
        foreign = await self.dispatch("foreign-audit")
        foreign_row = self.broker.db.update(
            foreign["id"], state="working", workspace_id=old_row["workspace_id"],
            tab_id=old_row["tab_id"], pane_id=old_row["pane_id"], agent_name="foreign-agent",
            agent_session_id="01a08362-8c74-7171-be8d-81f0b95c4aea", herdr_state="working",
        )
        foreign_pane = self.broker.herdr.panes[old_row["pane_id"]]
        foreign_pane.update({
            "agent_name": foreign_row["agent_name"], "agent_status": "working",
            "agent_session": {"value": foreign_row["agent_session_id"]},
            "process_name": "codex",
            "process_argv": ["codex", "--remote", "unix://", "resume", foreign_row["agent_session_id"]],
        })

        continued = await self.broker.continue_task({"task_id": old["id"], "text": "resume old"})
        rebound = self.broker.db.task(old["id"])
        self.assertEqual(continued["state"], "working")
        self.assertNotEqual(rebound["pane_id"], old_row["pane_id"])
        self.assertEqual(self.broker.db.task(foreign["id"])["pane_id"], old_row["pane_id"])
        self.assertNotIn("prompt_text", foreign_pane)
        self.assertNotIn("sent_text", foreign_pane)

    async def test_reused_ids_are_isolated_from_lifecycle_and_cleanup(self):
        old = await self.dispatch("old-terminal")
        await self.broker._start_task(self.broker.db.update(old["id"], state="starting"))
        old_row = self.broker.db.task(old["id"])
        await self.broker.report({"task_id": old["id"], "state": "completed", "summary": "retained result"})
        self.broker.db.update(old["id"], cleanup_deadline=0)
        foreign = await self.dispatch("foreign-owner")
        foreign_row = self.broker.db.update(
            foreign["id"], state="working", workspace_id=old_row["workspace_id"],
            tab_id=old_row["tab_id"], pane_id=old_row["pane_id"], agent_name="foreign-owner",
            agent_session_id="01a08362-8c74-7171-be8d-81f0b95c4aea", herdr_state="working",
        )
        pane = self.broker.herdr.panes[old_row["pane_id"]]
        pane.update({
            "agent_name": foreign_row["agent_name"], "agent_status": "working",
            "agent_session": {"value": foreign_row["agent_session_id"]},
            "process_name": "codex",
            "process_argv": ["codex", "--remote", "unix://", "resume", foreign_row["agent_session_id"]],
        })

        await self.broker._cleanup_after(old["id"], 0)
        self.broker.db.update(old["id"], cleanup_deadline=0)
        await self.broker._apply_lifecycle(old["id"], "working")
        await self.broker._handle_event("pane.updated", {"pane_id": old_row["pane_id"], "agent_status": "working"})
        retained = self.broker.db.task(old["id"])
        self.assertEqual(retained["state"], "completed")
        self.assertEqual(self.broker.db.task(foreign["id"])["state"], "working")
        self.assertIn(old_row["tab_id"], self.broker.herdr.tabs)
        events = [entry["event"] for entry in self.broker.db.transitions(old["id"])]
        self.assertIn("lifecycle_binding_foreign_isolated", events)
        self.assertIn("cleanup_binding_foreign_isolated", events)

    async def test_stale_native_metadata_does_not_replace_exact_thread(self):
        task = await self.dispatch("stale-native-metadata")
        await self.broker._start_task(self.broker.db.update(task["id"], state="starting"))
        row = self.broker.db.task(task["id"])
        pane = self.broker.herdr.panes[row["pane_id"]]
        pane["agent_session"] = {}
        pane["agent_status"] = "working"
        await self.broker.reconcile()
        retained = self.broker.db.task(task["id"])
        self.assertEqual(retained["agent_session_id"], row["agent_session_id"])
        self.assertEqual(retained["state"], "working")

    async def test_followup_clears_superseded_graceful_stop_request(self):
        task = await self.dispatch("resume-after-stop")
        await self.broker._start_task(self.broker.db.update(task["id"], state="starting"))
        row = self.broker.db.task(task["id"])
        self.broker._native_session_exists = lambda value: value == row["agent_session_id"]
        await self.broker.request_stop({"task_id": task["id"]})
        resumed = await self.broker.continue_task({"task_id": task["id"], "text": "resume safely"})
        self.assertEqual(resumed["state"], "working")
        self.assertIsNone(self.broker.db.task(task["id"])["stop_requested_at"])

    async def test_appserver_completion_repairs_missed_herdr_settle(self):
        task = await self.dispatch("missed-settle")
        await self.broker._start_task(self.broker.db.task(task["id"]))
        row = self.broker.db.task(task["id"])
        self.broker.herdr.panes[row["pane_id"]]["agent_status"] = "working"
        self.broker.db.update(task["id"], herdr_state="working")

        async def completed_turn(argv, timeout=30, env=None):
            return {"thread_id": row["agent_session_id"], "turn_status": "completed"}

        self.broker._run_json_required = completed_turn
        await self.broker.report(
            {"task_id": task["id"], "state": "completed", "summary": "done"}
        )
        await asyncio.sleep(0.08)
        repaired = self.broker.db.task(task["id"])
        self.assertEqual(repaired["herdr_state"], "closed")
        self.assertEqual(repaired["session_state"], "dormant")
        events = [entry["event"] for entry in self.broker.db.transitions(task["id"])]
        self.assertIn("appserver_turn_settled", events)

    async def test_native_completion_without_report_is_attention_unknown(self):
        task = await self.dispatch("missing-native-report")
        await self.broker._start_task(self.broker.db.task(task["id"]))
        row = self.broker.db.task(task["id"])
        self.broker.native_turn_status[row["native_turn_id"]] = "completed"
        await self.broker._monitor_native_turn(task["id"], row["native_turn_id"])
        missing = self.broker.db.task(task["id"])
        self.assertEqual(missing["state"], "unknown")
        self.assertEqual(missing["priority"], "attention")
        self.assertIsNone(missing["terminal_reported_at"])
        self.assertIn("no semantic result is trusted", missing["summary"])
        await asyncio.sleep(0)
        self.assertIn((task["id"], "unknown", missing["summary"]), self.broker.alerts)

    async def test_explicit_report_wins_over_native_terminal_monitor(self):
        task = await self.dispatch("reported-native-terminal")
        await self.broker._start_task(self.broker.db.task(task["id"]))
        row = self.broker.db.task(task["id"])
        self.broker.native_turn_status[row["native_turn_id"]] = "failed"
        await self.broker.report({"task_id": task["id"], "state": "completed", "summary": "authoritative result"})
        await self.broker._monitor_native_turn(task["id"], row["native_turn_id"])
        retained = self.broker.db.task(task["id"])
        self.assertEqual(retained["state"], "completed")
        self.assertEqual(retained["summary"], "authoritative result")

    async def test_stale_turn_monitor_cannot_overwrite_followup_turn(self):
        task = await self.dispatch("stale-native-monitor")
        await self.broker._start_task(self.broker.db.task(task["id"]))
        row = self.broker.db.task(task["id"])
        stale_turn = row["native_turn_id"]
        self.broker.db.update(task["id"], native_turn_id="replacement-turn", state="working")
        self.broker.native_turn_status[stale_turn] = "completed"
        await self.broker._monitor_native_turn(task["id"], stale_turn)
        retained = self.broker.db.task(task["id"])
        self.assertEqual(retained["state"], "working")
        self.assertEqual(retained["native_turn_id"], "replacement-turn")

    async def test_expired_followup_never_creates_session(self):
        task = await self.dispatch("expired")
        self.broker.db.update(task["id"], agent_session_id="01a07cca-14a8-7ec1-aeff-a33fa6124e27", session_state="expired")
        with self.assertRaisesRegex(ValueError, "expired after 30 days"):
            await self.broker.continue_task({"task_id": task["id"], "text": "resume"})

    async def test_waiting_reply_stop_and_process_exit(self):
        task = await self.dispatch("question")
        await self.broker._start_task(self.broker.db.task(task["id"]))
        waiting = await self.broker.report(
            {
                "task_id": task["id"],
                "state": "waiting_human",
                "summary": "need a choice",
                "question": "Proceed?",
                "priority": "attention",
            }
        )
        self.assertEqual(waiting["state"], "waiting_human")
        self.broker.herdr.panes[waiting["pane_id"]]["agent_status"] = "idle"
        resumed = await self.broker.reply({"task_id": task["id"], "text": "Proceed safely"})
        self.assertEqual(resumed["state"], "working")
        stopped = await self.broker.request_stop({"task_id": task["id"]})
        self.assertIsNotNone(stopped["stop_requested_at"])
        await self.broker._handle_event("pane.exited", {"pane_id": stopped["pane_id"]})
        self.assertEqual(self.broker.db.task(task["id"])["state"], "failed")

    async def test_waiting_reply_queues_when_turn_has_not_settled(self):
        task = await self.dispatch("question-race")
        await self.broker._start_task(self.broker.db.task(task["id"]))
        waiting = await self.broker.report(
            {
                "task_id": task["id"],
                "state": "waiting_human",
                "summary": "need a choice",
                "question": "Which word?",
            }
        )
        self.broker.herdr.panes[waiting["pane_id"]]["agent_status"] = "working"
        queued: list[list[str]] = []

        async def fake_run(argv, timeout=30):
            queued.append(argv)

        self.broker._run_required = fake_run
        resumed = await self.broker.reply({"task_id": task["id"], "text": "amber"})
        self.assertEqual(resumed["state"], "working")
        self.assertEqual(queued[0][1:3], ["queue", "--remote"])
        events = [entry["event"] for entry in self.broker.db.transitions(task["id"])]
        self.assertIn("reply_queued", events)

    async def test_reconnect_reconciliation_marks_missing_worker_unknown(self):
        task = await self.dispatch("reconnect")
        await self.broker._start_task(self.broker.db.task(task["id"]))
        pane_id = self.broker.db.task(task["id"])["pane_id"]
        self.broker.herdr.panes.pop(pane_id)
        await self.broker.reconcile()
        self.assertEqual(self.broker.db.task(task["id"])["state"], "unknown")

    async def test_authorized_plan_completion_promotes_once_and_plan_only_stops(self):
        feature = await self.broker.create_feature({
            "feature_id": "end-to-end", "target": "workflow-e2e", "cwd": str(self.root), "title": "E2E",
            "scope": "end_to_end", "required_stages": ["plan", "implementation"],
            "actions": {"plan": {"prompt": "make plan"}, "implementation": {"prompt": "implement", "model": "gpt-5.6-terra"}},
        })
        plan = next(s for s in feature["stages"] if s["stage"] == "plan")
        self.assertEqual(plan["state"], "pending")  # feature object predates its one-step advance
        await self.broker.advance_feature("end-to-end")
        plan_task = self.broker.db.feature("end-to-end")["stages"][1]["task_id"]
        self.assertIn("select a concise Conventional Commit", self.broker.db.task(plan_task)["prompt_pending"])
        self.assertIn("Do not pause for human commit-wording alignment", self.broker.db.task(plan_task)["prompt_pending"])
        await self.broker.report({"task_id": plan_task, "state": "completed", "summary": "plan artifact"})
        stages = {s["stage"]: s for s in self.broker.db.feature("end-to-end")["stages"]}
        implementation_task = stages["implementation"]["task_id"]
        self.assertIsNotNone(implementation_task)
        await self.broker.advance_for_task(plan_task)  # duplicate/replayed consumption
        self.assertEqual(self.broker.db.feature("end-to-end")["stages"][2]["task_id"], implementation_task)

        await self.broker.create_feature({
            "feature_id": "plan-only", "target": "workflow-plan", "cwd": str(self.root), "title": "Plan only",
            "scope": "plan_only", "required_stages": ["plan", "implementation"],
            "actions": {"plan": {"prompt": "make plan"}, "implementation": {"prompt": "must not run"}},
        })
        await self.broker.advance_feature("plan-only")
        plan_only_task = self.broker.db.feature("plan-only")["stages"][1]["task_id"]
        await self.broker.report({"task_id": plan_only_task, "state": "completed", "summary": "plan"})
        self.assertIsNone(self.broker.db.feature("plan-only")["stages"][2]["task_id"])

    async def test_feature_fail_closed_gates_and_failed_child_remain_open(self):
        await self.broker.create_feature({
            "feature_id": "gates", "target": "workflow-gates", "cwd": str(self.root), "title": "Gates",
            "scope": "end_to_end", "required_stages": ["review", "ci", "install", "live_uat", "accepted"],
            "actions": {"review": {"prompt": "review"}},
        })
        await self.broker.advance_feature("gates")
        review_task = next(s for s in self.broker.db.feature("gates")["stages"] if s["stage"] == "review")["task_id"]
        await self.broker.report({"task_id": review_task, "state": "completed", "summary": "review says pass"})
        feature = self.broker.db.feature("gates")
        self.assertEqual(next(s for s in feature["stages"] if s["stage"] == "review")["state"], "awaiting_evidence")
        self.assertEqual(feature["state"], "open")
        with self.assertRaisesRegex(ValueError, "requires review"):
            self.broker.db.record_feature_evidence("gates", "review", "ci", "wrong")
        for stage, kind in (("review", "review"), ("ci", "ci"), ("install", "installed_runtime"), ("live_uat", "live_uat"), ("accepted", "acceptance")):
            self.broker.db.record_feature_evidence("gates", stage, kind, f"{stage}-evidence")
        self.assertEqual(self.broker.db.feature("gates")["state"], "accepted")

        await self.broker.create_feature({"feature_id": "failed-child", "target": "workflow-fail", "cwd": str(self.root), "title": "Failure", "scope": "end_to_end", "required_stages": ["plan"], "actions": {"plan": {"prompt": "plan"}}})
        await self.broker.advance_feature("failed-child")
        task_id = next(s for s in self.broker.db.feature("failed-child")["stages"] if s["stage"] == "plan")["task_id"]
        await self.broker.report({"task_id": task_id, "state": "failed", "summary": "deferred external failure"})
        self.assertEqual(next(s for s in self.broker.db.feature("failed-child")["stages"] if s["stage"] == "plan")["state"], "blocked")
        self.assertEqual(self.broker.db.feature("failed-child")["state"], "open")

    async def test_protocol20_journal_consumer_preserves_required_dependency_gate(self):
        task = self.broker.db.add_task({"id": "xcsh-journal", "target": "xcsh-journal", "cwd": str(self.root),
                                        "prompt": "native", "summary": "queued", "parent_id": None,
                                        "priority": "normal", "work_kind": "xcsh"})
        self.broker.db.update(task["id"], state="working", pane_id="w1:p1", agent_session_id=self.native_launch["session_header"]["id"], native_turn_id="turn-1")
        executable = self.xcsh_executable
        executable_sha256 = self.xcsh_executable_sha256
        request = {"execution_id": task["id"], "generation": 0, "native_launch": self.native_launch, "workspace_id": "w1", "cwd": str(self.root), "text": "native", "xcsh_executable_sha256": executable_sha256}
        encoded = json.dumps(request, sort_keys=True, separators=(",", ":"))
        self.broker.db.claim_native_generation(task["id"], generation=0, session_id=self.native_launch["session_header"]["id"], workspace_id="w1", request_sha256=hashlib.sha256(encoded.encode()).hexdigest(), request_json=encoded)
        argv = [executable,"--mode","json","--session-dir",self.native_launch["session_dir"],"--resume",self.native_launch["session_path"],"--model",self.native_launch["model"],"--tools","read","--no-mcp","--no-lsp","--no-memories","--no-skills","--no-rules","--no-pty","--print","native"]
        self.broker.db.admit_native_generation(task["id"], 0, {"execution_id": "backend-journal", "backend_execution_id": "backend-journal", "semantic_execution_id": task["id"], "generation": 0, "native_producer": "xcsh", "producer_session_id": self.native_launch["session_header"]["id"], "workspace_id": "w1", "cwd": str(self.root), "native_launch": self.native_launch, "command": {"mode": "argv", "argv": argv}, "native_executable": {"canonical_path": executable, "sha256": executable_sha256}, "injected_env": {"HERDR_EXECUTION_ID": task["id"], "HERDR_EXECUTION_GENERATION": "0"}, "tab_id": "w1:t1", "pane_id": "w1:p1"})
        self.broker.db.create_feature({"feature_id": "journal-feature", "target": "journal-feature", "cwd": str(self.root),
                                       "title": "Journal", "scope": "end_to_end", "required_stages": ["implementation", "tests"],
                                       "children": {"implementation": task["id"]}, "actions": {"tests": {"prompt": "must not run"}}})
        self.broker.db.ensure_feature_dependency("journal-feature", "native_consumer", before_stage="tests", blocker="await installed capability")
        self.broker.config_path.write_text(json.dumps({"agent_turn_consumer_enabled": True, "agent_turn_producer": "xcsh"}))
        result = "accepted result"
        self.broker.herdr.agent_turns = [
            {"revision": 1, "report": {"execution_id": task["id"], "pane_id": "w1:p1", "producer": "xcsh", "session_id": self.native_launch["session_header"]["id"], "turn_id": "turn-1", "generation": 0, "event_revision": 1, "state": "starting"}},
            {"revision": 2, "report": {"execution_id": task["id"], "pane_id": "w1:p1", "producer": "xcsh", "session_id": self.native_launch["session_header"]["id"], "turn_id": "turn-1", "generation": 0, "event_revision": 2, "state": "completed", "result": result, "result_digest": hashlib.sha256(result.encode()).hexdigest()}},
        ]
        consumed = await self.broker.consume_native_turns()
        self.assertEqual(consumed["last_revision"], 2)
        self.assertEqual(self.broker.db.task(task["id"])["state"], "completed")
        stages = {s["stage"]: s for s in self.broker.db.feature("journal-feature")["stages"]}
        self.assertEqual(stages["implementation"]["state"], "completed")
        self.assertEqual(stages["native_consumer"]["state"], "blocked")
        self.assertIsNone(stages["tests"]["task_id"])

    async def test_manager_reconnect_uses_exact_absolute_launch(self):
        created = await self.broker.herdr.request(
            "workspace.create", {"cwd": str(self.root), "label": "control"}
        )
        pane = created["root_pane"]
        pane["cwd"] = str(self.root)
        self.broker.config_path.write_text(
            json.dumps(
                {
                    "manager_thread_id": "01a07cad-d970-7393-82a4-14ae9a1c16ee",
                    "manager_cwd": str(self.root),
                    "manager_workspace_id": created["workspace"]["workspace_id"],
                    "manager_pane_id": pane["pane_id"],
                    "app_server_remote": "unix://",
                    "app_server_socket": "/owned/appserver.sock",
                    "profile": "control-manager",
                    "codex_binary": "/bin/true",
                }
            )
        )
        await self.broker.reconcile()
        sent = pane["sent_text"]
        self.assertTrue(sent.startswith(
            f"/usr/bin/env CODEX_HOME={self.root}/remote-client-codex-home /usr/bin/true "
            "--disable hooks --remote unix:///owned/appserver.sock"
        ))
        self.assertIn("resume 01a07cad-d970-7393-82a4-14ae9a1c16ee", sent)

    async def test_manager_runtime_transfers_and_restores_ownership_without_broker_restart(self):
        """Guarded handoff and rollback dynamically stop/restart the observer."""
        thread_id = "01a07cad-d970-7393-82a4-14ae9a1c16ee"
        self.broker.herdr.socket_path = self.root / "herdr.sock"
        self.broker.config_path.write_text(json.dumps({"manager_thread_id": thread_id,
                                                        "manager_cwd": str(self.root),
                                                        "supervisor_owns_recovery": False}))
        launched = []

        class Process:
            def __init__(self):
                self.returncode = None
                self.stdout = asyncio.StreamReader()
                self.stdout.feed_data(json.dumps({"ready": True, "thread_id": thread_id}).encode() + b"\n")
                self.done = asyncio.get_running_loop().create_future()
            async def wait(self):
                return await asyncio.shield(self.done)
            def terminate(self):
                if self.returncode is None:
                    self.returncode = -15; self.done.set_result(self.returncode)
            def kill(self): self.terminate()

        async def spawn(*args, **kwargs):
            process = Process(); launched.append(process); return process

        with patch("control_broker.asyncio.create_subprocess_exec", side_effect=spawn):
            runtime = asyncio.create_task(self.broker._manager_runtime_loop())
            for _ in range(50):
                if len(launched) == 1: break
                await asyncio.sleep(.01)
            self.assertEqual(len(launched), 1)
            self.broker.config_path.write_text(json.dumps({"manager_thread_id": thread_id,
                                                            "manager_cwd": str(self.root),
                                                            "supervisor_owns_recovery": True}))
            for _ in range(50):
                if launched[0].returncode is not None: break
                await asyncio.sleep(.03)
            self.assertIsNotNone(launched[0].returncode)
            self.broker.config_path.write_text(json.dumps({"manager_thread_id": thread_id,
                                                            "manager_cwd": str(self.root),
                                                            "supervisor_owns_recovery": False}))
            for _ in range(80):
                if len(launched) == 2: break
                await asyncio.sleep(.03)
            self.assertEqual(len(launched), 2)
            self.broker.stopping.set()
            launched[-1].terminate()
            await asyncio.wait_for(runtime, 2)

    async def test_supervisor_claimed_topology_reconcile_reattaches_exact_thread_once(self):
        """Supervisor ownership blocks ordinary relaunch but permits one claimed repair."""
        created = await self.broker.herdr.request("workspace.create", {"cwd": str(self.root), "label": "control"})
        pane = created["root_pane"]
        self.broker.config_path.write_text(json.dumps({
            "manager_thread_id": "01a07cad-d970-7393-82a4-14ae9a1c16ee", "manager_cwd": str(self.root),
            "manager_workspace_id": created["workspace"]["workspace_id"], "manager_pane_id": pane["pane_id"],
            "app_server_remote": "unix://", "profile": "control-manager", "codex_binary": "/bin/true",
            "supervisor_owns_recovery": True,
        }))
        await self.broker.reconcile()
        self.assertNotIn("sent_text", pane, "ordinary broker reconcile must yield to the supervisor")
        action = "recovery-" + "a" * 32
        await self.broker.handle("reconcile_topology", {"recovery_action_id": action})
        await self.broker.handle("reconcile_topology", {"recovery_action_id": action})
        sends = [call for call in self.broker.herdr.calls if call[0] == "pane.send_text"]
        self.assertEqual(len(sends), 1, "claimed topology retries must not duplicate a manager admission")
        self.assertIn("resume 01a07cad-d970-7393-82a4-14ae9a1c16ee", sends[0][1]["text"])
        self.assertFalse(any(call[0] == "agent.prompt" for call in self.broker.herdr.calls))
        if self.broker.manager_reconnect_task:
            self.broker.manager_reconnect_task.cancel()
            await asyncio.gather(self.broker.manager_reconnect_task, return_exceptions=True)

    async def test_stale_snapshot_cannot_replace_configured_manager_during_launch(self):
        """A periodic inventory cannot invent wP:p1 while wE:p1 is resuming."""
        created = await self.broker.herdr.request("workspace.create", {"cwd": str(self.root), "label": "control"})
        pane = created["root_pane"]
        config = {
            "manager_thread_id": "01a07cad-d970-7393-82a4-14ae9a1c16ee", "manager_cwd": str(self.root),
            "manager_workspace_id": created["workspace"]["workspace_id"], "manager_pane_id": pane["pane_id"],
            "app_server_remote": "unix://", "profile": "control-manager", "codex_binary": "/bin/true",
            "supervisor_owns_recovery": True,
        }
        self.broker.config_path.write_text(json.dumps(config))
        original_bytes = self.broker.config_path.read_bytes()
        admitted = await self.broker._ensure_manager({}, {pane["pane_id"]: pane}, allow_supervisor_reattach=True)
        self.assertEqual(admitted["state"], "admitted")
        self.assertTrue(self.broker.manager_launching)
        with self.assertRaisesRegex(RuntimeError, "transient while reattachment is in progress"):
            await self.broker._reconcile_root_topology({"workspaces": [], "tabs": [], "panes": [], "agents": []})
        self.assertEqual(self.broker.config_path.read_bytes(), original_bytes)
        self.assertFalse(any(name in {"workspace.create", "tab.create"} for name, _ in self.broker.herdr.calls[1:]))
        self.broker.manager_reconnect_task.cancel()
        await asyncio.gather(self.broker.manager_reconnect_task, return_exceptions=True)

    async def test_stale_snapshot_live_exact_binding_preserves_manager_and_worker_admission(self):
        created = await self.broker.herdr.request("workspace.create", {"cwd": str(self.root), "label": "control"})
        pane = created["root_pane"]
        config = {
            "manager_thread_id": "01a07cad-d970-7393-82a4-14ae9a1c16ee", "manager_cwd": str(self.root),
            "manager_workspace_id": created["workspace"]["workspace_id"], "manager_pane_id": pane["pane_id"],
        }
        self.broker.config_path.write_text(json.dumps(config))
        original_bytes = self.broker.config_path.read_bytes()
        control = await self.broker._reconcile_root_topology({"workspaces": [], "tabs": [], "panes": [], "agents": []})
        self.assertEqual(control["workspace_id"], created["workspace"]["workspace_id"])
        self.assertEqual(self.broker.config_path.read_bytes(), original_bytes)
        self.assertFalse(any(name == "workspace.create" for name, _ in self.broker.herdr.calls[1:]))
        task = await self.dispatch("post-restore-control-worker")
        await self.broker._start_task(self.broker.db.task(task["id"]))
        admitted = self.broker.db.task(task["id"])
        self.assertEqual(admitted["workspace_id"], created["workspace"]["workspace_id"])
        self.assertNotEqual(admitted["pane_id"], pane["pane_id"])

    async def test_cancelled_manager_admission_keeps_uncertain_launch_gated(self):
        created = await self.broker.herdr.request("workspace.create", {"cwd": str(self.root), "label": "control"})
        pane = created["root_pane"]
        self.broker.config_path.write_text(json.dumps({
            "manager_thread_id": "01a07cad-d970-7393-82a4-14ae9a1c16ee", "manager_cwd": str(self.root),
            "manager_workspace_id": created["workspace"]["workspace_id"], "manager_pane_id": pane["pane_id"],
            "app_server_remote": "unix://", "profile": "control-manager", "codex_binary": "/bin/true",
            "supervisor_owns_recovery": True,
        }))
        with patch("control_broker.asyncio.sleep", side_effect=asyncio.CancelledError):
            with self.assertRaises(asyncio.CancelledError):
                await self.broker._ensure_manager({}, {pane["pane_id"]: pane}, allow_supervisor_reattach=True)
        self.assertTrue(self.broker.manager_launching)
        self.assertIsNotNone(self.broker.manager_reconnect_task)
        await asyncio.sleep(0)
        self.broker.manager_reconnect_task.cancel()
        await asyncio.gather(self.broker.manager_reconnect_task, return_exceptions=True)
        self.assertFalse(self.broker.manager_launching)

    async def test_strict_topology_proof_does_not_wait_for_slow_unrelated_task_reconcile(self):
        created = await self.broker.herdr.request("workspace.create", {"cwd": str(self.root), "label": "control"})
        manager = created["root_pane"]
        thread = "01a07cad-d970-7393-82a4-14ae9a1c16ee"
        manager.update({"process_name": "codex", "agent_status": "idle", "agent_session": {"value": thread}})
        unrelated = await self.broker.herdr.request("tab.create", {
            "workspace_id": created["workspace"]["workspace_id"], "cwd": str(self.root), "label": "slow-worker",
        })
        self.broker.db.add_task({"id": "slow-unrelated", "target": "slow-unrelated", "cwd": str(self.root),
            "prompt": "x", "summary": "working", "parent_id": None, "priority": "normal", "work_kind": "codex",
            "state": "working", "pane_id": unrelated["root_pane"]["pane_id"], "agent_name": "slow-unrelated"})
        self.broker.config_path.write_text(json.dumps({
            "manager_thread_id": thread, "manager_cwd": str(self.root),
            "manager_workspace_id": created["workspace"]["workspace_id"], "manager_pane_id": manager["pane_id"],
            "app_server_remote": "unix://", "profile": "control-manager", "codex_binary": "/bin/true",
            "supervisor_owns_recovery": True,
        }))
        original = self.broker.herdr.request
        async def slow_unrelated(method, params=None, timeout=65):
            if method == "agent.get" and (params or {}).get("target") == "slow-unrelated":
                await asyncio.sleep(10)
            return await original(method, params, timeout)
        self.broker.herdr.request = slow_unrelated
        result = await asyncio.wait_for(self.broker.handle("reconcile_topology", {
            "recovery_action_id": "recovery-" + "e" * 32,
        }), .25)
        self.assertEqual(result["topology_reattach"], "verified")
        self.assertFalse(any(name == "agent.get" and params.get("target") == "slow-unrelated"
                             for name, params in self.broker.herdr.calls))

    async def test_strict_topology_never_resumes_same_workspace_substitute_pane(self):
        created = await self.broker.herdr.request("workspace.create", {"cwd": str(self.root), "label": "control"})
        substitute_created = await self.broker.herdr.request("tab.create", {
            "workspace_id": created["workspace"]["workspace_id"], "cwd": str(self.root), "label": "other",
        })
        substitute = substitute_created["root_pane"]
        self.broker.config_path.write_text(json.dumps({
            "manager_thread_id": "01a07cad-d970-7393-82a4-14ae9a1c16ee", "manager_cwd": str(self.root),
            "manager_workspace_id": created["workspace"]["workspace_id"], "manager_pane_id": "missing-manager-pane",
            "app_server_remote": "unix://", "profile": "control-manager", "codex_binary": "/bin/true",
            "supervisor_owns_recovery": True,
        }))
        with self.assertRaisesRegex(RuntimeError, "configured canonical pane is missing"):
            await self.broker.handle("reconcile_topology", {"recovery_action_id": "recovery-" + "f" * 32})
        self.assertNotIn("sent_text", substitute)

    async def test_existing_exact_remote_client_repairs_only_missing_session_metadata(self):
        created = await self.broker.herdr.request("workspace.create", {"cwd": str(self.root), "label": "control"})
        pane = created["root_pane"]
        thread = "01a07cad-d970-7393-82a4-14ae9a1c16ee"
        pane.update({"process_name": "codex", "process_argv": [str(Path("/bin/true").resolve()), "--disable", "hooks", "--remote", "unix://", "-C", str(self.root), "resume", thread], "agent_status": "idle"})
        self.broker.config_path.write_text(json.dumps({
            "manager_thread_id": thread, "manager_cwd": str(self.root),
            "manager_workspace_id": created["workspace"]["workspace_id"], "manager_pane_id": pane["pane_id"],
            "app_server_remote": "unix://", "profile": "control-manager", "codex_binary": "/bin/true",
            "supervisor_owns_recovery": True,
        }))
        result = await self.broker.handle("reconcile_topology", {"recovery_action_id": "recovery-" + "c" * 32})
        self.assertEqual(result["topology_reattach"], "verified")
        self.assertEqual(pane["agent_session"]["value"], thread)
        self.assertFalse(any(call[0] == "pane.send_text" for call in self.broker.herdr.calls))

    async def test_foreign_native_session_is_never_overwritten(self):
        created = await self.broker.herdr.request("workspace.create", {"cwd": str(self.root), "label": "control"})
        pane = created["root_pane"]
        thread = "01a07cad-d970-7393-82a4-14ae9a1c16ee"
        pane.update({"process_name": "codex", "process_argv": [str(Path("/bin/true").resolve()), "--disable", "hooks", "--remote", "unix://", "-C", str(self.root), "resume", thread], "agent_status": "working", "agent_session": {"value": "foreign-session"}})
        self.broker.config_path.write_text(json.dumps({
            "manager_thread_id": thread, "manager_cwd": str(self.root),
            "manager_workspace_id": created["workspace"]["workspace_id"], "manager_pane_id": pane["pane_id"],
            "app_server_remote": "unix://", "profile": "control-manager", "codex_binary": "/bin/true",
            "supervisor_owns_recovery": True,
        }))
        with self.assertRaisesRegex(RuntimeError, "topology is unverified"):
            await self.broker.handle("reconcile_topology", {"recovery_action_id": "recovery-" + "d" * 32})
        self.assertEqual(pane["agent_session"]["value"], "foreign-session")
        self.assertFalse(any(call[0] == "pane.report_agent_session" for call in self.broker.herdr.calls))

    async def test_claimed_repair_replaces_exact_terminally_disconnected_manager(self):
        created = await self.broker.herdr.request("workspace.create", {"cwd": str(self.root), "label": "control"})
        pane = created["root_pane"]
        thread = "01a07cad-d970-7393-82a4-14ae9a1c16ee"
        pane.update({
            "process_name": "codex",
            "process_argv": [str(Path("/bin/true").resolve()), "--disable", "hooks", "--remote", "unix://",
                             "-C", str(self.root), "resume", thread],
            # Herdr reports the fatal reconnect spinner as working in the real
            # Codex TUI; the complete terminal footer is the decisive proof.
            "agent_status": "working", "agent_session": {"value": thread},
            "output": "Automatic reconnect could not restore this session.\n"
                      "app-server session could not be restored\n"
                      "Reconnect failed — check the endpoint, then relaunch\n"
                      "Ask Codex to do anything\nctrl+c quit",
        })
        self.broker.config_path.write_text(json.dumps({
            "manager_thread_id": thread, "manager_cwd": str(self.root),
            "manager_workspace_id": created["workspace"]["workspace_id"], "manager_pane_id": pane["pane_id"],
            "app_server_remote": "unix://", "profile": "control-manager", "codex_binary": "/bin/true",
            "supervisor_owns_recovery": True,
        }))
        replacement = AsyncMock(return_value={"state": "verified", "reason": "same thread reattached"})
        with patch.object(self.broker, "_verified_supervisor_attachment_claim", return_value="claim-sha"), \
             patch.object(self.broker, "_recover_proven_lost_manager_binding", replacement):
            result = await self.broker._reconcile_manager_topology(
                allow_supervisor_reattach=True, strict=True, require_manager_reattach=True,
                recovery_action_id="recovery-" + "b" * 32, recovery_claim_sha256="claim-sha",
                recovery_claim_key="c" * 64, recovery_owner_generation="d" * 32,
            )
        self.assertEqual(result["manager_reattach"]["state"], "verified")
        self.assertNotIn(pane["pane_id"], self.broker.herdr.panes)
        replacement.assert_awaited_once()

    async def test_claimed_repair_never_closes_busy_canonical_manager(self):
        created = await self.broker.herdr.request("workspace.create", {"cwd": str(self.root), "label": "control"})
        pane = created["root_pane"]
        thread = "01a07cad-d970-7393-82a4-14ae9a1c16ee"
        pane.update({
            "process_name": "codex",
            "process_argv": [str(Path("/bin/true").resolve()), "--disable", "hooks", "--remote", "unix://",
                             "-C", str(self.root), "resume", thread],
            "agent_status": "working", "agent_session": {"value": thread},
            "output": "app-server session could not be restored\nReconnect failed — check the endpoint",
        })
        self.broker.config_path.write_text(json.dumps({
            "manager_thread_id": thread, "manager_cwd": str(self.root),
            "manager_workspace_id": created["workspace"]["workspace_id"], "manager_pane_id": pane["pane_id"],
            "app_server_remote": "unix://", "profile": "control-manager", "codex_binary": "/bin/true",
            "supervisor_owns_recovery": True,
        }))
        with patch.object(self.broker, "_verified_supervisor_attachment_claim", return_value="claim-sha"):
            result = await self.broker._reconcile_manager_topology(
                allow_supervisor_reattach=True, strict=True, require_manager_reattach=True,
                recovery_action_id="recovery-" + "e" * 32, recovery_claim_sha256="claim-sha",
                recovery_claim_key="f" * 64, recovery_owner_generation="a" * 32,
            )
        self.assertEqual(result["manager_reattach"]["state"], "verified")
        self.assertIn(pane["pane_id"], self.broker.herdr.panes)
        self.assertFalse(any(method == "pane.close" for method, _params in self.broker.herdr.calls))


if __name__ == "__main__":
    unittest.main()
