from __future__ import annotations

import io
import json
import sys
import unittest
from unittest.mock import patch

import worker_appserver


class FakeServer:
    def __init__(self, *, turn_status: str = "inProgress"):
        self.turn_status = turn_status
        self.calls: list[tuple[str, dict | None]] = []

    def request(self, method, params=None):
        self.calls.append((method, params))
        if method == "thread/read":
            return {"thread": {"id": "thread-1", "status": "active"}}
        if method == "thread/turns/list":
            return {"data": [{"id": "turn-1", "status": self.turn_status, "items": []}]}
        if method == "turn/steer":
            return {"turnId": "turn-1"}
        if method == "thread/queue/list":
            return {"data": [], "nextCursor": None}
        if method == "thread/queue/delete":
            return {"deleted": True}
        raise AssertionError(method)

    def close(self):
        pass


class WorkerAppServerTests(unittest.TestCase):
    def invoke(self, server: FakeServer, *argv: str) -> dict:
        output = io.StringIO()
        with patch.object(worker_appserver, "AppServer", return_value=server), \
             patch.object(sys, "argv", ["worker_appserver.py", *argv]), \
             patch("sys.stdout", output):
            self.assertEqual(worker_appserver.main(), 0)
        return json.loads(output.getvalue())

    def test_steer_uses_authoritative_turn_precondition_and_client_identity(self):
        server = FakeServer()
        result = self.invoke(
            server, "steer", "--thread-id", "thread-1", "--expected-turn-id", "turn-1",
            "--client-user-message-id", "followup-1", "--text", "correct current work",
        )
        self.assertEqual(result["delivery"], "accepted")
        steer = next(params for method, params in server.calls if method == "turn/steer")
        self.assertEqual(steer, {
            "threadId": "thread-1", "expectedTurnId": "turn-1",
            "clientUserMessageId": "followup-1",
            "input": [{"type": "text", "text": "correct current work"}],
        })
        read = next(params for method, params in server.calls if method == "thread/read")
        self.assertIs(read["includeTurns"], False)

    def test_failed_turn_is_rejected_without_steering_or_queueing(self):
        server = FakeServer(turn_status="failed")
        result = self.invoke(
            server, "steer", "--thread-id", "thread-1", "--expected-turn-id", "turn-1",
            "--client-user-message-id", "followup-1", "--text", "stale correction",
        )
        self.assertEqual(result["delivery"], "rejected")
        methods = [method for method, _params in server.calls]
        self.assertNotIn("turn/steer", methods)
        self.assertFalse(any(method.startswith("thread/queue/") for method in methods))

    def test_queue_delete_uses_exact_submission_identity(self):
        server = FakeServer()
        result = self.invoke(
            server, "queue-delete", "--thread-id", "thread-1",
            "--queued-submission-id", "queued-1",
        )
        self.assertTrue(result["deleted"])
        self.assertIn(("thread/queue/delete", {
            "threadId": "thread-1", "queuedSubmissionId": "queued-1",
        }), server.calls)

    def test_worker_config_binds_remote_shell_tools_to_herdr_pane(self):
        config = worker_appserver.worker_config(
            "task-1", "/tmp/control.sock", "", "gpt-5.6-sol", "low",
            "/tmp/herdr.sock", "w7", "w7:t3", "w7:p9",
        )
        environment = config["shell_environment_policy"]["set"]
        self.assertEqual(environment["HERDR_ENV"], "1")
        self.assertEqual(environment["HERDR_SOCKET_PATH"], "/tmp/herdr.sock")
        self.assertEqual(environment["HERDR_WORKSPACE_ID"], "w7")
        self.assertEqual(environment["HERDR_TAB_ID"], "w7:t3")
        self.assertEqual(environment["HERDR_PANE_ID"], "w7:p9")


if __name__ == "__main__":
    unittest.main()
