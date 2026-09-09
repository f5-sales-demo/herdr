"""Independent manager acceptance tests: abrupt death during durable admission.

All processes, databases and Herdr surfaces here are disposable fixtures.
The child exits without cleanup at a SQLite write boundary, so these tests
exercise actual rollback/reopen behavior rather than a mocked restart.
"""
import asyncio
import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

from control_mcp import TOOLS
import control_mcp
import control_client
from test_control_broker import TestBroker


CHILD = r'''
import asyncio, json, os, sys
from pathlib import Path
from test_control_broker import TestBroker
root, method, params = Path(sys.argv[1]), sys.argv[2], json.loads(sys.argv[3])
broker = TestBroker(root)
def crash(sql):
    # sqlite calls this before executing the statement. No cleanup/commit
    # occurs after the hard exit; the parent opens the surviving database.
    if sql.lstrip().upper().startswith('INSERT INTO ADMISSION_IDEMPOTENCY'):
        os._exit(91)
broker.db.conn.set_trace_callback(crash)
asyncio.run(getattr(broker, method)(params))
raise SystemExit('fault-injection boundary was not reached')
'''


class AdmissionCrashAcceptance(unittest.IsolatedAsyncioTestCase):
    async def check_crash(self, method):
        with tempfile.TemporaryDirectory(prefix="control-admission-crash-") as td:
            root = Path(td)
            params = {"cwd": td, "priority": "normal", "idempotency_key": "crash-key"}
            if method == "dispatch":
                params.update(target="crash-audit", prompt="fixture only")
            else:
                params.update(label="crash-audit", shell="bash", command="true")
            result = await asyncio.to_thread(
                subprocess.run,
                [sys.executable, "-c", CHILD, td, method, json.dumps(params)],
                cwd=Path(__file__).resolve().parent,
                capture_output=True, text=True, timeout=15,
            )
            self.assertEqual(result.returncode, 91, result.stderr)
            broker = TestBroker(root)
            try:
                admitted = await getattr(broker, method)(params)
                repeated = await getattr(broker, method)(params)
                self.assertEqual(admitted["id"], repeated["id"])
                count = broker.db.conn.execute("SELECT count(*) FROM tasks").fetchone()[0]
                self.assertEqual(count, 1, "crash/retry left duplicate durable tasks")
                keys = broker.db.conn.execute(
                    "SELECT count(*) FROM admission_idempotency"
                ).fetchone()[0]
                self.assertEqual(keys, 1)
                if method == "run_command":
                    starts = [c for c in broker.herdr.calls if c[0] == "execution.start"]
                    self.assertLessEqual(len(starts), 1)
            finally:
                for timer in broker.settle_timers.values():
                    timer.cancel()
                for timer in broker.native_turn_timers.values():
                    timer.cancel()
                broker.db.close()

    async def test_dispatch_abrupt_death_does_not_duplicate(self):
        await self.check_crash("dispatch")

    async def test_command_abrupt_death_does_not_duplicate(self):
        await self.check_crash("run_command")

    def test_mcp_admissions_accept_caller_idempotency_keys(self):
        for tool in TOOLS:
            if tool["name"] in {"dispatch", "run_command"}:
                with self.subTest(tool=tool["name"]):
                    self.assertIn("idempotency_key", tool["inputSchema"]["properties"])

    def test_mcp_forwards_key_unchanged_without_wait_options(self):
        for method in ("dispatch", "run_command"):
            with self.subTest(method=method), patch.object(
                control_mcp, "request", return_value={"state": "starting"}
            ) as request:
                control_mcp.call_tool(method, {"idempotency_key": "retry-key", "wait_seconds": 0})
                self.assertEqual(request.call_count, 1)
                self.assertEqual(request.call_args.args[1]["idempotency_key"], "retry-key")
                self.assertNotIn("wait_seconds", request.call_args.args[1])

    def test_cli_forwards_key_for_both_admissions(self):
        commands = [
            ["dispatch", "--target", "fixture", "--prompt", "fixture"],
            ["run", "--label", "fixture", "--command", "true"],
        ]
        for command in commands:
            argv = ["controlctl", *command, "--cwd", "/tmp", "--wait", "0", "--json",
                    "--idempotency-key", "retry-key"]
            with self.subTest(command=command[0]), patch.object(sys, "argv", argv), patch.object(
                control_client, "request", return_value={"state": "starting"}
            ) as request, contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(control_client.controlctl(), 0)
                self.assertEqual(request.call_count, 1)
                self.assertEqual(request.call_args.args[1]["idempotency_key"], "retry-key")


if __name__ == "__main__":
    unittest.main()
