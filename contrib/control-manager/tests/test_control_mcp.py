from __future__ import annotations

import unittest
from unittest.mock import patch

import control_mcp


class ControlMcpTests(unittest.TestCase):
    def test_continue_defaults_to_nonblocking(self):
        calls: list[tuple[str, dict]] = []

        def fake_request(method: str, params: dict):
            calls.append((method, params))
            return {"id": "ctl-test", "state": "working"}

        with patch.object(control_mcp, "request", side_effect=fake_request):
            result = control_mcp.call_tool(
                "continue_task", {"task_id": "ctl-test", "text": "follow up"}
            )

        self.assertEqual(result["state"], "working")
        self.assertEqual(calls, [("continue_task", {"task_id": "ctl-test", "text": "follow up"})])


if __name__ == "__main__":
    unittest.main()
