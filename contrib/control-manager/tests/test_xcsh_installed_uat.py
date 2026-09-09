import hashlib
import json
import unittest
from pathlib import Path

from xcsh_installed_uat import CATALOG, PreflightError, preflight, validate_journal


ROOT = Path(__file__).parent


def manifest():
    digest = "a" * 64
    return {"schema_version": 1, "isolation": "dedicated_disposable_runtime",
            "artifacts": {name: {"release_version": "1.0.0", "artifact_uri": f"file:///isolated/{name}", "sha256": digest}
                          for name in ("xcsh", "herdr", "manager")},
            "runtime": {"broker_socket": "/isolated/control.sock", "herdr_socket": "/isolated/herdr.sock"},
            "required_capabilities": ["native_xcsh_admit", "agent_turn_journal"]}


class InstalledPromptUatTests(unittest.TestCase):
    def test_preflight_binds_all_artifacts_and_all_required_prompt_boundaries(self):
        receipt = preflight(manifest(), json.loads(CATALOG.read_text()))
        self.assertEqual(receipt["preflight"], "passed")
        self.assertEqual(receipt["live_execution"], "not_started")
        self.assertEqual(len(receipt["scenario_ids"]), 9)
        self.assertIn("native_xcsh_admit", receipt["required_capabilities"])

    def test_preflight_rejects_nonisolated_or_incomplete_catalog(self):
        invalid = manifest(); invalid["isolation"] = "shared_runtime"
        with self.assertRaisesRegex(PreflightError, "dedicated disposable"):
            preflight(invalid, json.loads(CATALOG.read_text()))
        invalid = manifest(); invalid["required_capabilities"] = ["agent_turn_journal"]
        with self.assertRaisesRegex(PreflightError, "native_xcsh_admit"):
            preflight(invalid, json.loads(CATALOG.read_text()))

    def test_oracle_requires_real_provenance_not_a_sentinel_substring(self):
        case = next(item for item in json.loads(CATALOG.read_text())["scenarios"] if item["id"] == "success")
        task = {"id": "task-1", "pane_id": "pane-1", "agent_session_id": "session-1"}
        result = "sentinel is irrelevant"
        reports = [
            {"revision": 1, "report": {"execution_id": "task-1", "pane_id": "pane-1", "session_id": "session-1", "state": "starting"}},
            {"revision": 2, "report": {"execution_id": "task-1", "pane_id": "pane-1", "session_id": "session-1", "state": "working"}},
            {"revision": 3, "report": {"execution_id": "task-1", "pane_id": "pane-1", "session_id": "session-1", "state": "completed", "result": result, "result_digest": hashlib.sha256(result.encode()).hexdigest()}},
        ]
        validate_journal(case, reports, task, {"task_id": "task-1", "stage": "consumed"})
        reports[-1]["report"]["pane_id"] = "wrong-pane"
        with self.assertRaisesRegex(AssertionError, "provenance"):
            validate_journal(case, reports, task, {"task_id": "task-1", "stage": "consumed"})
