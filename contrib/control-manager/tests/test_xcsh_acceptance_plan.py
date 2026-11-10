"""Unit contracts for the private producer plan, not live UAT evidence."""
import hashlib
import json
import sys
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parents[1] / "runtime"))
from xcsh_isolated_uat_controller import ControllerError, DisposableHerdrController, IsolatedController

class AcceptancePlanTests(unittest.TestCase):
    def test_descriptor_publication_precedes_the_controller_effect_claim(self):
        """Synthetic readiness contract; the effect stub is not producer evidence."""
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            plan = DisposableHerdrController.create_acceptance_plan(root / "private")
            binary = root / "herdr"
            binary.write_bytes(b"fixture")
            controller = DisposableHerdrController(root / "actions.sqlite", {
                "session_id": "xcsh-uat-fixture", "service_id": "xcsh-uat-fixture", "workspace_id": "w1", "pane_ids": ["p1"], "acceptance_plan": str(plan),
            }, "token", binary)
            IsolatedController.register_execution(controller, {"id": "e1", "workspace_id": "w1", "tab_id": "t1", "pane_id": "p1"})
            controller.conn.execute("INSERT INTO uat_native_bindings VALUES(?,?)", ("e1", json.dumps({"session_id": "0123abcd4567ef89", "generation": 0})))
            controller.verify_live_ownership = lambda: None
            descriptor = plan.parent / "0123abcd4567ef89.0.123.json"
            def publish(_delay):
                self.assertEqual(controller.conn.execute("SELECT count(*) FROM uat_actions").fetchone()[0], 0)
                descriptor.write_text("{}"); descriptor.chmod(0o600)
            def effect(kind, pane, key):
                self.assertTrue(descriptor.is_file())
                self.assertEqual(controller.conn.execute("SELECT state FROM uat_actions WHERE key=?", (key,)).fetchone()[0], "claimed")
                return {"version": 1, "effect": {"synthetic_unit_stub": True}}
            controller.producer_action = effect
            try:
                with patch("xcsh_isolated_uat_controller.time.sleep", side_effect=publish):
                    receipt = controller.real_action("reconnect_replay", "p1", "action-key", "token")
                self.assertEqual(receipt["kind"], "reconnect_replay")
            finally: controller.close()

    def test_approval_binds_measured_session_and_executable(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            plan = DisposableHerdrController.create_acceptance_plan(root / "private")
            binary = root / "xcsh"
            binary.write_bytes(b"measured producer")
            controller = object.__new__(DisposableHerdrController)
            controller.ownership = {"acceptance_plan": str(plan)}
            receipt = {"session_id": "0123abcd4567ef89", "xcsh_executable": str(binary),
                       "xcsh_executable_sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
            controller.approve_producer_session(receipt, ["reconnect_replay", "release"])
            approved = json.loads(plan.read_text())["approvals"]
            self.assertEqual(len(approved), 1)
            self.assertEqual(approved[0]["session_id"], receipt["session_id"])
            self.assertEqual(plan.stat().st_mode & 0o777, 0o600)
            binary.write_bytes(b"changed")
            with self.assertRaisesRegex(ControllerError, "measurement"):
                controller.approve_producer_session(receipt, ["release"])

    def test_public_plan_and_unapproved_actions_are_refused(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            plan = DisposableHerdrController.create_acceptance_plan(root / "private")
            plan.chmod(0o644)
            with self.assertRaisesRegex(ControllerError, "owner-private"):
                DisposableHerdrController.read_acceptance_plan(plan)

if __name__ == "__main__":
    unittest.main()
