import tempfile
import unittest
from pathlib import Path

from test_control_broker import TestBroker


class WaitingPaneRecovery(unittest.IsolatedAsyncioTestCase):
    async def broker(self, root):
        return TestBroker(root)

    def add_task(self, broker, root, task_id):
        return broker.db.add_task({"id": task_id, "target": "control", "cwd": str(root),
                                   "prompt": "preserve", "summary": "queued", "parent_id": None,
                                   "priority": "attention"})

    async def test_missing_pane_preserves_waiting_human_semantics_and_feature_claim(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw); broker = await self.broker(root)
            try:
                task = self.add_task(broker, root, "waiting-pane")
                broker.db.create_feature({"feature_id": "governance", "target": "control", "cwd": str(root),
                                          "title": "Governance", "scope": "end_to_end",
                                          "required_stages": ["implementation"], "children": {"implementation": task["id"]}})
                broker.db.conn.execute("UPDATE feature_stages SET claim_key=? WHERE feature_id=? AND stage=?", ("governance:implementation", "governance", "implementation")); broker.db.conn.commit()
                await broker.report({"task_id": task["id"], "state": "waiting_human", "summary": "Maintainer decision is required.", "question": "May the maintainer take this release?"})
                broker.db.update(task["id"], pane_id="missing-pane", agent_name="external-worker", herdr_state="working")
                await broker.reconcile()
                retained = broker.db.task(task["id"]); stage = next(s for s in broker.db.feature("governance")["stages"] if s["stage"] == "implementation")
                self.assertEqual(retained["state"], "waiting_human")
                self.assertEqual(retained["summary"], "Maintainer decision is required.")
                self.assertEqual(retained["question"], "May the maintainer take this release?")
                self.assertEqual(retained["herdr_state"], "missing")
                self.assertEqual(retained["session_state"], "dormant")
                self.assertEqual(stage["state"], "blocked")
                self.assertEqual(stage["claim_key"], "governance:implementation")
                self.assertIn("settled_wait_pane_absent_on_reconcile", [e["event"] for e in broker.db.transitions(task["id"])])
            finally:
                broker.db.close()

    async def test_missing_pane_marks_genuinely_inflight_work_unknown(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw); broker = await self.broker(root)
            try:
                task = self.add_task(broker, root, "inflight-pane")
                broker.db.update(task["id"], state="working", pane_id="missing-pane", agent_name="worker", herdr_state="working", summary="Model turn is in flight.")
                await broker.reconcile()
                retained = broker.db.task(task["id"])
                self.assertEqual(retained["state"], "unknown")
                self.assertEqual(retained["herdr_state"], "missing")
                self.assertEqual(retained["summary"], "Worker pane is absent after reconciliation.")
            finally:
                broker.db.close()


if __name__ == "__main__":
    unittest.main()
