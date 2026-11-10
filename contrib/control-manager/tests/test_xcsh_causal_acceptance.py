"""Causal receipt validation units; these fixtures are not installed UAT."""
import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parents[1] / "runtime"))
from xcsh_installed_uat import PreflightError, require_causal_action

class CausalAcceptanceTests(unittest.TestCase):
    def test_reply_loss_requires_two_matching_actual_acknowledgements(self):
        task = {"id": "e1", "agent_session_id": "0123abcd4567ef89"}
        effect = {"execution_id": "e1", "producer_session_id": task["agent_session_id"], "producer_pid": 123,
                  "before_revision": 2, "action": "producer_report_reply_loss", "redelivered": True,
                  "response_discarded": True, "payload_sha256": "a" * 64, "replay_payload_sha256": "a" * 64,
                  "acknowledgements": [{"admitted": True, "revision": 3}, {"admitted": False, "revision": 3}]}
        receipt = {"kind": "reconnect_replay", "producer_receipt_version": 1, "effect": effect}
        require_causal_action(receipt, kind="reconnect_replay", task=task, records_before=[{"revision": 2}])
        effect["acknowledgements"][1]["revision"] = 4
        with self.assertRaisesRegex(PreflightError, "acknowledgements"):
            require_causal_action(receipt, kind="reconnect_replay", task=task, records_before=[{"revision": 2}])

    def test_controller_exit_assertion_cannot_replace_the_producer_shutdown_receipt(self):
        task = {"id": "e1", "agent_session_id": "0123abcd4567ef89"}
        receipt = {"kind": "restart_loss", "producer_receipt_version": 1, "effect": {
            "execution_id": "e1", "producer_session_id": task["agent_session_id"], "producer_pid": 123,
            "before_revision": 2, "action": "producer_process_cutpoint", "process_exited": True,
        }}
        with self.assertRaisesRegex(PreflightError, "genuine shutdown"):
            require_causal_action(receipt, kind="restart_loss", task=task, records_before=[{"revision": 2}])

if __name__ == "__main__": unittest.main()
