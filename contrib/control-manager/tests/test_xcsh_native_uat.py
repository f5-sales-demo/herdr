import unittest

from xcsh_native_uat import run_catalog


class XcshNativeUatFixtureTests(unittest.TestCase):
    def test_catalog_exercises_all_semantic_boundaries_without_acceptance(self):
        receipt = run_catalog()
        self.assertTrue(receipt["pass"])
        self.assertFalse(receipt["accepted"])
        self.assertEqual(receipt["fixture_mode"], "synthetic_process_and_journal")
        results = {case["id"]: case for case in receipt["cases"]}
        self.assertEqual(set(results), {"success", "failure", "waiting_input", "cancel", "native_continuation", "reconnect_replay_dedup", "generation_supersession", "cleanup_boundary", "restart_loss_boundary"})
        self.assertEqual(results["success"]["final_state"], "completed")
        self.assertEqual(results["waiting_input"]["final_state"], "waiting_human")
        self.assertEqual(results["restart_loss_boundary"]["final_state"], "unknown")
        self.assertIn("duplicate_replay_ignored", results["reconnect_replay_dedup"]["assertions"])
        self.assertIn("stale_or_conflicting_generation_rejected", results["generation_supersession"]["assertions"])


if __name__ == "__main__":
    unittest.main()
