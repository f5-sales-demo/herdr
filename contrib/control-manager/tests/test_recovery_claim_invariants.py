"""Manager acceptance checks for durable global recovery serialization/cooldown."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from control_supervisor import RecoveryDB


class RecoveryClaimAcceptance(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="recovery-claim-acceptance-")
        self.path = Path(self.tmp.name) / "recovery.db"
        self.db = RecoveryDB(self.path)

    def tearDown(self):
        self.db.conn.close()
        self.tmp.cleanup()

    def test_different_components_share_one_active_recovery(self):
        other = RecoveryDB(self.path)
        try:
            first = self.db.claim("broker", "restart", "verified failure", idempotency_key="broker-case")
            second = other.claim("herdr", "restart", "verified failure", idempotency_key="herdr-case")
            self.assertTrue(first["admitted"])
            self.assertFalse(second["admitted"], "two active recovery actions admitted across components")
            count = self.db.conn.execute(
                "SELECT count(*) FROM recovery_actions WHERE state IN ('claimed','recovering')"
            ).fetchone()[0]
            self.assertEqual(count, 1)
        finally:
            other.conn.close()

    def test_cooldown_survives_reopen_and_rolling_window_expiry(self):
        for index, now in enumerate((1000, 1011, 1022)):
            with patch("control_supervisor.utc", return_value=now):
                action = self.db.claim("broker", "restart", "verified failure", idempotency_key=f"attempt-{index}")
                self.db.finish(action, {"state": "failed"}, "failed")
        with patch("control_supervisor.utc", return_value=1033):
            with self.assertRaises(RuntimeError):
                self.db.claim("broker", "restart", "verified failure", idempotency_key="exhausted")
        self.db.conn.close()
        self.db = RecoveryDB(self.path)
        # First attempt has aged out of the rolling 900-second window,
        # but the exhaustion-triggered cooldown has not elapsed.
        with patch("control_supervisor.utc", return_value=1910):
            with self.assertRaises(RuntimeError, msg="stored cooldown was ignored after reopen"):
                self.db.claim("broker", "restart", "verified failure", idempotency_key="too-early")


if __name__ == "__main__":
    unittest.main()
