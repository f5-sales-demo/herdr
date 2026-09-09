from __future__ import annotations
import json, subprocess, tempfile, unittest
from pathlib import Path
from unittest.mock import patch
import control_recovery_rollout as rollout

def result(*argv, code=0, stdout=""):
    return {"argv":["systemctl","--user",*argv],"returncode":code,"stdout":stdout,"stderr":""}

class RolloutTests(unittest.TestCase):
    def test_systemctl_timeout_is_bounded_failure(self):
        with patch("control_recovery_rollout.subprocess.run",side_effect=subprocess.TimeoutExpired(["systemctl"],15)):
            self.assertEqual(rollout.systemctl("start","x.service")["returncode"],124)

    def test_observation_start_failure_is_not_reported_applied(self):
        with patch("control_recovery_rollout.systemctl",return_value=result("start","s",code=1)):
            with self.assertRaisesRegex(RuntimeError,"failed to start"):
                rollout.apply_observation({"supervisor_mode":"observation_only"},"s")

    def test_failed_handoff_restores_distinct_enabled_and_active_states(self):
        with tempfile.TemporaryDirectory() as raw:
            receipt=Path(raw)/"receipt.json"; calls=[]
            def fake(*args):
                calls.append(args)
                if args[0] == "is-enabled": return result(*args,code=0,stdout="enabled")
                if args[0] == "is-active": return result(*args,code=0,stdout="active") if args[1] == "timer" else result(*args,code=3,stdout="inactive")
                if args[:2] == ("restart","supervisor"): return result(*args,code=1)
                return result(*args)
            config=Path(raw)/"machine.json"
            herdr = Path(raw) / "herdr.toml"; herdr.write_text("[ui]\ntheme = 'dark'\n")
            cfg={"supervisor_mode":"observation_only","recovery_live_enabled":False,"supervisor_owns_recovery":False,"herdr_config_path":str(herdr),
                 "guarded_live_desired":{"supervisor_mode":"guarded_live","recovery_live_enabled":True,"supervisor_owns_recovery":True}}
            config.write_text(json.dumps(cfg))
            with patch("control_recovery_rollout.systemctl",side_effect=fake):
                with self.assertRaisesRegex(RuntimeError,"prior unit state was restored"):
                    rollout.apply_guarded(cfg,"supervisor","timer",receipt,config)
            saved=json.loads(receipt.read_text())
            self.assertEqual(saved["state"],"rolled_back")
            restored=json.loads(config.read_text())
            self.assertEqual(restored["supervisor_mode"],"observation_only")
            self.assertFalse(restored["supervisor_owns_recovery"])
            self.assertEqual(herdr.read_text(), "[ui]\ntheme = 'dark'\n")
            self.assertIn(("enable","timer"),calls)
            self.assertIn(("start","timer"),calls)
            self.assertIn(("stop","supervisor"),calls)

    def test_rollback_failure_is_reported(self):
        with tempfile.TemporaryDirectory() as raw:
            receipt=Path(raw)/"receipt.json"
            rollout.write_receipt(receipt,{"version":2,"supervisor_unit":"s","competing_timer":"t","prior":{}})
            with patch("control_recovery_rollout.systemctl",return_value=result("x",code=1)):
                with self.assertRaisesRegex(RuntimeError,"every prior unit state"):
                    rollout.rollback(receipt)

    def test_unknown_unit_state_fails_closed(self):
        with patch("control_recovery_rollout.systemctl",return_value=result("is-active","x",code=4,stdout="unknown")):
            with self.assertRaisesRegex(RuntimeError,"cannot determine"):
                rollout.active("x")

    def test_herdr_handoff_retains_other_settings_and_exactly_restores_absent_file(self):
        with tempfile.TemporaryDirectory() as raw:
            path=Path(raw)/"nested"/"herdr.toml"
            prior=rollout.set_resume_agents_on_restore(path,False)
            self.assertIn("[session]",path.read_text())
            self.assertIn("resume_agents_on_restore = false",path.read_text())
            rollout.restore_herdr_config({"herdr_config":prior})
            self.assertFalse(path.exists())

    def test_herdr_session_key_is_inserted_before_following_table_and_preserves_mode(self):
        with tempfile.TemporaryDirectory() as raw:
            path=Path(raw)/"herdr.toml"; original="[session]\nfoo = 'x'\n[ui]\ntheme = 'dark'\n"
            path.write_text(original); path.chmod(0o640); prior=rollout.capture_herdr_config(path)
            rollout.set_resume_agents_on_restore(path,False,prior)
            changed=path.read_text()
            self.assertEqual(changed,"[session]\nfoo = 'x'\nresume_agents_on_restore = false\n[ui]\ntheme = 'dark'\n")
            self.assertEqual(path.stat().st_mode & 0o777,0o640)
            rollout.restore_herdr_config({"herdr_config":prior})
            self.assertEqual(path.read_text(),original)

if __name__ == "__main__": unittest.main()
