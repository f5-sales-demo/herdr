"""Rollback must not start a competing recovery owner after failed quiescence."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import control_recovery_rollout as rollout


class RollbackSafetyAcceptance(unittest.TestCase):
    def test_failed_supervisor_stop_does_not_enable_timer_or_change_config(self):
        with tempfile.TemporaryDirectory(prefix='control-rollback-safety-') as raw:
            root=Path(raw); config=root/'machine.json'; receipt=root/'receipt.json'
            live={'supervisor_mode':'guarded_live','recovery_live_enabled':True,
                  'supervisor_owns_recovery':True}
            config.write_text(json.dumps(live))
            rollout.write_receipt(receipt,{'version':2,'supervisor_unit':'owned-supervisor.service',
                'competing_timer':'owned-timer.timer','config_path':str(config),
                'prior':{'runtime_config':{'supervisor_mode':'observation_only',
                         'recovery_live_enabled':False,'supervisor_owns_recovery':False},
                         'competing_timer_enabled':True,'competing_timer_active':True,
                         'supervisor_active':True}})
            calls=[]
            def fail_stop(*args):
                calls.append(args)
                return {'returncode':1 if args==('stop','owned-supervisor.service') else 0,
                        'argv':list(args),'stdout':'','stderr':'fixture failure'}
            with patch.object(rollout,'systemctl',side_effect=fail_stop):
                with self.assertRaises(RuntimeError): rollout.rollback(receipt)
            self.assertEqual(calls,[('stop','owned-supervisor.service')],
                             'rollback started another owner after failed supervisor stop')
            self.assertEqual(json.loads(config.read_text()),live)


if __name__=='__main__': unittest.main()
