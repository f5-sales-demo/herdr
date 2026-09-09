"""Supervisor reconciliation must fail closed when authoritative state is absent."""
import tempfile
import unittest
import json
from pathlib import Path
from unittest.mock import AsyncMock,patch
from test_control_broker import TestBroker


class ReconcileEvidenceAcceptance(unittest.IsolatedAsyncioTestCase):
    async def test_failed_herdr_observation_cannot_report_reconciled(self):
        for method,params in [('reconcile',{}),('reconcile_topology',{
            'recovery_action_id':'recovery-'+'a'*32})]:
            with self.subTest(method=method),tempfile.TemporaryDirectory(prefix='control-reconcile-evidence-') as raw:
                broker=TestBroker(Path(raw))
                try:
                    with patch.object(broker.herdr,'request',AsyncMock(side_effect=ConnectionError('fixture Herdr unreachable'))):
                        try: result=await broker.handle(method,params)
                        except (RuntimeError,ConnectionError): pass
                        else:
                            self.assertIsNot(result.get('reconciled'),True,
                                             'unobserved state cannot prove reconciliation')
                finally:
                    broker.db.close()

    async def test_topology_missing_or_busy_never_reports_verified_reconcile(self):
        action='recovery-'+'b'*32
        for scenario in ('missing','busy'):
            with self.subTest(scenario=scenario),tempfile.TemporaryDirectory(prefix='control-topology-evidence-') as raw:
                broker=TestBroker(Path(raw))
                try:
                    workspace=await broker.herdr.request('workspace.create',{'cwd':raw,'label':'control'})
                    pane=workspace['root_pane']
                    broker.config_path.write_text(json.dumps({
                        'manager_thread_id':'01a07cad-d970-7393-82a4-14ae9a1c16ee', 'manager_cwd':raw,
                        'manager_workspace_id':'missing-workspace' if scenario=='missing' else workspace['workspace']['workspace_id'],
                        'manager_pane_id':'missing-pane' if scenario=='missing' else pane['pane_id'],
                        'app_server_remote':'unix://','profile':'control-manager','codex_binary':'/bin/true',
                        'supervisor_owns_recovery':True,
                    }))
                    if scenario=='busy': broker.manager_launching=True
                    with self.assertRaisesRegex(RuntimeError,'canonical manager topology is unverified'):
                        await broker.handle('reconcile_topology',{'recovery_action_id':action})
                    self.assertFalse(any(call[0]=='agent.prompt' for call in broker.herdr.calls))
                finally:
                    broker.db.close()


if __name__=='__main__': unittest.main()
