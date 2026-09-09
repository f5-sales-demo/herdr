from __future__ import annotations
import asyncio, json, tempfile, unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch
from control_supervisor import PROBE_TIMEOUT, RecoveryDB, Supervisor

class SupervisorTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_components_are_bounded_and_recovery_is_idempotent(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); config=root/'machine.json'; config.write_text(json.dumps({'herdr_socket':str(root/'gone'),'broker_socket':str(root/'gone2'),'app_server_socket':'','manager_thread_id':'canonical'}))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            first=await supervisor.check(); self.assertEqual(
                {x['component'] for x in first['components']},
                {'herdr','broker','app_server','manager_observer','remote_control','manager_thread','manager_turn','manager_native','provider_auth','provider_health','broker_tools'},
            )
            admitted=await supervisor.recover(); duplicate=await supervisor.recover()
            self.assertTrue(admitted['admitted']); self.assertFalse(duplicate['admitted'])
            supervisor.db.set_paused(True); self.assertNotEqual((await supervisor.recover())['state'],'waiting_user')
    def test_persistent_limit_cools_down(self):
        with tempfile.TemporaryDirectory() as raw:
            db=RecoveryDB(Path(raw)/'state.sqlite3')
            for number in range(3):
                action=db.claim('broker',f'reconnect_{number}','test'); db.finish(action,{})
            with self.assertRaisesRegex(RuntimeError,'cooldown'): db.claim('broker','another','test')
    def test_crash_boundary_is_recorded_without_replay(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); db=RecoveryDB(root/'state.sqlite3'); db.claim('broker','reconnect','test')
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',root/'missing.json')
            self.assertEqual(supervisor.interrupted_actions, 1)
            self.assertEqual(supervisor.db.status()['recent_outcomes'][0]['state'], 'uncertain')

    def test_second_instance_cannot_reconcile_first_instances_active_claim(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); first=Supervisor(root/'socket',root/'state.sqlite3',root/'config')
            action=first.db.claim('broker','restart','fixture')
            with self.assertRaisesRegex(RuntimeError,'already owns'):
                Supervisor(root/'socket',root/'state.sqlite3',root/'config')
            self.assertEqual(first.db.conn.execute('SELECT state FROM recovery_actions WHERE action_id=?',(action['action_id'],)).fetchone()[0],'claimed')
            import os
            for fd in first._lock_fds: os.close(fd)

    def test_recovery_caller_key_is_durable_across_database_reopen(self):
        with tempfile.TemporaryDirectory() as raw:
            path=Path(raw)/'state.sqlite3'
            first=RecoveryDB(path).claim('broker','restart','fixture',idempotency_key='same-call')
            second=RecoveryDB(path).claim('broker','restart','fixture',idempotency_key='same-call')
            self.assertTrue(first['admitted']); self.assertFalse(second['admitted'])
            self.assertEqual(first['action_id'],second['action_id'])

    def test_all_recovery_has_independent_persistent_component_restart_budgets(self):
        with tempfile.TemporaryDirectory() as raw:
            path=Path(raw)/'state.sqlite3'; db=RecoveryDB(path)
            for number in range(3):
                db.reserve_restart('broker',f'all-{number}')
            # Another component is unaffected by broker exhaustion.
            db.reserve_restart('app_server','all-3')
            reopened=RecoveryDB(path)
            with self.assertRaisesRegex(RuntimeError,'broker restart is in .*cooldown'):
                reopened.reserve_restart('broker','all-4')
            # Retrying the same durable action never consumes a second slot.
            reopened.reserve_restart('app_server','all-3')

    async def test_hung_probe_failures_are_parallel_and_bounded(self):
        # The real Unix-socket hang is exercised by the native harness.  This
        # unit test verifies supervisor fan-out without leaving peer handlers.
        import control_supervisor
        original = control_supervisor.unix_probe
        async def hung(path, payload=None):
            await asyncio.sleep(.05)
            return False, 'TimeoutError: injected hung probe'
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); config=root/'machine.json'; config.write_text(json.dumps({'herdr_socket':'a','broker_socket':'b','app_server_socket':'c','manager_thread_id':'canonical','manager_required_tools_verified':True}))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            control_supervisor.unix_probe=hung
            try:
                started=asyncio.get_running_loop().time(); result=await supervisor.check(); elapsed=asyncio.get_running_loop().time()-started
            finally:
                control_supervisor.unix_probe=original
            self.assertLess(elapsed,.15)
            self.assertTrue(any(item['component']=='broker' and item['status']=='degraded' for item in result['components']))

    async def test_threshold_admission_is_single_durable_action(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); config=root/'machine.json'; config.write_text(json.dumps({'manager_thread_id':'canonical','supervisor_mode':'isolated_active'}))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            for _ in range(3): check=await supervisor.check()
            first=await supervisor.automatic_recover(check); second=await supervisor.automatic_recover(check)
            self.assertTrue(first['admitted']); self.assertFalse(second['admitted'])
            self.assertEqual(len(supervisor.db.status()['recent_outcomes']),1)

    async def test_native_controls_when_broker_model_absent_and_input_saturated(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); config=root/'machine.json'; config.write_text('{}')
            supervisor=Supervisor(root/'supervisor.sock',root/'state.sqlite3',config); serving=asyncio.create_task(supervisor.serve())
            for _ in range(100):
                if (root/'supervisor.sock').exists(): break
                await asyncio.sleep(.01)
            reader,writer=await asyncio.open_unix_connection(str(root/'supervisor.sock'))
            writer.write(b'x'*70000+b'\n'); await writer.drain(); saturated=await asyncio.wait_for(reader.readline(),2)
            self.assertIn(b'error',saturated); writer.close(); await writer.wait_closed()
            async def rpc(method):
                reader,writer=await asyncio.open_unix_connection(str(root/'supervisor.sock')); writer.write(json.dumps({'method':method}).encode()+b'\n'); await writer.drain(); reply=json.loads(await reader.readline()); writer.close(); await writer.wait_closed(); return reply
            self.assertTrue((await rpc('pause'))['ok']); self.assertNotEqual((await rpc('recover'))['result']['state'],'waiting_user'); self.assertTrue((await rpc('resume'))['ok'])
            supervisor.stop.set(); await asyncio.wait_for(serving,2)

    async def test_manual_recover_succeeds_as_already_healthy_while_automatic_is_paused(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); config=root/'machine.json'; config.write_text(json.dumps({'manager_thread_name':'Configured Manager'}))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            supervisor.db.observe('broker',True,'responsive')
            supervisor.db.set_paused(True)
            with patch.object(supervisor,'check',AsyncMock(return_value={
                'state':'waiting_user','paused':True,
                'components':[{'component':'broker','status':'healthy','reason':'responsive'}],
                'authoritative':{},
            })):
                manual=await supervisor.recover('broker',idempotency_key='healthy-manual')
                automatic=await supervisor.recover('broker',idempotency_key='healthy-auto',automatic=True)
            self.assertEqual(manual['state'],'completed')
            self.assertEqual(manual['outcome']['note'],'requested components are already healthy')
            self.assertEqual(automatic['state'],'waiting_user')
            self.assertEqual(supervisor.status()['affected_sessions'],['Configured Manager'])

    def test_explicit_affected_sessions_override_configured_manager(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); config=root/'machine.json'; config.write_text(json.dumps({
                'manager_thread_name':'Configured Manager','affected_sessions':['Explicit Session']}))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            self.assertEqual(supervisor.status()['affected_sessions'],['Explicit Session'])

    async def test_isolated_component_recovery_runs_only_configured_actions(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw)
            config=root/'machine.json'; config.write_text(json.dumps({
                'supervisor_mode':'isolated_active',
                'recovery_commands': {'observer_reconnect':['/bin/true'],'restart_broker':['/bin/true']},
                'broker_socket':str(root/'missing-broker'),
            }))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            for _ in range(2): await supervisor.check()
            result=await supervisor.recover('broker',idempotency_key='isolated-recover')
            self.assertTrue(result['admitted'])
            self.assertEqual(result['outcome']['observer_reconnect']['state'],'completed')
            self.assertEqual(result['outcome']['restart:broker']['state'],'completed')
            duplicate=await supervisor.recover('broker',idempotency_key='isolated-recover')
            self.assertFalse(duplicate['admitted'])

    async def test_completed_restart_waits_for_delayed_authoritative_socket_readiness(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); config=root/'machine.json'; config.write_text(json.dumps({
                'supervisor_mode':'isolated_active','post_restart_readiness_timeout':1,
            }))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            unavailable={'state':'unavailable','components':[{'component':'broker','status':'unavailable','reason':'ConnectionRefusedError'}],'authoritative':{}}
            healthy={'state':'healthy','components':[{'component':'broker','status':'healthy','reason':'reachable and responsive'}],'authoritative':{}}
            checks=AsyncMock(side_effect=[unavailable,unavailable,unavailable,healthy])
            async def action(_cfg,_action,name):
                return {'state':'completed'} if name in {'observer_reconnect','restart_broker'} else {'state':'waiting_rollout'}
            with patch.object(supervisor,'check',checks), patch.object(supervisor,'_run_action',side_effect=action):
                result=await supervisor.recover('broker',idempotency_key='delayed-broker')
            self.assertEqual(result['state'],'completed')
            self.assertEqual(result['outcome']['verified_check']['components'][0]['status'],'healthy')
            self.assertEqual(checks.await_count,4)

    async def test_delayed_herdr_readiness_precedes_exact_topology_reconciliation(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); config=root/'machine.json'; config.write_text(json.dumps({
                'supervisor_mode':'isolated_active','post_restart_readiness_timeout':1,
            }))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            unavailable={'state':'unavailable','components':[{'component':'herdr','status':'unavailable','reason':'ConnectionRefusedError'}],'authoritative':{}}
            healthy={'state':'healthy','components':[{'component':'herdr','status':'healthy','reason':'reachable and responsive'}],'authoritative':{}}
            events=[]
            async def checked():
                events.append('check')
                return unavailable if events.count('check') < 4 else healthy
            async def action(_cfg,_action,name):
                events.append(name)
                return {'state':'completed'}
            with patch.object(supervisor,'check',side_effect=checked), patch.object(supervisor,'_run_action',side_effect=action):
                result=await supervisor.recover('herdr',idempotency_key='delayed-herdr')
            self.assertEqual(result['state'],'completed')
            self.assertEqual(result['outcome']['manager_topology_reconciliation']['state'],'completed')
            topology_index=events.index('reconcile_manager_topology')
            self.assertGreaterEqual(events[:topology_index].count('check'),4)
            self.assertNotIn('reconcile_manager_topology',events[:events.index('restart_herdr')])

    async def test_observation_mode_never_executes_component_restart(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); marker=root/'marker'
            config=root/'machine.json'; config.write_text(json.dumps({
                'supervisor_mode':'observation_only', 'broker_socket':str(root/'missing-broker'),
                'recovery_commands': {'observer_reconnect':['/usr/bin/touch',str(marker)],'restart_broker':['/usr/bin/touch',str(marker)]},
            }))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            result=await supervisor.recover('broker')
            self.assertEqual(result['outcome']['observer_reconnect']['state'],'waiting_rollout')
            self.assertFalse(marker.exists())

    async def test_appserver_probe_requires_exact_thread_and_live_tool_inventory(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); thread='thread-fixture'
            payload={'thread':{'id':thread,'status':'working'},'turn':{'id':'turn','status':'working'},'control_broker':{'inventory_verified':True,'runtime_status':'connected','tools':['completion_inbox','ack_completion','dispatch','run_command','continue_task','status','reply','request_stop']}}
            config=root/'machine.json'; config.write_text(json.dumps({
                'app_server_socket':'fixture-socket','manager_thread_id':thread,
                'appserver_probe_command':['/usr/bin/python3','-c',f'import json; print(json.dumps({payload!r}))'],
            }))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            result=await supervisor.check()
            by={x['component']:x for x in result['components']}
            self.assertEqual(by['app_server']['status'],'healthy')
            self.assertEqual(by['broker_tools']['status'],'healthy')

    async def test_transient_terminal_manager_recovers_after_reconcile_without_replay(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); thread='thread-fixture'
            payload={'thread':{'id':thread,'status':'failed'},'turn':{'id':'turn','status':'failed','error':'transient model transport failure'},'control_broker':{'runtime_status':'connected','tools':['completion_inbox','ack_completion','dispatch','run_command','continue_task','status','reply','request_stop']}}
            command=['/usr/bin/python3','-c',f'import json; print(json.dumps({payload!r}))']
            config=root/'machine.json'; config.write_text(json.dumps({
                'app_server_socket':'fixture','manager_thread_id':thread,'appserver_probe_command':command,
                'supervisor_mode':'isolated_active','recovery_commands':{'observer_reconnect':['/bin/true'],'reconcile_durable_work':['/bin/true'],'manager_continuation':['/bin/true']},
            }))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            for _ in range(2): await supervisor.check()
            import control_supervisor
            from unittest.mock import AsyncMock, patch
            real_runner=control_supervisor.bounded_process
            async def action_runner(argv, env, timeout=5):
                # Preserve authoritative probes. Only fake the resume helper;
                # a blanket runner patch would turn every probe into `{}`.
                if len(argv) >= 3 and argv[1].endswith('appserver_manager.py') and argv[2] == 'resume':
                    self.assertEqual(argv[-1],'turn')
                    return 0,'{}',''
                return await real_runner(argv,env,timeout)
            with patch('control_supervisor.broker_reconcile',AsyncMock(return_value=(True,'reconciled'))), \
                 patch('control_supervisor.bounded_process',side_effect=action_runner):
                result=await supervisor.recover('manager')
            manager=result['outcome']['manager_recovery']
            self.assertEqual(manager['reconcile']['state'],'completed')
            self.assertEqual(manager['continuation']['state'],'completed')
            self.assertTrue(manager['never_replayed_original_request'])

    def test_only_positive_transient_evidence_is_resumable(self):
        self.assertFalse(Supervisor._transient(None))
        self.assertFalse(Supervisor._transient("model invalid request"))
        self.assertFalse(Supervisor._transient({"code":"transport_error","retryable":False}))
        self.assertTrue(Supervisor._transient({"code":"transport_error","retryable":True}))
        self.assertTrue(Supervisor._transient("transient model transport failure"))

    async def test_guarded_live_uses_only_explicit_component_unit_adapter(self):
        from unittest.mock import AsyncMock, patch
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); config=root/'machine.json'; config.write_text(json.dumps({'supervisor_mode':'guarded_live','recovery_live_enabled':True,'component_units':{'broker':'fixture-broker.service'}}))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config); action={'action_id':'fixture'}
            with patch('control_supervisor.bounded_process',AsyncMock(return_value=(0,'ok',''))) as runner:
                result=await supervisor._run_action(supervisor.bindings(),action,'restart_broker')
            self.assertEqual(result['state'],'completed')
            self.assertEqual(runner.call_args.args[0],['systemctl','--user','restart','fixture-broker.service'])

    async def test_guarded_live_uses_supported_codex_daemon_adapter_without_unit(self):
        from unittest.mock import AsyncMock, patch
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); binary=root/'codex'; binary.write_text('#!/bin/sh\nexit 0\n'); binary.chmod(0o700)
            config=root/'machine.json'; config.write_text(json.dumps({
                'supervisor_mode':'guarded_live','recovery_live_enabled':True,
                'codex_binary':str(binary),'component_units':{'app_server':''}}))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            with patch('control_supervisor.bounded_process',AsyncMock(return_value=(0,'ok',''))) as runner:
                result=await supervisor._run_action(supervisor.bindings(),{'action_id':'daemon-fixture'},'restart_app_server')
            self.assertEqual(result['state'],'completed')
            self.assertEqual(runner.call_args.args[0],[str(binary),'app-server','daemon','restart'])

    async def test_guarded_live_requires_explicit_live_gate(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); config=root/'machine.json'; config.write_text(json.dumps({
                'supervisor_mode':'guarded_live','recovery_live_enabled':False,
                'component_units':{'broker':'fixture-broker.service'},
            }))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            result=await supervisor._run_action(supervisor.bindings(),{'action_id':'fixture'},'restart_broker')
            self.assertEqual(result['state'],'waiting_rollout')

    async def test_supervisor_topology_reconcile_is_bound_to_its_claimed_action(self):
        from unittest.mock import AsyncMock, patch
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); config=root/'machine.json'; config.write_text(json.dumps({'supervisor_mode':'isolated_active','broker_socket':'fixture'}))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            action=supervisor.db.claim('herdr','reconnect_observer','fixture')
            with patch('control_supervisor.broker_reconcile',AsyncMock(return_value=(True,'topology checked'))) as reconcile:
                result=await supervisor._run_action(supervisor.bindings(),action,'reconcile_manager_topology')
            self.assertEqual(result['state'],'completed')
            self.assertTrue(result['never_replayed_original_request'])
            self.assertEqual(reconcile.call_args.kwargs,{
                'topology':True,'action_id':action['action_id'],'claim_key':action['claim_key'],
                'owner_generation':action['owner_generation'],
            })

    async def test_supervisor_retries_transient_topology_inventory_race_with_same_claim(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); config=root/'machine.json'; config.write_text(json.dumps({
                'supervisor_mode':'isolated_active','broker_socket':'fixture',
                'topology_reconciliation_timeout':1,
            }))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            action=supervisor.db.claim('herdr','reconnect_observer','fixture')
            try:
                with patch('control_supervisor.broker_reconcile',AsyncMock(side_effect=[
                    (False,'cannot inspect configured canonical pane'),(True,'topology checked'),
                ])) as reconcile:
                    result=await supervisor._run_action(supervisor.bindings(),action,'reconcile_manager_topology')
                self.assertEqual(result['state'],'completed')
                self.assertEqual(reconcile.await_count,2)
                self.assertTrue(all(call.kwargs['action_id']==action['action_id'] for call in reconcile.await_args_list))
                self.assertTrue(all(call.kwargs['topology'] for call in reconcile.await_args_list))
            finally:
                supervisor.db.conn.close()

    async def test_topology_retry_deadline_bounds_stalled_rpc(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); config=root/'machine.json'
            config.write_text(json.dumps({'supervisor_mode':'isolated_active',
                'broker_socket':'fixture','topology_reconciliation_timeout':.05}))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            action=supervisor.db.claim('herdr','reconnect_observer','fixture')
            cancelled=asyncio.Event()
            async def stalled(*args,**kwargs):
                try: await asyncio.sleep(10)
                finally: cancelled.set()
            try:
                with patch('control_supervisor.broker_reconcile',stalled):
                    result=await asyncio.wait_for(supervisor._run_action(supervisor.bindings(),action,'reconcile_manager_topology'),.3)
                self.assertEqual(result['state'],'failed')
                self.assertIn('deadline',result['reason'])
                self.assertTrue(cancelled.is_set())
            finally: supervisor.db.conn.close()

    async def test_topology_retry_never_outlives_its_enclosing_action_lease(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); config=root/'machine.json'
            config.write_text(json.dumps({'supervisor_mode':'isolated_active','broker_socket':'fixture',
                'topology_reconciliation_timeout':1,'recovery_action_timeout':.03}))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            action=supervisor.db.claim('herdr','reconnect_observer','fixture')
            async def stalled(*args,**kwargs): await asyncio.sleep(10)
            try:
                with patch('control_supervisor.broker_reconcile',stalled):
                    result=await asyncio.wait_for(supervisor._run_action(supervisor.bindings(),action,'reconcile_manager_topology'),.2)
                self.assertEqual(result['state'],'failed')
                self.assertIn('deadline',result['reason'])
            finally: supervisor.db.conn.close()

    async def test_watchdog_heartbeats_during_async_work_but_stops_after_deadline(self):
        import os
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as raw, patch.dict(os.environ,{'WATCHDOG_USEC':'90000','WATCHDOG_PID':str(os.getpid())},clear=False), patch('control_supervisor.systemd_notify') as notify:
            root=Path(raw); supervisor=Supervisor(root/'socket',root/'state.sqlite3',root/'missing.json')
            token=supervisor._begin_work(.12); task=asyncio.create_task(supervisor._watchdog_heartbeat())
            await asyncio.sleep(.08); before=notify.call_count
            await asyncio.sleep(.20); after=notify.call_count
            await asyncio.sleep(.10); settled=notify.call_count
            supervisor._end_work(token); supervisor.stop.set(); await task
            self.assertGreater(before,0)
            self.assertEqual(settled,after)

    async def test_managed_observer_reconnects_and_drains_saturated_output(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw)
            code="import json,sys,time; print(json.dumps({'ready':True,'thread_id':'manager'}),flush=True); print(json.dumps({'observer_heartbeat':{'thread_id':'manager','timestamp':time.time(),'status':'idle','error':None}}),flush=True); sys.stderr.write('x'*200000); sys.stderr.flush(); time.sleep(30)"
            command=['/usr/bin/python3','-c',code]
            config=root/'machine.json'; config.write_text(json.dumps({'supervisor_mode':'isolated_active','observer_command':command,'manager_thread_id':'manager'}))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            first=await supervisor.check()
            self.assertIn('observer', {item['component'] for item in first['components']})
            action=supervisor.db.claim('observer','reconnect_observer','fixture')
            outcome=await supervisor._run_action(supervisor.bindings(),action,'observer_reconnect')
            self.assertEqual(outcome['state'],'completed')
            self.assertTrue(supervisor.observer_drains)
            supervisor.stop.set()
            if supervisor.observer_process and supervisor.observer_process.returncode is None:
                supervisor.observer_process.terminate(); await supervisor.observer_process.wait()
            for task in supervisor.observer_drains: task.cancel()

    async def test_observer_launch_uses_nondefault_machine_bindings_from_config(self):
        class Process:
            def __init__(self):
                self.returncode=None
                self.stdout=asyncio.StreamReader(); self.stderr=asyncio.StreamReader()
                self.stdout.feed_data(b'{"ready":true,"thread_id":"manager"}\n')
                self.stdout.feed_data(b'{"observer_heartbeat":{"thread_id":"manager","timestamp":1}}\n')
            async def wait(self):
                await asyncio.Future()
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); config=root/'machine.json'; config.write_text(json.dumps({
                'supervisor_mode':'isolated_active','observer_command':['observer-fixture'],'manager_thread_id':'manager',
                'manager_cwd':'/nondefault/manager','app_server_socket':'/nondefault/app.sock',
                'herdr_socket':'/nondefault/herdr.sock','broker_socket':'/nondefault/broker.sock',
                'codex_binary':'/nondefault/codex',
            }))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            process=Process()
            try:
                with patch('control_supervisor.asyncio.create_subprocess_exec',AsyncMock(return_value=process)) as spawn:
                    ok,_=await supervisor._observer_status(supervisor.bindings())
                self.assertTrue(ok)
                env=spawn.call_args.kwargs['env']
                self.assertEqual(env['CODEX_APP_SERVER_SOCKET'],'/nondefault/app.sock')
                self.assertEqual(env['CODEX_CONTROL_CONFIG_PATH'],str(config))
                self.assertEqual(env['CODEX_CONTROL_HERDR_SOCKET'],'/nondefault/herdr.sock')
                self.assertEqual(env['CONTROL_BROKER_SOCKET'],'/nondefault/broker.sock')
                self.assertEqual(env['CODEX_BINARY'],'/nondefault/codex')
                self.assertEqual(env['CODEX_CONTROL_MANAGER_CWD'],'/nondefault/manager')
            finally:
                for task in supervisor.observer_drains: task.cancel()
                await asyncio.gather(*supervisor.observer_drains,return_exceptions=True)
                supervisor.db.conn.close()

    async def test_guarded_live_observer_runs_and_status_reports_effective_mode(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); code="import json,time; print(json.dumps({'ready':True,'thread_id':'manager'}),flush=True); print(json.dumps({'observer_heartbeat':{'thread_id':'manager','timestamp':time.time(),'status':'idle','error':None}}),flush=True); time.sleep(30)"; command=['/usr/bin/python3','-c',code]
            config=root/'machine.json'; config.write_text(json.dumps({
                'supervisor_mode':'guarded_live','recovery_live_enabled':True,'observer_command':command,
                'manager_thread_id':'manager'}))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            ok,reason=await supervisor._observer_status(supervisor.bindings())
            self.assertTrue(ok,reason)
            status=supervisor.status()
            self.assertEqual(status['supervisor_mode'],'guarded_live')
            self.assertTrue(status['recovery_live_enabled'])
            self.assertEqual(status['effective_recovery_mode'],'guarded_live')
            if supervisor.observer_process and supervisor.observer_process.returncode is None:
                import os, signal
                os.killpg(supervisor.observer_process.pid,signal.SIGTERM)
                await supervisor.observer_process.wait()
            for task in supervisor.observer_drains: task.cancel()

    async def test_observer_command_resolves_current_authoritative_thread_at_launch(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); config=root/'machine.json'
            command=['/usr/bin/python3','-c',
                     "import json,sys,time; t=sys.argv[1]; print(json.dumps({'ready':True,'thread_id':t}),flush=True); print(json.dumps({'observer_heartbeat':{'thread_id':t,'timestamp':time.time(),'status':'idle','error':None}}),flush=True); time.sleep(30)",
                     '{manager_thread_id}']
            config.write_text(json.dumps({'supervisor_mode':'isolated_active','observer_command':command,'manager_thread_id':'current-manager'}))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            ok,reason=await supervisor._observer_status(supervisor.bindings())
            self.assertTrue(ok,reason)
            self.assertEqual(supervisor.observer_thread_id,'current-manager')
            if supervisor.observer_process and supervisor.observer_process.returncode is None:
                import os, signal
                os.killpg(supervisor.observer_process.pid,signal.SIGTERM); await supervisor.observer_process.wait()
            for task in supervisor.observer_drains: task.cancel()

    async def test_observer_heartbeat_becomes_stale_without_process_exit(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); code="import json,time; print(json.dumps({'ready':True,'thread_id':'manager'}),flush=True); print(json.dumps({'observer_heartbeat':{'thread_id':'manager','timestamp':time.time(),'status':'idle','error':None}}),flush=True); time.sleep(30)"
            config=root/'machine.json'; config.write_text(json.dumps({'supervisor_mode':'isolated_active','observer_command':['/usr/bin/python3','-c',code],'manager_thread_id':'manager','observer_heartbeat_timeout':.05}))
            supervisor=Supervisor(root/'socket',root/'state.sqlite3',config)
            ok,reason=await supervisor._observer_status(supervisor.bindings()); self.assertTrue(ok,reason)
            await asyncio.sleep(.08)
            ok,reason=await supervisor._observer_status(supervisor.bindings()); self.assertFalse(ok); self.assertIn('stale',reason)
            if supervisor.observer_process and supervisor.observer_process.returncode is None:
                import os, signal
                os.killpg(supervisor.observer_process.pid,signal.SIGTERM); await supervisor.observer_process.wait()
            for task in supervisor.observer_drains: task.cancel()
