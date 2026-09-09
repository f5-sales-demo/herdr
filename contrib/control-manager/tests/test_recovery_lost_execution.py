"""Positive lost-execution evidence must not leave work reported as running."""
import tempfile
import unittest
from pathlib import Path
from test_control_broker import TestBroker


class LostExecutionAcceptance(unittest.IsolatedAsyncioTestCase):
    async def test_lost_without_output_completion_is_durable_unknown_not_replayed(self):
        with tempfile.TemporaryDirectory(prefix='control-lost-execution-') as raw:
            broker=TestBroker(Path(raw))
            try:
                task=await broker.run_command({'label':'isolated uncertainty fixture',
                    'cwd':raw,'shell':'bash','command':'true','priority':'normal',
                    'idempotency_key':'lost-fixture'})
                execution=broker.herdr.executions[task['id']]
                execution.update(state='lost',output_complete=False,
                                 evidence_gap='fixture PTY exit evidence was lost')
                await broker._reconcile_native_commands()
                row=broker.db.task(task['id'])
                self.assertEqual(row['state'],'unknown')
                self.assertIn('evidence',row['summary'])
                self.assertIsNone(row['command_exit_status'])
                generation=row['run_generation']
                repeated=await broker.run_command({'label':'isolated uncertainty fixture',
                    'cwd':raw,'shell':'bash','command':'true','priority':'normal',
                    'idempotency_key':'lost-fixture'})
                self.assertEqual(repeated['id'],task['id'])
                self.assertEqual(broker.db.task(task['id'])['run_generation'],generation)
                self.assertEqual(len(broker.herdr.executions),1)
                self.assertEqual(broker.db.pending_completions(),[])
                broker.db.conn.commit()
                import sqlite3
                connection=sqlite3.connect(broker.db.conn.execute('PRAGMA database_list').fetchone()[2])
                try: self.assertEqual(connection.execute('SELECT state FROM tasks WHERE id=?',(task['id'],)).fetchone()[0],'unknown')
                finally: connection.close()
            finally:
                for timer in broker.settle_timers.values(): timer.cancel()
                for timer in broker.native_turn_timers.values(): timer.cancel()
                broker.db.close()


if __name__=='__main__': unittest.main()
