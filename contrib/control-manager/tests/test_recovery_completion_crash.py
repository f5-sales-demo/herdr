"""Completion durability at abrupt process-death boundaries, using private DBs."""
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from control_broker import StateDB


CHILD = r'''
import os, sys
from pathlib import Path
from control_broker import StateDB
db = StateDB(Path(sys.argv[1]))
mode = sys.argv[2]
def crash(sql):
    sql = sql.lstrip().upper()
    if mode == 'terminal' and sql.startswith('INSERT INTO COMPLETION_OUTBOX'):
        os._exit(92)
    if mode == 'ack' and sql.startswith('UPDATE COMPLETION_OUTBOX'):
        os._exit(92)
db.conn.set_trace_callback(crash)
if mode == 'terminal':
    db.update('fixture', state='completed', summary='durable answer', finished_at=1)
else:
    event = db.task('fixture')['terminal_event_id']
    db.ack_completion(event, 'consumed', 'manager_mcp', 'fixture-receipt', None)
raise SystemExit('crash boundary not reached')
'''


class CompletionCrashAcceptance(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='control-completion-crash-')
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'state.sqlite3'
        db = StateDB(self.path)
        db.add_task({'id': 'fixture', 'target': 'fixture', 'cwd': self.temp.name,
                     'prompt': 'isolated fixture', 'summary': 'queued',
                     'parent_id': None, 'priority': 'routine'})
        db.close()

    def crash(self, mode):
        result = subprocess.run([sys.executable, '-c', CHILD, str(self.path), mode],
                                cwd=Path(__file__).resolve().parent,
                                capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 92, result.stderr)

    def test_terminal_and_outbox_commit_together_after_hard_death(self):
        self.crash('terminal')
        db = StateDB(self.path)
        try:
            self.assertEqual(db.task('fixture')['state'], 'queued')
            self.assertEqual(db.pending_completions(include_consumed=True), [])
            self.assertEqual(db.conn.execute('SELECT count(*) FROM event_journal').fetchone()[0], 0)
            done = db.update('fixture', state='completed', summary='durable answer', finished_at=1)
            db.update('fixture', state='completed', summary='durable answer', finished_at=1)
            events = db.pending_completions(include_consumed=True)
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]['event_id'], done['terminal_event_id'])
            self.assertEqual(events[0]['payload']['summary'], 'durable answer')
        finally:
            db.close()

    def test_interrupted_ack_replays_same_event_and_preserves_delivery_boundary(self):
        db = StateDB(self.path)
        done = db.update('fixture', state='completed', summary='durable answer', finished_at=1)
        event_id = done['terminal_event_id']
        db.deliver_inbox()
        db.close()
        self.crash('ack')
        db = StateDB(self.path)
        try:
            events = db.pending_completions()
            self.assertEqual([x['event_id'] for x in events], [event_id])
            self.assertEqual(events[0]['delivery_state'], 'delivered')
            self.assertEqual(db.conn.execute("SELECT count(*) FROM completion_acks WHERE stage='consumed'").fetchone()[0], 0)
            for _ in range(2):
                db.ack_completion(event_id, 'consumed', 'manager_mcp', 'fixture-receipt', None)
            row = db.conn.execute('SELECT * FROM completion_outbox').fetchone()
            self.assertIsNotNone(row['consumed_at'])
            self.assertIsNone(row['response_produced_at'])
            self.assertIsNone(row['client_delivered_at'])
            self.assertEqual(db.conn.execute("SELECT count(*) FROM completion_acks WHERE stage='consumed'").fetchone()[0], 1)
        finally:
            db.close()


if __name__ == '__main__':
    unittest.main()
