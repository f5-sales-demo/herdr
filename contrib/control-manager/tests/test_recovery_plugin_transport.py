"""Native plugin transport must tolerate fragmentation and reject endless input."""
import importlib.util
import contextlib
import io
import socket
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('recovery_plugin', Path(__file__).parents[1] / 'herdr-control-recovery/recovery_plugin.py')
plugin = importlib.util.module_from_spec(spec)
spec.loader.exec_module(plugin)


class NativeTransportAcceptance(unittest.TestCase):
    def exchange(self, parts, *, delay=0, timeout=.5, max_bytes=1024):
        with tempfile.TemporaryDirectory(prefix='recovery-plugin-') as td:
            path = str(Path(td) / 'fixture.sock')
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(path); server.listen(1); server.settimeout(2)
            def serve():
                try:
                    conn, _ = server.accept()
                    with conn:
                        conn.recv(4096)
                        for part in parts:
                            conn.sendall(part)
                            if delay: time.sleep(delay)
                except (OSError, TimeoutError):
                    pass
            thread = threading.Thread(target=serve, daemon=True)
            thread.start()
            try:
                return plugin._exchange(path, {'method': 'status'}, timeout=timeout, max_bytes=max_bytes)
            finally:
                server.close(); thread.join(timeout=2)
                self.assertFalse(thread.is_alive())

    def test_fragmented_json_reply(self):
        self.assertEqual(self.exchange([b'{"ok":', b'true, "result":', b'{} }\n'], delay=.01),
                         {'ok': True, 'result': {}})

    def test_trickle_has_overall_deadline(self):
        started = time.monotonic()
        with self.assertRaises(TimeoutError):
            self.exchange([b' '] * 100, delay=.02, timeout=.1)
        self.assertLess(time.monotonic() - started, .5)

    def test_response_size_is_bounded(self):
        with self.assertRaisesRegex(RuntimeError, 'size limit'):
            self.exchange([b' ' * 200], max_bytes=100)

    def test_observation_mode_and_every_component_are_visible(self):
        with patch.object(plugin, '_config', return_value={}):
            screen = plugin.render({'components': [
                {'component': 'component-' + str(n), 'status': 'healthy', 'checked_at': 1}
                for n in range(8)]})
        self.assertIn('Automatic recovery: observation only', screen)
        for n in range(8): self.assertIn('component-' + str(n), screen)
        self.assertIn('1970-01-01 00:00:01Z', screen)

    def test_configured_manager_session_is_visible(self):
        with patch.object(plugin, '_config', return_value={}):
            screen = plugin.render({'affected_sessions':['Configured Manager']})
        self.assertIn('Affected sessions:\n- Configured Manager', screen)

    def test_recovery_progress_opens_before_waiting_for_response(self):
        events = []
        def stalled(method):
            events.append(method)
            raise TimeoutError('upstream recovery is still running')
        with patch.object(plugin.sys, 'argv', ['plugin', 'recover']), \
             patch.object(plugin, '_reopen_popup', side_effect=lambda notice: events.append('popup')), \
             patch.object(plugin, '_request', side_effect=stalled), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(plugin.main(), 1)
        self.assertEqual(events, ['popup', 'recover'])


if __name__ == '__main__': unittest.main()
