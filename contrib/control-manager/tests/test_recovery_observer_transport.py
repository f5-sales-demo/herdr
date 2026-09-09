import base64
import hashlib
import json
import socket
import struct
import threading
import time
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch
from pathlib import Path

import appserver_manager as manager


def data_frame(payload, *, final=True, opcode=1):
    lead = (0x80 if final else 0) | opcode
    if len(payload) < 126:
        return bytes((lead, len(payload))) + payload
    return bytes((lead, 126)) + struct.pack("!H", len(payload)) + payload


def frame(value, *, final=True, opcode=1):
    return data_frame(json.dumps(value, separators=(",", ":")).encode(), final=final, opcode=opcode)


def read_client_frame(peer):
    first, second = recv_exact(peer, 2)
    size = second & 0x7f
    if size == 126:
        size = struct.unpack("!H", recv_exact(peer, 2))[0]
    elif size == 127:
        size = struct.unpack("!Q", recv_exact(peer, 8))[0]
    mask = recv_exact(peer, 4)
    payload = recv_exact(peer, size)
    return bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))


def recv_exact(peer, size):
    result = bytearray()
    while len(result) < size:
        part = peer.recv(size - len(result))
        if not part:
            raise EOFError
        result.extend(part)
    return bytes(result)


class UnixPeer:
    def __init__(self, path, handler):
        self.listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.listener.bind(str(path))
        self.listener.listen(1)
        self.error = None

        def run():
            try:
                connection, _ = self.listener.accept()
                with connection:
                    handler(connection)
            except BaseException as exc:
                self.error = exc
            finally:
                self.listener.close()

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()

    def join(self):
        self.thread.join(1)
        if self.thread.is_alive():
            raise AssertionError("fake Unix peer exceeded outer fixture deadline")
        if self.error:
            raise self.error


class ObserverTransportTests(unittest.TestCase):
    def test_split_handshake_and_coalesced_first_frame(self):
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            path = f"{directory}/app.sock"

            def peer(connection):
                request = bytearray()
                while b"\r\n\r\n" not in request:
                    request.extend(connection.recv(1024))
                key = next(line.split(b":", 1)[1].strip() for line in request.split(b"\r\n")
                           if line.lower().startswith(b"sec-websocket-key:"))
                accept = base64.b64encode(hashlib.sha1(
                    key + b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest())
                response_payload = json.dumps({"id": 1, "result": {"ok": True}}, separators=(",", ":")).encode()
                response = data_frame(response_payload[:9], final=False) + data_frame(
                    response_payload[9:], opcode=0)
                header = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + b"\r\n\r\n"
                connection.sendall(header[:19])
                connection.sendall(header[19:] + response[:3])
                connection.sendall(response[3:])
                init = json.loads(read_client_frame(connection))
                assert init["id"] == 1
                json.loads(read_client_frame(connection))  # initialized

            fake = UnixPeer(path, peer)
            with patch.object(manager, "APP_SERVER_SOCKET", path):
                server = manager.AppServer(connect_timeout=.5, rpc_timeout=.5)
                server.socket.close()
            fake.join()

    def test_handshake_trickle_cannot_extend_setup_deadline(self):
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            path = f"{directory}/app.sock"

            def peer(connection):
                connection.recv(4096)
                for _ in range(20):
                    try:
                        connection.sendall(b"x")
                        time.sleep(.025)
                    except OSError:
                        return

            fake = UnixPeer(path, peer)
            started = time.monotonic()
            with patch.object(manager, "APP_SERVER_SOCKET", path):
                with self.assertRaises((RuntimeError, socket.timeout)):
                    manager.AppServer(connect_timeout=.12, rpc_timeout=.12)
            self.assertLess(time.monotonic() - started, .3)
            fake.join()

    def test_failed_initialize_closes_socket_after_eof(self):
        import tempfile
        saw_eof = threading.Event()
        with tempfile.TemporaryDirectory() as directory:
            path = f"{directory}/app.sock"

            def peer(connection):
                while b"\r\n\r\n" not in connection.recv(4096):
                    pass
                # Validity is irrelevant: EOF must fail and the client must close.
                connection.shutdown(socket.SHUT_WR)
                connection.settimeout(.5)
                if connection.recv(1) == b"":
                    saw_eof.set()

            fake = UnixPeer(path, peer)
            with patch.object(manager, "APP_SERVER_SOCKET", path):
                with self.assertRaises(RuntimeError):
                    manager.AppServer(connect_timeout=.2, rpc_timeout=.2)
            fake.join()
            self.assertTrue(saw_eof.wait(.2))

    def connected_server(self, timeout=.15):
        client, peer = socket.socketpair()
        server = manager.AppServer.__new__(manager.AppServer)
        server.socket = client
        server._receive_buffer = bytearray()
        server.notifications = manager.deque()
        server._notification_bytes = 0
        server.request_id = 0
        server.rpc_timeout = timeout
        return server, peer

    def test_rpc_total_deadline_survives_notification_trickle(self):
        server, peer = self.connected_server(.14)

        def trickle():
            read_client_frame(peer)
            for index in range(20):
                try:
                    peer.sendall(frame({"method": "tick", "params": {"n": index}}))
                    time.sleep(.025)
                except OSError:
                    return

        worker = threading.Thread(target=trickle, daemon=True); worker.start()
        started = time.monotonic()
        with self.assertRaisesRegex(RuntimeError, "timed out"):
            server.request("stalled")
        self.assertLess(time.monotonic() - started, .8)
        server.socket.close(); peer.close(); worker.join(.5)

    def test_saturated_outbound_socket_has_bounded_deadline(self):
        server, peer = self.connected_server(.12)
        server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        server.MAX_MESSAGE_BYTES = 8 * 1024 * 1024
        started = time.monotonic()
        with self.assertRaisesRegex(RuntimeError, "timed out sending"):
            server.request("flood", {"data": "x" * (4 * 1024 * 1024)})
        self.assertLess(time.monotonic() - started, .8)
        server.socket.close(); peer.close()

    def test_frame_and_notification_memory_are_bounded(self):
        server, peer = self.connected_server(.4)
        peer.sendall(bytes((0x81, 127)) + struct.pack("!Q", server.MAX_FRAME_BYTES + 1))
        with self.assertRaisesRegex(RuntimeError, "frame is too large"):
            server._receive_json(.2)
        server.socket.close(); peer.close()

        server, peer = self.connected_server(.5)
        server.MAX_NOTIFICATIONS = 4
        server.MAX_NOTIFICATION_BYTES = 500

        def flood():
            request = json.loads(read_client_frame(peer))
            for index in range(40):
                peer.sendall(frame({"method": "noise", "params": {"n": index, "v": "x" * 30}}))
            peer.sendall(frame({"id": request["id"], "result": "done"}))

        worker = threading.Thread(target=flood, daemon=True); worker.start()
        self.assertEqual(server.request("bounded"), "done")
        self.assertLessEqual(len(server.notifications), 4)
        self.assertLessEqual(server._notification_bytes, 500)
        server.socket.close(); peer.close(); worker.join(.5)


class HoldSemanticsTests(unittest.TestCase):
    def test_initial_active_state_and_failed_error_are_authoritative(self):
        class FakeServer:
            def __init__(self, status, error=None):
                self.status, self.error, self.reads = status, error, 0
            def request(self, method, params=None):
                if method == "thread/resume":
                    return {"thread": {"id": "canonical", "cwd": manager.MANAGER_CWD}}
                if method == "thread/settings/update":
                    return {}
                if method == "thread/read":
                    self.reads += 1
                    return {"thread": {"id": "canonical", "status": "idle", "turns": [
                        {"id": "turn", "status": self.status, "error": self.error}]}}
                raise AssertionError(method)
            def _receive_json(self, timeout=None):
                raise RuntimeError("stop fixture")

        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "config.json"
            config_path.write_text('{"manager_pane_id":"pane"}')
            for status, expected_state, error in (
                ("inProgress", "working", None),
                ("failed", "blocked", {"code": "provider_unavailable", "message": "retry later"}),
            ):
                with self.subTest(status=status), patch.object(manager, "CONFIG_PATH", config_path), patch.object(manager, "report_manager_lifecycle") as report:
                    output = StringIO()
                    with redirect_stdout(output), self.assertRaisesRegex(RuntimeError, "stop fixture"):
                        manager.hold(FakeServer(status, error), "canonical")
                    report.assert_called_once()
                    self.assertEqual(report.call_args.args[2], expected_state)
                    records = [json.loads(line) for line in output.getvalue().splitlines()]
                    self.assertTrue(records[0]["ready"])
                    self.assertEqual(records[0]["thread_id"], "canonical")
                    self.assertEqual(records[0]["status"], status)
                    self.assertEqual(records[0]["error"], error)
                    self.assertEqual(records[1]["observer_heartbeat"]["error"], error)


if __name__ == "__main__":
    unittest.main()
