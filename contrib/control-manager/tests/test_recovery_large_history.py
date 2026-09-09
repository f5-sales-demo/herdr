"""Long-lived manager histories must fit the bounded WebSocket transport."""
import json
import socket
import struct
import threading
import unittest
from collections import deque
from appserver_manager import AppServer


class LargeHistoryAcceptance(unittest.TestCase):
    def test_multi_megabyte_history_response_is_read_without_removing_limits(self):
        client,peer=socket.socketpair()
        server=AppServer.__new__(AppServer)
        server.socket=client; server._receive_buffer=bytearray()
        server.notifications=deque(); server._notification_bytes=0
        payload=json.dumps({'result':{'history':'x'*(3*1024*1024)}}).encode()
        wire=bytes((0x81,127))+struct.pack('!Q',len(payload))+payload
        errors=[]
        def send():
            try: peer.sendall(wire)
            except OSError as exc: errors.append(str(exc))
        peer.settimeout(3)
        thread=threading.Thread(target=send,daemon=True); thread.start()
        try:
            result=server._receive_json(3)
            self.assertEqual(len(result['result']['history']),3*1024*1024)
            self.assertLessEqual(server.MAX_FRAME_BYTES,64*1024*1024)
            self.assertLessEqual(server.MAX_MESSAGE_BYTES,64*1024*1024)
        finally:
            client.close(); peer.close(); thread.join(3)
        self.assertFalse(thread.is_alive()); self.assertEqual(errors,[])


if __name__=='__main__': unittest.main()
