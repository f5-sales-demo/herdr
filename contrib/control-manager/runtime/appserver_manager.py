#!/usr/bin/env python3
"""Create or verify the canonical Control Manager app-server thread."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import socket
import struct
import sys
import time
import uuid
from collections import deque
from pathlib import Path
from typing import Any

from control_portable import (
    machine_config_path,
    package_root,
    runtime_root,
    state_root,
    terminal_appserver_disconnect,
)


CONTROL_ROOT = package_root()
RUNTIME_ROOT = runtime_root(CONTROL_ROOT)
CODEX = os.environ.get("CODEX_BINARY", "codex")
MANAGER_NAME = os.environ.get("CODEX_CONTROL_MANAGER_NAME", "Control Manager")
MANAGER_CWD = os.environ.get("CODEX_CONTROL_MANAGER_CWD", str(CONTROL_ROOT))
CONFIG_PATH = machine_config_path()
APP_SERVER_SOCKET = os.environ.get(
    "CODEX_APP_SERVER_SOCKET", str(Path.home() / ".codex/app-server-control/app-server-control.sock")
)
APP_SERVER_REMOTE = os.environ.get("CODEX_APP_SERVER_REMOTE") or f"unix://{APP_SERVER_SOCKET}"
HERDR_SOCKET = os.environ.get("CODEX_CONTROL_HERDR_SOCKET", str(Path.home() / ".config/herdr/herdr.sock"))
BROKER_SOCKET = os.environ.get("CONTROL_BROKER_SOCKET", str(state_root() / "control.sock"))
MANAGER_MODEL = "gpt-6-astra"
MANAGER_EFFORT = "medium"
REQUIRED_CONTROL_TOOLS = {
    "completion_inbox", "ack_completion", "dispatch", "run_command",
    "continue_task", "status", "reply", "request_stop", "create_feature",
    "feature_status", "record_feature_evidence",
}


class AppServerResponseError(RuntimeError):
    """A definite JSON-RPC rejection, distinct from transport uncertainty."""


def manager_config() -> dict[str, Any]:
    return {
        "default_permissions": ":danger-full-access",
        "model": MANAGER_MODEL,
        "model_reasoning_effort": MANAGER_EFFORT,
        "web_search": "live",
        "features": {
            "apps": True,
            "browser_use": True,
            "browser_use_external": True,
            "computer_use": True,
            "hooks": False,
            "image_generation": True,
            "in_app_browser": True,
            "multi_agent": False,
            "network_proxy": True,
            "plugins": True,
            "realtime_conversation": True,
            "shell_tool": False,
            "unified_exec": False,
        },
        "mcp_servers": {
            "computer-use-xvfb": {"enabled": False},
            "playwright-headless": {"enabled": False},
            "control_broker": {
                "command": "/usr/bin/python3",
                "args": [str(RUNTIME_ROOT / "control_mcp.py")],
                "cwd": MANAGER_CWD,
                "env": {"CONTROL_BROKER_SOCKET": BROKER_SOCKET},
                "enabled": True,
                "required": True,
                "enabled_tools": [
                    "completion_inbox", "ack_completion", "dispatch", "run_command",
                    "continue_task", "status", "reply", "request_stop", "create_feature",
                    "feature_status", "record_feature_evidence"
                ],
                "default_tools_approval_mode": "approve",
                "startup_timeout_sec": 10,
                "tool_timeout_sec": 100,
            },
        },
    }


class AppServer:
    CONNECT_TIMEOUT = 5.0
    RPC_TIMEOUT = 5.0
    MAX_HANDSHAKE_BYTES = 16 * 1024
    # thread/read includes retained history; real long-lived manager threads
    # exceed 1 MiB. Bound the full response without rejecting ordinary history.
    MAX_FRAME_BYTES = 64 * 1024 * 1024
    MAX_MESSAGE_BYTES = 64 * 1024 * 1024
    MAX_NOTIFICATIONS = 256
    MAX_NOTIFICATION_BYTES = 2 * 1024 * 1024

    def __init__(self, *, connect_timeout: float | None = None, rpc_timeout: float | None = None):
        self.connect_timeout = connect_timeout or self.CONNECT_TIMEOUT
        self.rpc_timeout = rpc_timeout or self.RPC_TIMEOUT
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._receive_buffer = bytearray()
        self.notifications: deque[dict[str, Any]] = deque()
        self._notification_bytes = 0
        self.request_id = 0
        self.socket.settimeout(self.connect_timeout)
        try:
            self.socket.connect(APP_SERVER_SOCKET)
            self._websocket_handshake()
            self.request(
                "initialize",
                {
                    "clientInfo": {
                        "name": "control_manager_bootstrap",
                        "title": "Control Manager Bootstrap",
                        "version": "1.0.0",
                    },
                    "capabilities": {"experimentalApi": True},
                },
            )
            self.notify("initialized", {})
        except BaseException:
            self.socket.close()
            raise
        finally:
            if self.socket.fileno() >= 0:
                self.socket.settimeout(None)

    def _websocket_handshake(self) -> None:
        deadline = time.monotonic() + self.connect_timeout
        key = base64.b64encode(os.urandom(16)).decode()
        handshake = (
            "GET / HTTP/1.1\r\n"
            "Host: localhost\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        )
        self.socket.sendall(handshake.encode())
        response = bytearray()
        while b"\r\n\r\n" not in response:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("timed out waiting for app-server WebSocket handshake")
            self.socket.settimeout(remaining)
            piece = self.socket.recv(4096)
            if not piece:
                raise RuntimeError("app-server closed during WebSocket handshake")
            response.extend(piece)
            if len(response) > self.MAX_HANDSHAKE_BYTES:
                raise RuntimeError("oversized app-server WebSocket handshake")
        header, extra = bytes(response).split(b"\r\n\r\n", 1)
        if not header.startswith(b"HTTP/1.1 101 "):
            raise RuntimeError(f"app-server WebSocket handshake failed: {header[:200]!r}")
        expected = base64.b64encode(
            hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
        ).decode()
        if f"sec-websocket-accept: {expected}".lower() not in header.decode(errors="replace").lower():
            raise RuntimeError("app-server WebSocket handshake validation failed")
        self._receive_buffer.extend(extra)

    def close(self) -> None:
        try:
            self._send_frame(b"", opcode=8)
        except OSError:
            pass
        self.socket.close()

    def _read_exact(self, size: int) -> bytes:
        while len(self._receive_buffer) < size:
            piece = self.socket.recv(min(65536, size - len(self._receive_buffer)))
            if not piece:
                raise RuntimeError("app-server WebSocket closed")
            self._receive_buffer.extend(piece)
        data = bytes(self._receive_buffer[:size])
        del self._receive_buffer[:size]
        return data

    def _send_frame(self, payload: bytes, *, opcode: int = 1) -> None:
        if len(payload) > self.MAX_MESSAGE_BYTES:
            raise RuntimeError("app-server WebSocket outbound message is too large")
        mask = os.urandom(4)
        header = bytearray([0x80 | opcode])
        size = len(payload)
        if size < 126:
            header.append(0x80 | size)
        elif size < 65536:
            header.extend((0x80 | 126,))
            header.extend(struct.pack("!H", size))
        else:
            header.extend((0x80 | 127,))
            header.extend(struct.pack("!Q", size))
        header.extend(mask)
        header.extend(bytes(value ^ mask[index % 4] for index, value in enumerate(payload)))
        self.socket.sendall(header)

    def _receive_json(self, timeout: float | None = None) -> dict[str, Any]:
        previous = self.socket.gettimeout()
        self.socket.settimeout(timeout)
        fragments = bytearray()
        try:
            while True:
                first, second = self._read_exact(2)
                opcode = first & 0x0F
                final = bool(first & 0x80)
                size = second & 0x7F
                if size == 126:
                    size = struct.unpack("!H", self._read_exact(2))[0]
                elif size == 127:
                    size = struct.unpack("!Q", self._read_exact(8))[0]
                if size > self.MAX_FRAME_BYTES:
                    raise RuntimeError(f"app-server WebSocket frame is too large: {size} > {self.MAX_FRAME_BYTES}")
                mask = self._read_exact(4) if second & 0x80 else None
                payload = self._read_exact(size)
                if mask:
                    payload = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
                if opcode == 8:
                    raise RuntimeError("app-server WebSocket closed")
                if opcode == 9:
                    self._send_frame(payload, opcode=10)
                    continue
                if opcode not in {0, 1}:
                    continue
                if len(fragments) + len(payload) > self.MAX_MESSAGE_BYTES:
                    raise RuntimeError("app-server WebSocket message is too large")
                fragments.extend(payload)
                if final:
                    return json.loads(fragments)
        finally:
            self.socket.settimeout(previous)

    def send(self, payload: dict[str, Any]) -> None:
        self._send_frame(json.dumps(payload, separators=(",", ":")).encode())

    def notify(self, method: str, params: dict[str, Any]) -> None:
        self.send({"method": method, "params": params})

    def request(self, method: str, params: dict[str, Any] | None = None) -> Any:
        deadline = time.monotonic() + self.rpc_timeout
        self.request_id += 1
        request_id = self.request_id
        previous = self.socket.gettimeout()
        self.socket.settimeout(max(0.001, deadline - time.monotonic()))
        try:
            self.send({"id": request_id, "method": method, "params": params or {}})
        except socket.timeout as exc:
            raise RuntimeError(f"timed out sending app-server {method}") from exc
        finally:
            self.socket.settimeout(previous)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError(f"timed out waiting for app-server {method}")
            try:
                message = self._receive_json(remaining)
            except socket.timeout as exc:
                raise RuntimeError(f"timed out waiting for app-server {method}") from exc
            if message.get("id") != request_id:
                if "method" in message and "id" not in message:
                    self._queue_notification(message)
                continue
            if "error" in message:
                error = message["error"]
                raise AppServerResponseError(
                    f"app-server {error.get('code', 'error')}: {error.get('message', error)}"
                )
            return message.get("result")

    def _queue_notification(self, message: dict[str, Any]) -> None:
        size = len(json.dumps(message, separators=(",", ":")).encode())
        if size > self.MAX_NOTIFICATION_BYTES:
            return
        while self.notifications and (
            len(self.notifications) >= self.MAX_NOTIFICATIONS
            or self._notification_bytes + size > self.MAX_NOTIFICATION_BYTES
        ):
            removed = self.notifications.popleft()
            self._notification_bytes -= len(json.dumps(removed, separators=(",", ":")).encode())
        self.notifications.append(message)
        self._notification_bytes += size

    def _pop_notification(self, index: int) -> dict[str, Any]:
        self.notifications.rotate(-index)
        message = self.notifications.popleft()
        self.notifications.rotate(index)
        self._notification_bytes -= len(json.dumps(message, separators=(",", ":")).encode())
        return message

    def wait_notification(self, method: str, timeout: float = 120) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while True:
            for index, message in enumerate(self.notifications):
                if message.get("method") == method:
                    return self._pop_notification(index)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError(f"timed out waiting for {method}")
            try:
                message = self._receive_json(remaining)
            except socket.timeout as exc:
                raise RuntimeError(f"timed out waiting for {method}") from exc
            if message.get("method") == method:
                return message
            if "method" in message and "id" not in message:
                self._queue_notification(message)


def find_exact(server: AppServer) -> list[dict[str, Any]]:
    if CONFIG_PATH.exists():
        try:
            configured_id = json.loads(CONFIG_PATH.read_text()).get("manager_thread_id")
            if configured_id:
                configured = server.request(
                    "thread/read", {"threadId": configured_id, "includeTurns": False}
                )["thread"]
                if configured.get("name") == MANAGER_NAME and configured.get("cwd") == MANAGER_CWD:
                    return [configured]
        except (OSError, json.JSONDecodeError, RuntimeError):
            pass
    result = server.request(
        "thread/list",
        {
            "limit": 100,
            "sortKey": "created_at",
            "sortDirection": "desc",
            "sourceKinds": ["appServer", "cli", "vscode"],
            "searchTerm": MANAGER_NAME,
            "archived": False,
        },
    )
    return [
        thread
        for thread in result.get("data", [])
        if thread.get("name") == MANAGER_NAME and thread.get("cwd") == MANAGER_CWD
    ]


def enforce_manager_model(server: AppServer, thread_id: str) -> None:
    server.request(
        "thread/settings/update",
        {"threadId": thread_id, "model": MANAGER_MODEL, "effort": MANAGER_EFFORT},
    )


def persist_config(thread_id: str) -> None:
    try:
        existing = json.loads(CONFIG_PATH.read_text()) if CONFIG_PATH.exists() else {}
    except (OSError, json.JSONDecodeError):
        existing = {}
    payload = {
        **existing,
        "manager_thread_id": thread_id,
        "manager_thread_name": MANAGER_NAME,
        "manager_cwd": MANAGER_CWD,
        "app_server_remote": APP_SERVER_REMOTE,
        "profile": "control-manager",
        "updated_at": int(time.time()),
    }
    temporary = CONFIG_PATH.with_suffix(".json.new")
    old_umask = os.umask(0o077)
    try:
        temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        os.replace(temporary, CONFIG_PATH)
        os.chmod(CONFIG_PATH, 0o600)
    finally:
        os.umask(old_umask)


def herdr_request(method: str, params: dict[str, Any]) -> Any:
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.settimeout(5)
    try:
        client.connect(HERDR_SOCKET)
        request_id = f"control-manager-{uuid.uuid4().hex}"
        client.sendall(
            json.dumps({"id": request_id, "method": method, "params": params}, separators=(",", ":")).encode()
            + b"\n"
        )
        chunks = bytearray()
        while b"\n" not in chunks:
            piece = client.recv(65536)
            if not piece:
                raise RuntimeError("Herdr closed the lifecycle connection")
            chunks.extend(piece)
        response = json.loads(bytes(chunks).split(b"\n", 1)[0])
        if "error" in response:
            raise RuntimeError(response["error"].get("message", str(response["error"])))
        return response.get("result")
    finally:
        client.close()


def broker_request(method: str, params: dict[str, Any]) -> Any:
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.settimeout(5)
    try:
        client.connect(BROKER_SOCKET)
        client.sendall(json.dumps({"method": method, "params": params}, separators=(",", ":")).encode() + b"\n")
        chunks = bytearray()
        while b"\n" not in chunks:
            piece = client.recv(65536)
            if not piece:
                raise RuntimeError("broker closed response")
            chunks.extend(piece)
        response = json.loads(bytes(chunks).split(b"\n", 1)[0])
        if not response.get("ok"):
            raise RuntimeError(response.get("error", "broker request failed"))
        return response["result"]
    finally:
        client.close()


def report_manager_lifecycle(
    pane_id: str, thread_id: str, state: str, message: str, seq: int
) -> None:
    # App-server observation is independent of the UI. Never bind a shell or
    # foreign native client to this thread merely because it occupies a saved
    # pane; the broker publishes identity after verifying its remote launch.
    try:
        config = json.loads(CONFIG_PATH.read_text())
        if config.get("manager_pane_id") != pane_id or config.get("manager_thread_id") != thread_id:
            return
        result = herdr_request("agent.get", {"target": pane_id})
        agent = result.get("agent", result)
        if agent.get("agent") != "codex" or (agent.get("agent_session") or {}).get("value") != thread_id:
            return
    except Exception:
        return
    herdr_request(
        "pane.report_agent",
        {
            "pane_id": pane_id,
            "source": "control-appserver",
            "agent": "codex",
            "state": state,
            "message": message[:500],
            "seq": seq,
            "agent_session_id": thread_id,
        },
    )


def ensure(server: AppServer) -> dict[str, Any]:
    matches = find_exact(server)
    if len(matches) > 1:
        ids = ", ".join(thread["id"] for thread in matches)
        raise RuntimeError(f"multiple exact Control Manager threads exist: {ids}")
    created = False
    if matches:
        thread = matches[0]
    else:
        result = server.request(
            "thread/start",
            {
                "cwd": MANAGER_CWD,
                "approvalPolicy": "never",
                "model": MANAGER_MODEL,
                "config": manager_config(),
                "serviceName": "control_manager",
                "threadSource": "appServer",
            },
        )
        thread = result["thread"]
        server.request("thread/name/set", {"threadId": thread["id"], "name": MANAGER_NAME})
        server.request(
            "turn/start",
            {
                "threadId": thread["id"],
                "model": MANAGER_MODEL,
                "effort": MANAGER_EFFORT,
                "input": [
                    {
                        "type": "text",
                        "text": "Bootstrap this Control Manager thread. Do not dispatch work. Reply only: Control Manager ready.",
                    }
                ],
            },
        )
        completed = server.wait_notification("turn/completed")
        turn = completed.get("params", {}).get("turn", {})
        if turn.get("status") != "completed":
            raise RuntimeError(f"Control Manager bootstrap turn did not complete: {turn}")
        created = True
    enforce_manager_model(server, thread["id"])
    verified = server.request("thread/read", {"threadId": thread["id"], "includeTurns": False})["thread"]
    if verified.get("name") != MANAGER_NAME or verified.get("cwd") != MANAGER_CWD:
        raise RuntimeError(f"thread verification failed: {verified}")
    persist_config(thread["id"])
    return {
        "created": created,
        "thread_id": thread["id"],
        "name": verified.get("name"),
        "cwd": verified.get("cwd"),
        "status": verified.get("status"),
        "is_pinned": verified.get("isPinned"),
    }


def create_verified_candidate(server: AppServer) -> dict[str, Any]:
    result = server.request(
        "thread/start",
        {
            "cwd": MANAGER_CWD,
            "approvalPolicy": "never",
            "model": MANAGER_MODEL,
            "config": manager_config(),
            "serviceName": "control_manager",
            "threadSource": "appServer",
        },
    )
    thread = result["thread"]
    candidate_name = f"Control Manager candidate {thread['id'][-8:]}"
    server.request("thread/name/set", {"threadId": thread["id"], "name": candidate_name})
    server.request(
        "turn/start",
        {
            "threadId": thread["id"],
            "model": MANAGER_MODEL,
            "effort": MANAGER_EFFORT,
            "input": [
                {
                    "type": "text",
                    "text": (
                        "Use the control_broker status tool once. If it succeeds, reply only: "
                        "Control Manager ready. Do not dispatch work."
                    ),
                }
            ],
        },
    )
    completed = server.wait_notification("turn/completed")
    turn = completed.get("params", {}).get("turn", {})
    if turn.get("status") != "completed":
        raise RuntimeError(f"candidate bootstrap turn did not complete: {turn}")
    calls = [
        message.get("params", {}).get("item", {})
        for message in server.notifications
        if message.get("method") == "item/completed"
    ]
    if not any(
        item.get("type") == "mcpToolCall"
        and item.get("server") == "control_broker"
        and item.get("tool") == "status"
        and item.get("status") == "completed"
        for item in calls
    ):
        raise RuntimeError("candidate did not complete the required control_broker.status call")
    enforce_manager_model(server, thread["id"])
    return thread


def replace(server: AppServer) -> dict[str, Any]:
    matches = find_exact(server)
    if len(matches) > 1:
        raise RuntimeError(f"multiple exact Control Manager threads exist: {[item['id'] for item in matches]}")
    candidate = create_verified_candidate(server)
    if matches:
        old = matches[0]
        retired_name = f"Control Manager retired {old['id'][-8:]}"
        server.request("thread/name/set", {"threadId": old["id"], "name": retired_name})
        server.request("thread/archive", {"threadId": old["id"]})
    server.request("thread/name/set", {"threadId": candidate["id"], "name": MANAGER_NAME})
    persist_config(candidate["id"])
    return {
        "created": True,
        "thread_id": candidate["id"],
        "name": MANAGER_NAME,
        "cwd": MANAGER_CWD,
        "replaced_thread_id": matches[0]["id"] if matches else None,
        "broker_tool_verified": True,
    }


def refresh_tools(server: AppServer, thread_id: str) -> dict[str, Any]:
    """Create a verified capability-refresh candidate without replacing source.

    A recovery supervisor must never archive or replace the canonical manager:
    it can prove a candidate has the desired MCP inventory, then report that
    evidence for a separately authorized promotion.  Failed candidates are
    archived because they are disposable; successful candidates are retained
    for explicit review and the source thread/config remain untouched.
    """
    source = server.request("thread/read", {"threadId": thread_id, "includeTurns": False})["thread"]
    if source.get("name") != MANAGER_NAME or source.get("cwd") != MANAGER_CWD:
        raise RuntimeError("tool refresh source is not the canonical Control Manager")
    forked = server.request(
        "thread/fork",
        {
            "threadId": thread_id,
            "cwd": MANAGER_CWD,
            "approvalPolicy": "never",
            "model": MANAGER_MODEL,
            "config": manager_config(),
            "excludeTurns": False,
        },
    )["thread"]
    candidate_id = forked["id"]
    try:
        server.request("thread/name/set", {"threadId": candidate_id, "name": f"{MANAGER_NAME} refresh candidate"})
        server.request("config/mcpServer/reload", None)
        status = server.request(
            "mcpServerStatus/list",
            {"threadId": candidate_id, "detail": "toolsAndAuthOnly"},
        )
        control = next((item for item in status.get("data", []) if item.get("name") == "control_broker"), None)
        tools = set((control or {}).get("tools", {}))
        missing = REQUIRED_CONTROL_TOOLS - tools
        if control is None or control.get("runtimeStatus") != "connected" or missing:
            raise RuntimeError(f"candidate control broker inventory is incomplete: missing={sorted(missing)}")
    except Exception:
        server.request("thread/archive", {"threadId": candidate_id})
        raise
    return {
        "candidate_thread_id": candidate_id,
        "forked_from": thread_id,
        "tools": sorted(tools),
        "history_preserved": True,
        "canonical_unchanged": True,
        "promotion": "requires separate authorized install/rollout action",
    }


def refresh_tools_in_place(server: AppServer, thread_id: str) -> dict[str, Any]:
    """Reload the canonical manager's MCP inventory without changing identity."""
    source = server.request(
        "thread/read", {"threadId": thread_id, "includeTurns": False}
    )["thread"]
    if source.get("id") != thread_id or source.get("name") != MANAGER_NAME or source.get("cwd") != MANAGER_CWD:
        raise RuntimeError("tool refresh source is not the canonical Control Manager")
    resumed = server.request(
        "thread/resume",
        {
            "threadId": thread_id,
            "cwd": MANAGER_CWD,
            "approvalPolicy": "never",
            "model": MANAGER_MODEL,
            "config": manager_config(),
            "excludeTurns": True,
        },
    )["thread"]
    if resumed.get("id") != thread_id or resumed.get("cwd") != MANAGER_CWD:
        raise RuntimeError("in-place tool refresh changed canonical manager identity")
    server.request("config/mcpServer/reload", None)
    status = server.request(
        "mcpServerStatus/list",
        {"threadId": thread_id, "detail": "toolsAndAuthOnly"},
    )
    control = next(
        (item for item in status.get("data", []) if item.get("name") == "control_broker"),
        None,
    )
    tools = set((control or {}).get("tools", {}))
    missing = REQUIRED_CONTROL_TOOLS - tools
    if control is None or control.get("runtimeStatus") != "connected" or missing:
        raise RuntimeError(f"canonical control broker inventory is incomplete: missing={sorted(missing)}")
    verified = server.request(
        "thread/read", {"threadId": thread_id, "includeTurns": False}
    )["thread"]
    if verified.get("id") != thread_id or verified.get("name") != MANAGER_NAME or verified.get("cwd") != MANAGER_CWD:
        raise RuntimeError("canonical manager identity changed during in-place tool refresh")
    return {
        "canonical_thread_id": thread_id,
        "tools": sorted(tools),
        "canonical_unchanged": True,
        "refreshed_in_place": True,
    }

def activate_refreshed_tools(server: AppServer, thread_id: str) -> dict[str,Any]:
    """Verify a full-history candidate, then atomically select it as canonical.

    This command is intentionally separate from inspection-only refresh-tools
    and is called by the supervisor only behind guarded-live authorization.
    """
    candidate=refresh_tools(server,thread_id); candidate_id=candidate["candidate_thread_id"]
    source=server.request("thread/read",{"threadId":thread_id,"includeTurns":False})["thread"]
    if source.get("name") != MANAGER_NAME or source.get("cwd") != MANAGER_CWD:
        raise RuntimeError("canonical source changed before verified activation")
    persisted=False
    try:
        server.request("thread/name/set",{"threadId":thread_id,"name":f"Control Manager retired {thread_id[-8:]}"})
        server.request("thread/name/set",{"threadId":candidate_id,"name":MANAGER_NAME})
        persist_config(candidate_id); persisted=True
        server.request("thread/archive",{"threadId":thread_id})
    except Exception:
        # Best-effort rollback keeps the prior canonical binding usable. The
        # candidate was already verified but must not remain ambiguously named.
        if persisted:
            with __import__("contextlib").suppress(Exception): persist_config(thread_id)
        with __import__("contextlib").suppress(Exception): server.request("thread/name/set",{"threadId":thread_id,"name":MANAGER_NAME})
        with __import__("contextlib").suppress(Exception): server.request("thread/name/set",{"threadId":candidate_id,"name":f"{MANAGER_NAME} refresh candidate"})
        with __import__("contextlib").suppress(Exception): server.request("thread/archive",{"threadId":candidate_id})
        raise
    return candidate | {"activated":True,"canonical_thread_id":candidate_id,"prior_thread_id":thread_id,"full_history_forked":True}


def probe(server: AppServer, thread_id: str) -> dict[str, Any]:
    """Read one exact canonical thread and MCP inventory without mutation."""
    thread = server.request("thread/read", {"threadId": thread_id, "includeTurns": False})["thread"]
    recent = server.request(
        "thread/turns/list",
        {"threadId": thread_id, "limit": 1, "sortDirection": "desc", "itemsView": "notLoaded"},
    )
    # The exact-thread read is the transport/readiness authority.  Inventory
    # lookup can fail for a loaded thread (including thread-not-found during an
    # MCP refresh) and must not recategorize a healthy app-server as down.
    try:
        inventory = server.request("mcpServerStatus/list", {"threadId": thread_id, "detail": "toolsAndAuthOnly"})
        control = next((item for item in inventory.get("data", []) if item.get("name") == "control_broker"), None)
        control_state = {"inventory_verified": True, "runtime_status": (control or {}).get("runtimeStatus"),
                         "tools": sorted((control or {}).get("tools", {}))}
    except Exception as exc:
        control_state = {"inventory_verified": False, "runtime_status": "unknown", "tools": [],
                         "inventory_error": f"{type(exc).__name__}: {exc}"[:500]}
    turns = recent.get("data") or []
    last = turns[-1] if turns else {}
    # Remote transport is independent of manager readiness. Unsupported or
    # failed status RPCs must not turn a healthy app-server into a restart.
    try:
        remote = server.request("remoteControl/status/read", None)
        remote_control = {"status": remote.get("status", "unknown")}
    except Exception as exc:
        remote_control = {"status": "unknown", "reason": str(exc)[:300]}
    return {
        "thread": {"id": thread.get("id"), "status": thread.get("status"), "cwd": thread.get("cwd"), "name": thread.get("name")},
        "turn": {"id": last.get("id"), "status": last.get("status"), "error": last.get("error")},
        "control_broker": control_state,
        "remote_control": remote_control,
    }


def native_pane_health(thread_id: str) -> dict[str, Any]:
    """Read the configured native manager attachment without creating work.

    Herdr's documented ``agent.get`` is the authority for the reserved pane's
    live native session.  The configured pane must report the exact canonical
    thread; a shell/no-agent is repairable by the broker's claimed topology
    reconcile, while a foreign or blocked agent is intentionally not touched.
    """
    try:
        config = json.loads(CONFIG_PATH.read_text())
    except Exception as exc:
        return {"state": "degraded", "reason": f"manager bindings unavailable: {type(exc).__name__}: {exc}"[:500]}
    pane_id = str(config.get("manager_pane_id") or "")
    configured_thread = str(config.get("manager_thread_id") or "")
    if not pane_id or configured_thread != thread_id:
        return {"state": "degraded", "reason": "configured native manager pane/thread binding is incomplete or changed"}
    try:
        result = herdr_request("agent.get", {"target": pane_id})
        agent = result.get("agent", result) if isinstance(result, dict) else {}
    except Exception as exc:
        return {"state": "unavailable", "reason": f"configured native manager agent is absent: {type(exc).__name__}: {exc}"[:500],
                "pane_id": pane_id, "thread_id": thread_id}
    # Agent metadata is persisted independently of a terminal runtime.  In a
    # headless restore it can correctly describe the old Codex session while
    # the pane has no process at all.  Never classify that snapshot as live.
    try:
        info = herdr_request("pane.process_info", {"pane_id": pane_id})
        process_info = info.get("process_info", info) if isinstance(info, dict) else {}
        foreground = process_info.get("foreground_processes") or []
    except Exception as exc:
        return {"state": "unavailable", "reason": f"configured native manager process is absent: {type(exc).__name__}: {exc}"[:500],
                "pane_id": pane_id, "thread_id": thread_id}
    if not foreground:
        return {"state": "unavailable", "reason": "configured native manager has saved metadata but no foreground runtime",
                "pane_id": pane_id, "thread_id": thread_id}
    session = agent.get("agent_session") or {}
    actual = str(session.get("value") or "")
    status = str(agent.get("agent_status") or "unknown")
    binary = Path(str(config.get("codex_binary") or "")).expanduser()
    expected: list[str] | None = None
    if binary.is_file() and os.access(binary, os.X_OK):
        expected = [str(binary.resolve()), "--disable", "hooks", "--remote",
                    str(config.get("app_server_remote") or ""), "--profile",
                    str(config.get("profile") or "control-manager"), "-C",
                    str(config.get("manager_cwd") or ""), "resume", thread_id]
    exact_runtime = (expected is not None and len(foreground) == 1
                     and foreground[0].get("name") == "codex"
                     and foreground[0].get("argv") == expected)
    if actual == thread_id and exact_runtime and status in {"idle", "done"}:
        try:
            read = herdr_request("agent.read", {
                "target": pane_id, "source": "detection", "lines": 120,
                "format": "text", "strip_ansi": True,
            })
            terminal = str((read.get("read", read) if isinstance(read, dict) else {}).get("text") or "")
        except Exception as exc:
            return {"state": "degraded",
                    "reason": f"canonical native terminal state is unreadable: {type(exc).__name__}: {exc}"[:500],
                    "pane_id": pane_id, "thread_id": thread_id, "agent_status": status}
        if terminal_appserver_disconnect(terminal):
            return {"state": "unavailable",
                    "reason": "canonical native manager reports terminal app-server reconnect failure",
                    "pane_id": pane_id, "thread_id": thread_id, "agent_status": status,
                    "terminal_disconnect_proven": True}
    if actual == thread_id and not exact_runtime:
        # Do not overwrite or interrupt a process that happens to carry stale
        # canonical metadata.  A non-idle process is a user-visible busy pane.
        state = "waiting_user" if status in {"working", "blocked"} else "degraded"
        return {"state": state, "reason": "configured native metadata lacks strict live remote-process proof",
                "pane_id": pane_id, "thread_id": thread_id, "agent_status": status}
    if not actual and status in {"idle", "done"} and agent.get("agent") == "codex":
        # Missing metadata is repairable only for the exact remote command
        # that the broker is authorized to own. Unknown/foreign clients retain
        # the non-actionable state below; this check never publishes identity.
        try:
            if exact_runtime:
                return {"state": "unavailable", "reason": "canonical remote client is missing native session metadata",
                        "pane_id": pane_id, "thread_id": thread_id, "agent_status": status}
        except Exception:
            pass
    if actual != thread_id:
        state = "waiting_user" if status in {"working", "blocked"} else "degraded"
        return {"state": state, "reason": "configured native pane is occupied by a different or unverified agent session",
                "pane_id": pane_id, "thread_id": thread_id, "agent_status": status}
    if status == "blocked":
        return {"state": "waiting_user", "reason": "canonical native manager is waiting for user input",
                "pane_id": pane_id, "thread_id": thread_id, "agent_status": status}
    if status in {"idle", "done", "working"}:
        return {"state": "healthy", "reason": "configured native pane is attached to exact canonical manager thread",
                "pane_id": pane_id, "thread_id": thread_id, "agent_status": status}
    return {"state": "degraded", "reason": "configured native manager agent has an unverified lifecycle state",
            "pane_id": pane_id, "thread_id": thread_id, "agent_status": status}


def resume_once(server: AppServer, thread_id: str, expected_failed_turn_id: str) -> dict[str, Any]:
    """Start one distinct recovery turn on the exact existing manager thread.

    The thread resume reattaches the runtime only.  The following turn is a
    narrowly scoped recovery continuation, not a replay of its failed user
    request, and is issued only after the supervisor has reconciled durable
    broker work and reread authoritative state.
    """
    if not expected_failed_turn_id:
        raise RuntimeError("manager recovery requires the exact failed turn id")
    # Re-read immediately before mutation.  A stale supervisor observation
    # must not append a recovery turn after a user/new manager turn won a race.
    authoritative = server.request("thread/read", {"threadId": thread_id, "includeTurns": False})["thread"]
    turns = server.request(
        "thread/turns/list",
        {"threadId": thread_id, "limit": 1, "sortDirection": "desc", "itemsView": "notLoaded"},
    ).get("data") or []
    last = turns[-1] if turns else {}
    if last.get("id") != expected_failed_turn_id or last.get("status") != "failed":
        raise RuntimeError("authoritative failed turn changed before recovery continuation")
    thread = server.request("thread/resume", {
        "threadId": thread_id, "cwd": MANAGER_CWD, "approvalPolicy": "never",
        "model": MANAGER_MODEL, "config": manager_config(), "excludeTurns": True,
    })["thread"]
    if thread.get("id") != thread_id or thread.get("cwd") != MANAGER_CWD:
        raise RuntimeError("manager resume did not preserve canonical identity")
    started=server.request("turn/start", {"threadId":thread_id,"model":MANAGER_MODEL,"effort":MANAGER_EFFORT,"input":[{"type":"text","text":"A supervisor has reconciled durable control work after a transient terminal failure. Perform only the claimed recovery continuation from current durable state. Do not replay any original user request, dispatch work, or alter manager identity. Report the verified current recovery outcome."}]})
    turn=(started.get("turn") or {})
    if not turn.get("id"):
        raise RuntimeError("manager recovery continuation did not create a turn")
    completed=server.wait_notification("turn/completed")
    finished=(completed.get("params") or {}).get("turn") or {}
    if finished.get("id") != turn["id"] or finished.get("status") != "completed":
        raise RuntimeError(f"manager recovery continuation did not complete successfully: {finished}")
    return {"thread_id":thread_id,"recovery_turn_id":turn["id"],"status":finished["status"],"history_preserved":True,"original_request_replayed":False}


def hold(server: AppServer, thread_id: str) -> None:
    result = server.request(
        "thread/resume",
        {
            "threadId": thread_id,
            "cwd": MANAGER_CWD,
            "approvalPolicy": "never",
            "model": MANAGER_MODEL,
            "config": manager_config(),
            "excludeTurns": True,
        },
    )
    enforce_manager_model(server, thread_id)
    thread = result["thread"]
    if thread.get("id") != thread_id or thread.get("cwd") != MANAGER_CWD:
        raise RuntimeError(f"manager runtime resumed with unexpected cwd: {thread.get('cwd')}")
    # Resume attaches the observer to the canonical native thread; this read is
    # authoritative and does not replay a turn or alter its identity/history.
    authoritative = server.request(
        "thread/read", {"threadId": thread_id, "includeTurns": False}
    )["thread"]
    if authoritative.get("id") != thread_id:
        raise RuntimeError("manager observer read returned a different thread")
    turns = server.request(
        "thread/turns/list",
        {"threadId": thread_id, "limit": 1, "sortDirection": "desc", "itemsView": "notLoaded"},
    ).get("data") or []
    latest_turn = turns[-1] if turns else {}
    latest_status = latest_turn.get("status") or authoritative.get("status") or "idle"
    latest_error = latest_turn.get("error")

    def emit_heartbeat(status: str, error: Any = None) -> None:
        print(json.dumps({"observer_heartbeat": {
            "thread_id": thread_id,
            "timestamp": time.time(),
            "status": status,
            "error": error,
        }}, separators=(",", ":"), default=str), flush=True)

    def lifecycle(status: str) -> tuple[str, str]:
        if status in {"inProgress", "in_progress", "running", "started", "working"}:
            return "working", "Control Manager turn active"
        if status in {"failed", "interrupted", "cancelled", "canceled"}:
            return "blocked", f"Control Manager turn {status}"
        return "idle", f"Control Manager settled ({status})"

    config = json.loads(CONFIG_PATH.read_text())
    pane_id = str(config.get("manager_pane_id", ""))
    seq = 1
    initial_state, initial_detail = lifecycle(latest_status)
    if pane_id:
        report_manager_lifecycle(pane_id, thread_id, initial_state, initial_detail, seq)
    print(json.dumps({
        "ready": True, "thread_id": thread_id, "status": latest_status, "error": latest_error,
    }, separators=(",", ":"), default=str), flush=True)
    emit_heartbeat(latest_status, latest_error)
    probe_interval = max(0.1, float(os.environ.get("CODEX_CONTROL_OBSERVER_PROBE_SECONDS", "10")))
    while True:
        try:
            message = server._receive_json(probe_interval)
        except socket.timeout:
            # WebSocket silence is normal.  A bounded read-only RPC proves both
            # transport liveness and the exact native thread's current state.
            try:
                observed = server.request(
                    "thread/read", {"threadId": thread_id, "includeTurns": False}
                )["thread"]
                if observed.get("id") != thread_id:
                    raise RuntimeError("manager heartbeat read returned a different thread")
                emit_heartbeat(latest_status, latest_error)
            except Exception as exc:
                emit_heartbeat("transport_error", {
                    "type": type(exc).__name__, "message": str(exc),
                })
                raise
            continue
        params = message.get("params") or {}
        message_thread = params.get("threadId") or (params.get("turn") or {}).get("threadId")
        if message_thread and message_thread != thread_id:
            continue
        method = message.get("method", "")
        state = None
        detail = method
        if method == "turn/started":
            state = "working"
            detail = "Control Manager turn started"
            latest_status, latest_error = "inProgress", None
        elif method in {
            "item/tool/requestUserInput",
            "mcpServer/elicitation/request",
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
            "item/permissions/requestApproval",
        }:
            state = "blocked"
            detail = "Control Manager is waiting for user input"
        elif method == "item/completed":
            item = params.get("item") or {}
            response_text = item.get("text") or item.get("message")
            if item.get("type") == "agentMessage" and response_text:
                try:
                    metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
                    event_ids = metadata.get("control_completion_event_ids")
                    # The current app-server does not promise this field.  If a
                    # future supported item supplies it, it is trusted broker
                    # metadata; otherwise the broker retains legacy text-only
                    # correlation without asking speech to carry IDs.
                    if not isinstance(event_ids, list) or not all(isinstance(value, str) for value in event_ids):
                        event_ids = None
                    correlation = broker_request("manager_response", {
                        "manager_turn_id": params.get("turnId") or item.get("turnId") or "unknown-manager-turn",
                        "item_id": item.get("id") or f"message-{uuid.uuid4().hex}",
                        "text": response_text,
                        **({"event_ids": event_ids} if event_ids else {}),
                    })
                    matched = correlation.get("matched_event_ids") or []
                    if matched:
                        print(f"manager response correlated {len(matched)} completion event(s)", file=sys.stderr, flush=True)
                except Exception as exc:
                    print(f"manager response correlation failed: {exc}", file=sys.stderr, flush=True)
        elif method == "turn/completed":
            turn = params.get("turn") or {}
            latest_status = turn.get("status", "completed")
            latest_error = turn.get("error")
            state, detail = lifecycle(latest_status)
            emit_heartbeat(latest_status, latest_error)
        if state and pane_id:
            seq += 1
            try:
                report_manager_lifecycle(pane_id, thread_id, state, detail, seq)
            except Exception as exc:
                print(f"manager lifecycle report failed: {exc}", file=sys.stderr, flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("ensure", "verify", "candidate", "replace", "refresh-tools", "refresh-tools-in-place", "refresh-tools-activate", "hold", "probe", "native-pane-health", "resume"), nargs="?", default="ensure"
    )
    parser.add_argument("thread_id", nargs="?")
    parser.add_argument("expected_failed_turn_id", nargs="?")
    args = parser.parse_args()
    if args.command == "native-pane-health":
        if not args.thread_id:
            raise RuntimeError("native-pane-health requires a thread id")
        print(json.dumps(native_pane_health(args.thread_id), sort_keys=True))
        return 0
    server = AppServer()
    try:
        if args.command == "hold":
            if not args.thread_id:
                raise RuntimeError("hold requires a thread id")
            hold(server, args.thread_id)
            return 0
        if args.command == "probe":
            if not args.thread_id:
                raise RuntimeError("probe requires a thread id")
            print(json.dumps(probe(server, args.thread_id), sort_keys=True))
            return 0
        if args.command == "resume":
            if not args.thread_id or not args.expected_failed_turn_id:
                raise RuntimeError("resume requires thread id and exact failed turn id")
            print(json.dumps(resume_once(server, args.thread_id, args.expected_failed_turn_id), sort_keys=True))
            return 0
        if args.command == "refresh-tools":
            if not args.thread_id:
                raise RuntimeError("refresh-tools requires a thread id")
            result = refresh_tools(server, args.thread_id)
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0
        if args.command == "refresh-tools-in-place":
            if not args.thread_id:
                raise RuntimeError("refresh-tools-in-place requires a thread id")
            result = refresh_tools_in_place(server, args.thread_id)
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0
        if args.command == "refresh-tools-activate":
            if not args.thread_id:
                raise RuntimeError("refresh-tools-activate requires a thread id")
            print(json.dumps(activate_refreshed_tools(server,args.thread_id),indent=2,sort_keys=True))
            return 0
        if args.command == "ensure":
            result = ensure(server)
        elif args.command == "candidate":
            thread = create_verified_candidate(server)
            result = {"thread_id": thread["id"], "name": thread.get("name"), "verified": True}
        elif args.command == "replace":
            result = replace(server)
        else:
            matches = find_exact(server)
            if len(matches) != 1:
                raise RuntimeError(f"expected exactly one Control Manager thread; found {len(matches)}")
            thread = server.request(
                "thread/read", {"threadId": matches[0]["id"], "includeTurns": False}
            )["thread"]
            result = {"thread_id": thread["id"], "name": thread.get("name"), "cwd": thread.get("cwd"), "status": thread.get("status")}
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    finally:
        server.close()


if __name__ == "__main__":
    raise SystemExit(main())
