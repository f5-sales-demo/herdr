#!/usr/bin/env python3
"""Native Herdr recovery control plugin; talks only to the owner supervisor."""
from __future__ import annotations

import json
import os
import socket
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _config() -> dict[str, Any]:
    # A linked/installed plugin does not inherit the supervisor's service
    # environment. Prefer explicit bindings and otherwise use the package's
    # machine-local state location; never assume a sibling package-root config.
    plugin_config = os.environ.get("HERDR_PLUGIN_CONFIG_DIR")
    xdg = Path(os.environ.get("XDG_STATE_HOME", str(Path.home() / ".local/state")))
    default = Path(plugin_config) / "machine.json" if plugin_config else xdg / "codex-control" / "machine.json"
    path = Path(os.environ.get("CODEX_CONTROL_CONFIG_PATH", default))
    return json.loads(path.read_text())


def _exchange(path: str, payload: dict[str, Any], *, timeout: float = 5,
              max_bytes: int = 1024 * 1024) -> dict[str, Any]:
    """Bound the complete exchange, including fragmented or trickling replies."""
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    deadline = time.monotonic() + timeout
    def remaining() -> None:
        seconds = deadline - time.monotonic()
        if seconds <= 0:
            raise TimeoutError("recovery request deadline exceeded")
        client.settimeout(seconds)
    try:
        remaining()
        client.connect(path)
        remaining()
        client.sendall(json.dumps(payload, separators=(",", ":")).encode() + b"\n")
        buffer = bytearray()
        while b"\n" not in buffer:
            remaining()
            part = client.recv(min(65536, max_bytes + 1 - len(buffer)))
            if not part:
                raise RuntimeError("peer closed before complete JSON response")
            buffer.extend(part)
            if len(buffer) > max_bytes:
                raise RuntimeError("recovery response exceeds size limit")
        response = json.loads(buffer.split(b"\n", 1)[0])
        if not isinstance(response, dict):
            raise RuntimeError("invalid recovery response")
        return response
    finally:
        client.close()


def _request(method: str) -> dict[str, Any]:
    response = _exchange(_config()["supervisor_socket"], {"method": method})
    if not response.get("ok"):
        raise RuntimeError(str(response.get("error", "supervisor rejected request")))
    return response["result"]


def _herdr_request(method: str, params: dict[str, Any]) -> dict[str, Any]:
    """Use only the session socket supplied to a native plugin process."""
    path = os.environ.get("HERDR_SOCKET_PATH")
    if not path:
        raise RuntimeError("Herdr did not supply a session socket")
    response = _exchange(path, {"id": "control-recovery-refresh", "method": method, "params": params})
    if "error" in response:
        raise RuntimeError(str(response["error"].get("message", response["error"])))
    return response


def _reopen_popup(notice: str) -> None:
    """Replace the modal so an action always leaves a live, refreshed view."""
    # A native action has no popup identity, but popup.close is session-modal
    # and scoped by HERDR_SOCKET_PATH. Ignore no-popup because actions are
    # also valid from the workspace launcher.
    try:
        _herdr_request("popup.close", {})
    except RuntimeError:
        pass
    _herdr_request("plugin.pane.open", {
        "plugin_id": "robin.control-recovery", "entrypoint": "health",
        "placement": "popup", "focus": True,
        "env": {"CODEX_CONTROL_RECOVERY_NOTICE": notice},
    })


def _when(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "—"
    return datetime.fromtimestamp(value, UTC).strftime("%Y-%m-%d %H:%M:%SZ")


def _outcome(action: dict[str, Any]) -> str:
    raw = action.get("outcome_json")
    if not raw:
        return str(action.get("state", "unknown"))
    try:
        parsed = json.loads(raw)
        return f"{action.get('state', 'unknown')}: {parsed.get('note') or parsed.get('reason') or parsed.get('state', 'recorded')}"
    except (TypeError, json.JSONDecodeError):
        return str(action.get("state", "recorded"))


def _brief(value: Any, limit: int = 120) -> str:
    text = " ".join(str(value or "").split())
    return text if len(text) <= limit else text[:limit - 1] + "…"


def _clip_line(value: str, width: int) -> str:
    """Keep output on one terminal row; the native popup does not wrap safely."""
    if width <= 1:
        return "…"
    return value if len(value) <= width else value[:width - 1] + "…"


def _fit_popup(header: list[str], sections: list[list[str]], footer: list[str], *, width: int, height: int) -> str:
    """Fit a live popup without sacrificing its health header or controls."""
    width = max(1, width)
    available = max(0, height - len(header) - len(footer))
    body: list[str] = []
    # Reserve durable-outcome rows before optional component/session details.
    outcomes = sections[-1] if sections else []
    reserved_outcomes = min(len(outcomes), 3) if outcomes and available else 0
    for section in sections[:-1]:
        for line in section:
            if len(body) >= max(0, available - reserved_outcomes):
                break
            body.append(line)
    for line in outcomes:
        if len(body) >= available:
            break
        body.append(line)
    return "\n".join(_clip_line(line, width) for line in [*header, *body, *footer][:height])


def render(status: dict[str, Any], *, notice: str | None = None,
           width: int | None = None, height: int | None = None) -> str:
    """Render stable, terminal-readable popup content rather than raw JSON."""
    header = ["Control Manager recovery", "=" * 24]
    header.append(f"Health: {status.get('state', 'unavailable')}")
    mode = status.get("supervisor_mode", _config().get("supervisor_mode", "observation_only"))
    automatic = "paused" if status.get("paused") else (
        "enabled" if mode == "isolated_active" or (
            mode == "guarded_live" and status.get("recovery_live_enabled", _config().get("recovery_live_enabled")) is True
        ) else "observation only")
    header.append(f"Automatic recovery: {automatic}")
    if notice:
        header.append(f"Action: {notice}")
    active = [item for item in status.get("recent_outcomes", []) if item.get("state") in {"claimed", "recovering"}]
    current = ["", "Current recovery action:"]
    if active:
        for item in active[:3]:
            current.append(f"- {item.get('component', 'unknown')} / {item.get('kind', 'unknown')} ({item.get('state')})")
    else:
        current.append("- none")
    health = ["", "Component health:"]
    components = status.get("components", [])
    if not components:
        health.append("- no supervisor observations yet")
    for item in components:
        detail = _brief(item.get("reason"), 46)
        suffix = f" — {detail}" if detail else ""
        health.append(f"- {item.get('component', 'unknown')}: {item.get('status', 'unknown')} ({item.get('failures', 0)} failures) [{_when(item.get('checked_at'))}]{suffix}")
    sessions = status.get("affected_sessions") or _config().get("affected_sessions") or []
    affected = ["", "Affected sessions:"]
    if sessions:
        for session in sessions[:10]:
            affected.append(f"- {session}")
    else:
        affected.append("- none reported by supervisor")
    outcomes = ["", "Durable recent outcomes:"]
    recent = status.get("recent_outcomes", [])
    if recent:
        for item in recent[:4]:
            outcomes.append(f"- {_when(item.get('updated_at'))} {item.get('component', 'unknown')}: {_brief(_outcome(item))}")
    else:
        outcomes.append("- none")
    footer = ["", "Controls: Recover now · Pause automatic recovery · Resume automatic recovery"]
    if width is None or height is None:
        return "\n".join([*header, *current, *health, *affected, *outcomes, *footer])
    return _fit_popup(header, [current, health, affected, outcomes], footer, width=width, height=height)


def main() -> int:
    action = sys.argv[1] if len(sys.argv) > 1 else "status"
    methods = {"status": "status", "recover": "recover", "pause": "pause", "resume": "resume"}
    if action not in methods:
        print(f"Control Manager recovery\n\nUnavailable: unsupported action {action!r}")
        return 2
    try:
        notice = None if action == "status" else f"{action.replace('_', ' ')} requested"
        if notice:
            # Show progress before awaiting an action: recovery may outlast the
            # client response deadline while the independent service continues.
            _reopen_popup(notice)
        result = _request(methods[action])
        status = result if "components" in result else _request("status")
        print(render(status, notice=notice), end="")
        return 0
    except Exception as exc:
        print(f"Control Manager recovery\n\nUnavailable: {exc}")
        return 1


def popup() -> int:
    """Polling native popup; actions replace it with a refreshed instance."""
    notice = os.environ.get("CODEX_CONTROL_RECOVERY_NOTICE")
    while True:
        try:
            size = os.get_terminal_size()
            screen = render(_request("status"), notice=notice, width=size.columns, height=size.lines)
        except Exception as exc:
            size = os.get_terminal_size()
            screen = _fit_popup(["Control Manager recovery", "", f"Unavailable: {exc}"], [], ["", "Controls: Recover now · Pause automatic recovery · Resume automatic recovery"], width=size.columns, height=size.lines)
        sys.stdout.write("\033[H\033[2J" + screen)
        sys.stdout.flush()
        time.sleep(0.5)


if __name__ == "__main__":
    raise SystemExit(popup() if len(sys.argv) > 1 and sys.argv[1] == "popup" else main())
