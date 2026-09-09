#!/usr/bin/env python3
"""Exercise installed recovery-manager entry points without contacting services."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def run(path: Path, *args: str, expected: int = 0) -> None:
    result = subprocess.run([sys.executable, str(path), *args], env=os.environ, text=True, capture_output=True)
    if result.returncode != expected:
        raise RuntimeError(f"installed entry point {path.name} returned {result.returncode}: {(result.stderr or result.stdout)[-500:]}")


def main() -> None:
    root = Path(os.environ["CODEX_CONTROL_ROOT"]).resolve()
    runtime = root / "runtime"
    if not (root / "control-package.json").is_file() or not runtime.is_dir():
        raise RuntimeError("smoke requires an installed package root")
    sys.path.insert(0, str(runtime))
    import appserver_manager

    expected_socket = os.environ.get("CODEX_APP_SERVER_SOCKET", appserver_manager.APP_SERVER_SOCKET)
    expected_remote = os.environ.get("CODEX_APP_SERVER_REMOTE") or f"unix://{expected_socket}"
    if appserver_manager.APP_SERVER_SOCKET != expected_socket or appserver_manager.APP_SERVER_REMOTE != expected_remote:
        raise RuntimeError("manager helpers do not share the configured AppServer endpoint")

    mcp_path = Path(appserver_manager.manager_config()["mcp_servers"]["control_broker"]["args"][0])
    if mcp_path != runtime / "control_mcp.py" or not mcp_path.is_file():
        raise RuntimeError("manager config does not bind the installed control MCP runtime")
    for name in ("control_broker.py", "worker_appserver.py", "appserver_manager.py", "control_supervisor.py", "control_recovery_rollout.py"):
        run(runtime / name, "--help")
    run(root / "herdr-control-recovery" / "recovery_plugin.py", "unsupported", expected=2)
    print("installed runtime entry points, MCP path, and AppServer endpoint verified")


if __name__ == "__main__":
    main()
