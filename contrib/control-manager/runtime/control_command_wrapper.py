#!/usr/bin/env python3
"""Run one visible command work unit and report lifecycle structurally."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

from control_client import request


TASK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")


def spool_exit(exit_status: int) -> Path:
    """Persist bounded structural exit evidence before contacting the broker."""
    task_id = os.environ.get("CONTROL_TASK_ID", "")
    broker_socket = Path(os.environ.get("CONTROL_BROKER_SOCKET", ""))
    if not TASK_ID_RE.fullmatch(task_id) or not broker_socket.is_absolute():
        raise RuntimeError("cannot derive a safe command event spool path")
    event_dir = broker_socket.parent / "command-events"
    old_umask = os.umask(0o077)
    try:
        event_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        info = event_dir.stat()
        if info.st_uid != os.getuid() or not event_dir.is_dir():
            raise RuntimeError("command event directory is not owner-controlled")
        os.chmod(event_dir, 0o700)
        destination = event_dir / f"{task_id}.json"
        temporary = event_dir / f".{task_id}.{os.getpid()}.json"
        payload = {
            "task_id": task_id,
            "phase": "exited",
            "exit_status": exit_status,
            "created_at": time.time(),
        }
        with temporary.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, destination)
        return destination
    finally:
        os.umask(old_umask)


def report(phase: str, exit_status: int | None = None) -> None:
    task_id = os.environ.get("CONTROL_TASK_ID")
    if not task_id:
        raise RuntimeError("CONTROL_TASK_ID is not set")
    params: dict[str, object] = {"task_id": task_id, "phase": phase}
    if exit_status is not None:
        params["exit_status"] = exit_status
    request("command_event", params)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shell", required=True, choices=("zsh", "bash"))
    parser.add_argument("command", nargs=1)
    args = parser.parse_args()
    shell = {"zsh": "/usr/bin/zsh", "bash": "/usr/bin/bash"}[args.shell]
    task_id = os.environ.get("CONTROL_TASK_ID", "unknown")
    print(f"[control-command task={task_id} phase=started shell={args.shell}]", flush=True)
    try:
        report("started")
    except Exception as exc:
        print(f"[control-command warning=start-report-failed detail={exc}]", file=sys.stderr, flush=True)
    process = subprocess.Popen([shell, "-lc", args.command[0]])
    try:
        status = process.wait()
    except KeyboardInterrupt:
        status = process.wait()
        if status < 0:
            status = 128 + abs(status)
    if status < 0:
        status = 128 + abs(status)
    print(f"[control-command task={task_id} phase=exited status={status}]", flush=True)
    spool_path: Path | None = None
    try:
        spool_path = spool_exit(status)
    except Exception as exc:
        print(f"[control-command warning=exit-spool-failed detail={exc}]", file=sys.stderr, flush=True)
    try:
        report("exited", status)
        if spool_path is not None:
            spool_path.unlink(missing_ok=True)
    except Exception as exc:
        print(f"[control-command warning=exit-report-failed detail={exc}]", file=sys.stderr, flush=True)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
