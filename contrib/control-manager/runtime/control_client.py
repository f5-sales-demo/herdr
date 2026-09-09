#!/usr/bin/env python3
"""Client entry points for controlctl and worker control-report."""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
from pathlib import Path
from typing import Any

from control_portable import state_root


DEFAULT_SOCKET = str(state_root() / "control.sock")
SETTLED = {"waiting_human", "blocked", "failed", "completed", "cancelled", "unknown"}


def is_settled(task: dict[str, Any]) -> bool:
    state = task.get("state")
    if state in {"waiting_human", "blocked", "unknown"}:
        return True
    if state not in {"failed", "completed", "cancelled"}:
        return False
    # Startup failures have no semantic terminal report and are immediately final.
    if task.get("terminal_reported_at") is None:
        return True
    # A terminal result is not relay-ready until the broker has captured its
    # bounded evidence excerpt.  This also closes the small idle-before-read
    # race in asynchronous command completion.
    if task.get("output_excerpt") is None:
        return False
    if task.get("work_kind") == "command":
        return task.get("herdr_state") in {"idle", "closed"}
    return task.get("herdr_state") in {"idle", "done", "closed"}


def request(method: str, params: dict[str, Any], *, socket_path: str | None = None) -> Any:
    path = socket_path or os.environ.get("CONTROL_BROKER_SOCKET", DEFAULT_SOCKET)
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.settimeout(15)
    try:
        client.connect(path)
        client.sendall(json.dumps({"method": method, "params": params}, separators=(",", ":")).encode() + b"\n")
        chunks = bytearray()
        while b"\n" not in chunks:
            piece = client.recv(65536)
            if not piece:
                raise RuntimeError("broker closed the socket without a response")
            chunks.extend(piece)
            if len(chunks) > 1_000_000:
                raise RuntimeError("broker response is too large")
        response = json.loads(bytes(chunks).split(b"\n", 1)[0])
    finally:
        client.close()
    if not response.get("ok"):
        raise RuntimeError(response.get("error", "broker request failed"))
    return response["result"]


def print_task(task: dict[str, Any]) -> None:
    line = f"{task['id']}  {task['state']}  {task['priority']}  {task['target']}  {task['summary']}"
    print(line)
    if task.get("question"):
        print(f"  question: {task['question']}")


def controlctl() -> int:
    parser = argparse.ArgumentParser(prog="controlctl", description="Dispatch and inspect Control Manager tasks")
    subs = parser.add_subparsers(dest="command", required=True)

    dispatch = subs.add_parser("dispatch")
    dispatch.add_argument("--target", required=True)
    dispatch.add_argument("--cwd", required=True, type=Path)
    dispatch.add_argument("--priority", choices=("routine", "normal", "attention", "critical"), default="normal")
    dispatch.add_argument("--prompt", required=True)
    dispatch.add_argument("--parent")
    dispatch.add_argument("--idempotency-key", help="Reuse this key and the same arguments to recover an uncertain admission")
    dispatch.add_argument(
        "--model",
        choices=("gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"),
        default="gpt-5.6-sol",
    )
    dispatch.add_argument(
        "--reasoning-effort",
        choices=("low", "medium", "high", "xhigh", "max"),
    )
    dispatch.add_argument("--wait", type=int, default=90, metavar="SECONDS")
    dispatch.add_argument("--json", action="store_true")

    run = subs.add_parser("run")
    run.add_argument("--label", required=True)
    run.add_argument("--cwd", required=True, type=Path)
    run.add_argument("--shell", choices=("zsh", "bash"), default="zsh")
    run.add_argument("--priority", choices=("routine", "normal", "attention", "critical"), default="normal")
    run.add_argument("--command", dest="shell_command", required=True)
    run.add_argument("--idempotency-key", help="Reuse this key and the same arguments to recover an uncertain admission")
    run.add_argument("--wait", type=int, default=90, metavar="SECONDS")
    run.add_argument("--json", action="store_true")

    status = subs.add_parser("status")
    status.add_argument("--task")
    status.add_argument("--json", action="store_true")

    reply = subs.add_parser("reply")
    reply.add_argument("task_id")
    reply.add_argument("text")
    reply.add_argument("--json", action="store_true")

    followup = subs.add_parser("continue")
    followup.add_argument("task_id")
    followup.add_argument("text")
    followup.add_argument("--idempotency-key", required=True)
    followup.add_argument("--supersede-pending", action="store_true")
    followup.add_argument("--wait", type=int, default=90, metavar="SECONDS")
    followup.add_argument("--json", action="store_true")

    stop = subs.add_parser("request-stop")
    stop.add_argument("task_id")
    stop.add_argument("--json", action="store_true")

    args = parser.parse_args()
    if args.command in {"dispatch", "run", "continue"}:
        if args.command == "dispatch":
            method = "dispatch"
            params = {
                "target": args.target,
                "cwd": str(args.cwd),
                "priority": args.priority,
                "prompt": args.prompt,
                "parent_id": args.parent,
                "model": args.model,
                "reasoning_effort": args.reasoning_effort,
            }
        elif args.command == "run":
            method = "run_command"
            params = {
                "label": args.label,
                "cwd": str(args.cwd),
                "shell": args.shell,
                "priority": args.priority,
                "command": args.shell_command,
            }
        else:
            method = "continue_task"
            params = {
                "task_id": args.task_id,
                "text": args.text,
                "idempotency_key": args.idempotency_key,
                "supersede_pending": args.supersede_pending,
            }
        if args.command in {"dispatch", "run"} and args.idempotency_key is not None:
            params["idempotency_key"] = args.idempotency_key
        task = request(
            method,
            params,
        )
        if args.wait > 0:
            deadline = time.monotonic() + args.wait
            while not is_settled(task) and time.monotonic() < deadline:
                time.sleep(1)
                task = request("status", {"task_id": task["id"]})["tasks"][0]
        if args.json:
            print(json.dumps(task, indent=2, sort_keys=True))
        else:
            print_task(task)
        return 0 if task["state"] not in {"failed"} else 1
    if args.command == "status":
        result = request("status", {"task_id": args.task})
        if args.json:
            print(json.dumps(result, indent=2, sort_keys=True))
        elif not result["tasks"]:
            print("No retained tasks.")
        else:
            for task in result["tasks"]:
                print_task(task)
        return 0
    if args.command == "reply":
        task = request("reply", {"task_id": args.task_id, "text": args.text})
    else:
        task = request("request_stop", {"task_id": args.task_id})
    if args.json:
        print(json.dumps(task, indent=2, sort_keys=True))
    else:
        print_task(task)
    return 0


def control_report() -> int:
    parser = argparse.ArgumentParser(prog="control-report", description="Report a worker semantic checkpoint")
    parser.add_argument("--task-id")
    parser.add_argument("--socket")
    parser.add_argument("--state", required=True, choices=("working", "waiting_human", "blocked", "failed", "completed", "cancelled", "unknown"))
    parser.add_argument("--summary", required=True)
    parser.add_argument("--question")
    parser.add_argument("--priority", choices=("routine", "normal", "attention", "critical"))
    args = parser.parse_args()
    task_id = args.task_id or os.environ.get("CONTROL_TASK_ID")
    if not task_id:
        raise RuntimeError("task id is required via --task-id or CONTROL_TASK_ID")
    task = request(
        "report",
        {
            "task_id": task_id,
            "state": args.state,
            "summary": args.summary,
            "question": args.question,
            "priority": args.priority,
        },
        socket_path=args.socket,
    )
    print(f"reported {task['id']} {task['state']}")
    return 0


def main() -> int:
    try:
        if os.environ.get("CONTROL_CLIENT_MODE") == "report" or Path(sys.argv[0]).name == "control-report":
            return control_report()
        return controlctl()
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
