#!/usr/bin/env python3
"""Create and prompt tracked Control Manager worker threads via app-server."""

from __future__ import annotations

import argparse
import json

from appserver_manager import AppServer


def worker_config(
    task_id: str,
    broker_socket: str,
    parent_id: str,
    model: str,
    reasoning_effort: str,
) -> dict:
    return {
        "default_permissions": ":danger-full-access",
        "model": model,
        "model_reasoning_effort": reasoning_effort,
        "features": {
            "hooks": False,
            "multi_agent": False,
        },
        "shell_environment_policy": {
            "inherit": "all",
            "set": {
                "CONTROL_TASK_ID": task_id,
                "CONTROL_BROKER_SOCKET": broker_socket,
                "CONTROL_PARENT_ID": parent_id,
            },
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("create")
    create.add_argument("--cwd", required=True)
    create.add_argument("--task-id", required=True)
    create.add_argument("--broker-socket", required=True)
    create.add_argument("--parent-id", default="")
    create.add_argument("--model", required=True)
    create.add_argument("--reasoning-effort", required=True)
    create.add_argument("--name", required=True)
    create.add_argument("--text", required=True)
    prompt = sub.add_parser("prompt")
    prompt.add_argument("--thread-id", required=True)
    prompt.add_argument("--task-id", required=True)
    prompt.add_argument("--broker-socket", required=True)
    prompt.add_argument("--parent-id", default="")
    prompt.add_argument("--model", required=True)
    prompt.add_argument("--reasoning-effort", required=True)
    prompt.add_argument("--text", required=True)
    status = sub.add_parser("status")
    status.add_argument("--thread-id", required=True)
    status.add_argument("--turn-id")
    args = parser.parse_args()

    server = AppServer()
    try:
        if args.command == "status":
            result = server.request(
                "thread/read", {"threadId": args.thread_id, "includeTurns": True}
            )
            thread = result["thread"]
            turns = thread.get("turns") or []
            latest = turns[-1] if turns else {}
            if args.turn_id:
                latest = next((turn for turn in turns if turn.get("id") == args.turn_id), {})
            output = {
                "thread_id": thread["id"],
                "thread_status": thread.get("status"),
                "turn_id": latest.get("id"),
                "turn_status": latest.get("status"),
            }
        elif args.command == "create":
            config = worker_config(
                args.task_id,
                args.broker_socket,
                args.parent_id,
                args.model,
                args.reasoning_effort,
            )
            result = server.request(
                "thread/start",
                {
                    "cwd": args.cwd,
                    "approvalPolicy": "never",
                    "model": args.model,
                    "config": config,
                    "serviceName": "control_worker",
                    "threadSource": "appServer",
                },
            )
            thread = result["thread"]
            server.request("thread/name/set", {"threadId": thread["id"], "name": args.name})
            server.request(
                "thread/settings/update",
                {
                    "threadId": thread["id"],
                    "model": args.model,
                    "effort": args.reasoning_effort,
                },
            )
            started = server.request(
                "turn/start",
                {
                    "threadId": thread["id"],
                    "model": args.model,
                    "effort": args.reasoning_effort,
                    "input": [{"type": "text", "text": args.text}],
                },
            )
            output = {
                "thread_id": thread["id"],
                "turn_id": started["turn"]["id"],
                "cwd": thread.get("cwd"),
            }
        else:
            config = worker_config(
                args.task_id,
                args.broker_socket,
                args.parent_id,
                args.model,
                args.reasoning_effort,
            )
            server.request(
                "thread/resume",
                {
                    "threadId": args.thread_id,
                    "approvalPolicy": "never",
                    "model": args.model,
                    "config": config,
                    "excludeTurns": True,
                },
            )
            server.request(
                "thread/settings/update",
                {
                    "threadId": args.thread_id,
                    "model": args.model,
                    "effort": args.reasoning_effort,
                },
            )
            result = server.request(
                "turn/start",
                {
                    "threadId": args.thread_id,
                    "model": args.model,
                    "effort": args.reasoning_effort,
                    "input": [{"type": "text", "text": args.text}],
                },
            )
            output = {"thread_id": args.thread_id, "turn_id": result["turn"]["id"]}
        print(json.dumps(output, separators=(",", ":")))
        return 0
    finally:
        server.close()


if __name__ == "__main__":
    raise SystemExit(main())
