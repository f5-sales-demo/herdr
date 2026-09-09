#!/usr/bin/env python3
"""Create and prompt tracked Control Manager worker threads via app-server."""

from __future__ import annotations

import argparse
import json

from appserver_manager import AppServer, AppServerResponseError


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
    prompt.add_argument("--client-user-message-id")
    steer = sub.add_parser("steer")
    steer.add_argument("--thread-id", required=True)
    steer.add_argument("--expected-turn-id", required=True)
    steer.add_argument("--client-user-message-id", required=True)
    steer.add_argument("--text", required=True)
    queue_list = sub.add_parser("queue-list")
    queue_list.add_argument("--thread-id", required=True)
    queue_delete = sub.add_parser("queue-delete")
    queue_delete.add_argument("--thread-id", required=True)
    queue_delete.add_argument("--queued-submission-id", required=True)
    status = sub.add_parser("status")
    status.add_argument("--thread-id", required=True)
    status.add_argument("--turn-id")
    args = parser.parse_args()

    server = AppServer()
    try:
        if args.command == "status":
            result = server.request(
                "thread/read", {"threadId": args.thread_id, "includeTurns": False}
            )
            thread = result["thread"]
            turns = server.request(
                "thread/turns/list",
                {
                    "threadId": args.thread_id,
                    "limit": 1,
                    "sortDirection": "desc",
                    "itemsView": "notLoaded",
                },
            ).get("data") or []
            latest = turns[0] if turns else {}
            if args.turn_id and latest.get("id") != args.turn_id:
                latest = {}
            output = {
                "thread_id": thread["id"],
                "thread_status": thread.get("status"),
                "turn_id": latest.get("id"),
                "turn_status": latest.get("status"),
            }
        elif args.command == "queue-list":
            submissions = []
            cursor = None
            while True:
                params = {"threadId": args.thread_id, "limit": 100}
                if cursor is not None:
                    params["cursor"] = cursor
                page = server.request("thread/queue/list", params)
                data = page.get("data") or []
                if not isinstance(data, list):
                    raise RuntimeError("app-server returned an invalid thread queue page")
                submissions.extend(data)
                if len(submissions) > 1000:
                    raise RuntimeError("thread queue exceeds the bounded reconciliation limit")
                cursor = page.get("nextCursor")
                if not cursor:
                    break
            output = {"thread_id": args.thread_id, "submissions": submissions}
        elif args.command == "queue-delete":
            result = server.request(
                "thread/queue/delete",
                {
                    "threadId": args.thread_id,
                    "queuedSubmissionId": args.queued_submission_id,
                },
            )
            output = {
                "thread_id": args.thread_id,
                "queued_submission_id": args.queued_submission_id,
                "deleted": result.get("deleted") is True,
            }
        elif args.command == "steer":
            metadata = server.request(
                "thread/read", {"threadId": args.thread_id, "includeTurns": False}
            )["thread"]
            recent = server.request(
                "thread/turns/list",
                {
                    "threadId": args.thread_id,
                    "limit": 1,
                    "sortDirection": "desc",
                    "itemsView": "notLoaded",
                },
            )
            turns = recent.get("data") or []
            current = turns[0] if turns else {}
            if metadata.get("id") != args.thread_id:
                raise RuntimeError("app-server returned a different worker thread")
            if current.get("id") != args.expected_turn_id or current.get("status") != "inProgress":
                output = {
                    "thread_id": args.thread_id,
                    "expected_turn_id": args.expected_turn_id,
                    "authoritative_turn_id": current.get("id"),
                    "authoritative_turn_status": current.get("status"),
                    "delivery": "rejected",
                    "reason": "the expected turn is no longer the active turn",
                }
            else:
                try:
                    result = server.request(
                        "turn/steer",
                        {
                            "threadId": args.thread_id,
                            "expectedTurnId": args.expected_turn_id,
                            "clientUserMessageId": args.client_user_message_id,
                            "input": [{"type": "text", "text": args.text}],
                        },
                    )
                except AppServerResponseError as exc:
                    output = {
                        "thread_id": args.thread_id,
                        "expected_turn_id": args.expected_turn_id,
                        "delivery": "rejected",
                        "reason": str(exc)[:1000],
                    }
                else:
                    if result.get("turnId") != args.expected_turn_id:
                        raise RuntimeError("app-server acknowledged steering on a different turn")
                    output = {
                        "thread_id": args.thread_id,
                        "turn_id": result["turnId"],
                        "client_user_message_id": args.client_user_message_id,
                        "delivery": "accepted",
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
            turn_params = {
                "threadId": args.thread_id,
                "model": args.model,
                "effort": args.reasoning_effort,
                "input": [{"type": "text", "text": args.text}],
            }
            if args.client_user_message_id:
                turn_params["clientUserMessageId"] = args.client_user_message_id
            result = server.request(
                "turn/start",
                turn_params,
            )
            output = {"thread_id": args.thread_id, "turn_id": result["turn"]["id"]}
        print(json.dumps(output, separators=(",", ":")))
        return 0
    finally:
        server.close()


if __name__ == "__main__":
    raise SystemExit(main())
