#!/usr/bin/env python3
"""Minimal stdio MCP bridge exposing only Control Manager broker operations."""

from __future__ import annotations

import json
import sys
import time
from typing import Any

from control_client import is_settled, request


TOOLS: list[dict[str, Any]] = [
    {
        "name": "completion_inbox",
        "description": "Drain durable pending task completions before progress claims or at turn/reconnect start. Returning an event records delivery to this manager runtime, not cognition or user display.",
        "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True},
        "inputSchema": {
            "type": "object", "additionalProperties": False,
            "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 20}},
        },
    },
    {
        "name": "ack_completion",
        "description": "Acknowledge a delivered completion stage with trusted runtime evidence. Manager may mark consumed after incorporating it; never claim response/client delivery without corresponding app/client evidence.",
        "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True},
        "inputSchema": {
            "type": "object", "additionalProperties": False, "required": ["event_id", "stage"],
            "properties": {
                "event_id": {"type": "string", "maxLength": 80},
                "stage": {"type": "string", "enum": ["consumed"]},
                "evidence_id": {"type": "string", "maxLength": 160},
                "manager_turn_id": {"type": "string", "maxLength": 80},
            },
        },
    },
    {
        "name": "run_command",
        "description": "Run one visible shell/CLI work unit in a no-focus sibling tab under control.",
        "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["label", "cwd", "command"],
            "properties": {
                "label": {"type": "string", "maxLength": 96},
                "idempotency_key": {"type": "string", "minLength": 1, "maxLength": 160, "description": "Stable caller-generated admission key. Reuse the same key and arguments after an uncertain response to retrieve the original task."},
                "cwd": {"type": "string"},
                "shell": {"type": "string", "enum": ["zsh", "bash"], "default": "zsh"},
                "command": {"type": "string", "maxLength": 32000},
                "priority": {"type": "string", "enum": ["routine", "normal", "attention", "critical"], "default": "normal"},
                "wait_seconds": {"type": "integer", "minimum": 0, "maximum": 3600, "default": 90},
            },
        },
    },
    {
        "name": "create_feature",
        "description": "Create a durable SDLC feature lifecycle. scope=plan_only never starts implementation; end_to_end permits only explicitly supplied stage actions. Gate stages require separate authoritative evidence before promotion or acceptance.",
        "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
        "inputSchema": {"type": "object", "additionalProperties": False,
          "required": ["feature_id", "target", "cwd", "title", "scope"],
          "properties": {"feature_id": {"type": "string", "maxLength": 120}, "target": {"type": "string", "maxLength": 64}, "cwd": {"type": "string"}, "title": {"type": "string", "maxLength": 500}, "scope": {"type": "string", "enum": ["plan_only", "end_to_end"]}, "actions": {"type": "object"}, "children": {"type": "object"}, "required_stages": {"type": "array", "items": {"type": "string"}}}},
    },
    {
        "name": "feature_status",
        "description": "Read durable feature stages, children, blockers, claims, and authoritative evidence. A feature is accepted only when every required stage is complete with required gate evidence.",
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True},
        "inputSchema": {"type": "object", "additionalProperties": False, "required": ["feature_id"], "properties": {"feature_id": {"type": "string", "maxLength": 120}}},
    },
    {
        "name": "record_feature_evidence",
        "description": "Record authoritative gate evidence after verifying it. review/ci/merge/release/install/live_uat stages fail closed until their matching evidence kind is supplied.",
        "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True},
        "inputSchema": {"type": "object", "additionalProperties": False, "required": ["feature_id", "stage", "kind", "evidence_id"], "properties": {"feature_id": {"type": "string", "maxLength": 120}, "stage": {"type": "string", "maxLength": 40}, "kind": {"type": "string", "maxLength": 40}, "evidence_id": {"type": "string", "maxLength": 500}}},
    },
    {
        "name": "continue_task",
        "description": "Apply a correction to the exact tracked Codex task. While its turn is active this uses same-turn AppServer steering and never queues future work or increments the run generation. Reuse idempotency_key after response uncertainty; uncertain delivery is retained and never replayed. Set supersede_pending=true only to preserve then remove this task's broker-owned legacy queued followups. Dispatch future independent work as a distinct task or feature stage.",
        "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True},
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["task_id", "text", "idempotency_key"],
            "properties": {
                "task_id": {"type": "string", "maxLength": 80},
                "text": {"type": "string", "maxLength": 16000},
                "idempotency_key": {"type": "string", "minLength": 1, "maxLength": 160, "description": "Stable caller-generated delivery key. Reuse only with identical arguments."},
                "supersede_pending": {"type": "boolean", "default": False, "description": "Explicitly preserve and remove only broker-owned legacy queued followups for this task before steering."},
                "wait_seconds": {"type": "integer", "minimum": 0, "maximum": 3600, "default": 0},
            },
        },
    },
    {
        "name": "dispatch",
        "description": "Dispatch a Codex worker and optionally wait up to 90 seconds for a settled result. For two or more independent sibling tasks, use wait_seconds=0 on every dispatch so all tabs are admitted before any cleanup.",
        "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["target", "cwd", "priority", "prompt"],
            "properties": {
                "target": {
                    "type": "string",
                    "maxLength": 64,
                    "pattern": "^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$",
                    "description": "Stable path-derived slug, never a raw filesystem path; reuse it for the same CWD.",
                },
                "cwd": {"type": "string"},
                "priority": {"type": "string", "enum": ["routine", "normal", "attention", "critical"]},
                "prompt": {"type": "string", "maxLength": 32000},
                "parent_id": {"type": "string", "maxLength": 80},
                "idempotency_key": {"type": "string", "minLength": 1, "maxLength": 160, "description": "Stable caller-generated admission key. Reuse the same key and arguments after an uncertain response to retrieve the original task."},
                "model": {
                    "type": "string",
                    "enum": ["gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"],
                    "default": "gpt-5.6-sol",
                    "description": "Use Sol by default, Terra for balanced coding, or Luna for simple fast work.",
                },
                "reasoning_effort": {
                    "type": "string",
                    "enum": ["low", "medium", "high", "xhigh", "max"],
                    "description": "Optional override; otherwise the selected model's default is used.",
                },
                "wait_seconds": {"type": "integer", "minimum": 0, "maximum": 90, "default": 90},
            },
        },
    },
    {
        "name": "status",
        "description": "Return the retained task digest or one task's current state.",
        "annotations": {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True},
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {"task_id": {"type": "string", "maxLength": 80}},
        },
    },
    {
        "name": "reply",
        "description": "Deliver Robin's answer to a waiting worker so that same task resumes.",
        "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["task_id", "text"],
            "properties": {
                "task_id": {"type": "string", "maxLength": 80},
                "text": {"type": "string", "maxLength": 16000},
            },
        },
    },
    {
        "name": "request_stop",
        "description": "Request a graceful stop for a task; this never force-kills a worker.",
        "annotations": {"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True},
        "inputSchema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["task_id"],
            "properties": {"task_id": {"type": "string", "maxLength": 80}},
        },
    },
]


def call_tool(name: str, arguments: dict[str, Any]) -> Any:
    if name == "completion_inbox":
        return request("completion_inbox", {"limit": arguments.get("limit", 20)})
    if name == "ack_completion":
        return request("ack_completion", {
            "event_id": arguments.get("event_id"), "stage": arguments.get("stage"),
            "source": "manager_mcp", "evidence_id": arguments.get("evidence_id"),
            "manager_turn_id": arguments.get("manager_turn_id"),
        })
    if name == "status":
        return request("status", {"task_id": arguments.get("task_id")})
    if name in {"create_feature", "feature_status", "record_feature_evidence"}:
        return request(name, arguments)
    if name == "reply":
        return request("reply", {"task_id": arguments.get("task_id"), "text": arguments.get("text")})
    if name == "request_stop":
        return request("request_stop", {"task_id": arguments.get("task_id")})
    if name in {"dispatch", "run_command", "continue_task"}:
        wait_seconds = arguments.get("wait_seconds", 0 if name == "continue_task" else 90)
        if not isinstance(wait_seconds, int) or not 0 <= wait_seconds <= 3600:
            raise ValueError("wait_seconds must be an integer from 0 through 3600")
        if name == "dispatch":
            params = {
                "target": arguments.get("target"),
                "cwd": arguments.get("cwd"),
                "priority": arguments.get("priority"),
                "prompt": arguments.get("prompt"),
                "parent_id": arguments.get("parent_id"),
                "model": arguments.get("model", "gpt-5.6-sol"),
                "reasoning_effort": arguments.get("reasoning_effort"),
            }
        elif name == "run_command":
            params = {
                "label": arguments.get("label"),
                "cwd": arguments.get("cwd"),
                "shell": arguments.get("shell", "zsh"),
                "priority": arguments.get("priority", "normal"),
                "command": arguments.get("command"),
            }
        else:
            params = {
                "task_id": arguments.get("task_id"),
                "text": arguments.get("text"),
                "idempotency_key": arguments.get("idempotency_key"),
                "supersede_pending": arguments.get("supersede_pending", False),
            }
        if name in {"dispatch", "run_command"} and "idempotency_key" in arguments:
            params["idempotency_key"] = arguments["idempotency_key"]
        task = request(name, params)
        deadline = time.monotonic() + wait_seconds
        while not is_settled(task) and time.monotonic() < deadline:
            time.sleep(1)
            task = request("status", {"task_id": task["id"]})["tasks"][0]
        return task
    raise ValueError(f"unknown tool {name!r}")


def respond(request_id: Any, result: Any = None, error: dict[str, Any] | None = None) -> None:
    message: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id}
    if error is None:
        message["result"] = result
    else:
        message["error"] = error
    sys.stdout.write(json.dumps(message, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def main() -> int:
    for raw in sys.stdin:
        try:
            message = json.loads(raw)
            method = message.get("method")
            request_id = message.get("id")
            if request_id is None:
                continue
            if method == "initialize":
                respond(
                    request_id,
                    {
                        "protocolVersion": message.get("params", {}).get("protocolVersion", "2025-06-18"),
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "control-broker", "version": "1.0.0"},
                    },
                )
            elif method == "ping":
                respond(request_id, {})
            elif method == "tools/list":
                respond(request_id, {"tools": TOOLS})
            elif method == "tools/call":
                params = message.get("params") or {}
                arguments = params.get("arguments") or {}
                if not isinstance(arguments, dict):
                    raise ValueError("tool arguments must be an object")
                value = call_tool(str(params.get("name", "")), arguments)
                encoded = json.dumps(value, indent=2, sort_keys=True)
                respond(
                    request_id,
                    {
                        "content": [{"type": "text", "text": encoded}],
                        "structuredContent": {"result": value},
                        "isError": False,
                    },
                )
            else:
                respond(request_id, error={"code": -32601, "message": f"method not found: {method}"})
        except Exception as exc:
            if "request_id" in locals() and request_id is not None:
                respond(
                    request_id,
                    {
                        "content": [{"type": "text", "text": f"Control broker error: {exc}"}],
                        "isError": True,
                    },
                )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
