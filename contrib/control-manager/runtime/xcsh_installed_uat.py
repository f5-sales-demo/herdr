#!/usr/bin/env python3
"""Installed-runtime UAT driver for real XCSH semantic-turn acceptance.

Dry-run/preflight validates a signed/immutable artifact binding and refuses to
launch anything.  Live execution is deliberately unavailable until the
installed broker exposes ``native_xcsh_admit``: that adapter must atomically
admit a broker ``work_kind=xcsh`` task and start the matching Herdr execution.
The driver never calls ``agent.turn.report`` and therefore cannot inject its
own terminal evidence or substitute a fake agent.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import socket
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from control_portable import package_root


CATALOG = package_root() / "xcsh-installed-uat" / "scenarios-v1.json"
SCHEMA_VERSION = 1
REQUIRED_CAPABILITY = "native_xcsh_admit"
ALLOWED_HERDR_METHODS = {"ping", "agent.turn.wait", "agent.turn.list", "execution.cancel"}


class PreflightError(RuntimeError):
    pass


def unix_request(path: Path, method: str, params: dict[str, Any]) -> Any:
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.settimeout(15)
    try:
        client.connect(str(path))
        client.sendall(json.dumps({"method": method, "params": params}, separators=(",", ":")).encode() + b"\n")
        line = client.makefile("rb").readline(1_000_001)
    finally:
        client.close()
    if not line:
        raise RuntimeError("broker closed without a response")
    reply = json.loads(line)
    if not reply.get("ok"):
        raise RuntimeError(reply.get("error", "broker request failed"))
    return reply["result"]


def herdr_request(path: Path, method: str, params: dict[str, Any]) -> Any:
    if method not in ALLOWED_HERDR_METHODS:
        raise RuntimeError(f"UAT driver refuses unsupported Herdr method {method}")
    request_id = f"installed-uat-{uuid.uuid4().hex}"
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.settimeout(15)
    try:
        client.connect(str(path))
        client.sendall(json.dumps({"id": request_id, "method": method, "params": params}, separators=(",", ":")).encode() + b"\n")
        reader = client.makefile("rb")
        while True:
            line = reader.readline(1_000_001)
            if not line:
                raise RuntimeError("Herdr closed without a response")
            reply = json.loads(line)
            if reply.get("id") != request_id:
                continue
            if "error" in reply:
                raise RuntimeError((reply["error"] or {}).get("message", "Herdr request failed"))
            return reply.get("result")
    finally:
        client.close()


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise PreflightError(f"invalid JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise PreflightError(f"{path} must contain an object")
    return value


def artifact_id(component: dict[str, Any], name: str) -> str:
    required = ("release_version", "artifact_uri", "sha256")
    if any(not isinstance(component.get(key), str) or not component[key] for key in required):
        raise PreflightError(f"{name} lacks immutable release_version/artifact_uri/sha256")
    if component["release_version"] == "REQUIRED" or component["artifact_uri"] == "REQUIRED":
        raise PreflightError(f"{name} still contains a template artifact identity")
    digest = component["sha256"]
    if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest.lower()):
        raise PreflightError(f"{name} sha256 is invalid")
    return f"{name}@{component['release_version']}#{digest[:12]}"


def preflight(manifest: dict[str, Any], catalog: dict[str, Any], *, probe: bool = False) -> dict[str, Any]:
    """Validate immutable installed-artifact prerequisites without dispatching prompts."""
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise PreflightError("unsupported installed-UAT manifest schema")
    if manifest.get("isolation") != "dedicated_disposable_runtime":
        raise PreflightError("installed UAT requires a dedicated disposable runtime")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict):
        raise PreflightError("manifest artifacts object is required")
    identities = {name: artifact_id(artifacts.get(name, {}), name) for name in ("xcsh", "herdr", "manager")}
    runtime = manifest.get("runtime")
    if not isinstance(runtime, dict):
        raise PreflightError("manifest runtime object is required")
    if not isinstance(runtime.get("broker_socket"), str) or not isinstance(runtime.get("herdr_socket"), str):
        raise PreflightError("manifest must bind isolated broker_socket and herdr_socket")
    required = set(manifest.get("required_capabilities", []))
    if REQUIRED_CAPABILITY not in required:
        raise PreflightError(f"manifest must require {REQUIRED_CAPABILITY}")
    scenarios = catalog.get("scenarios")
    if catalog.get("catalog_version") != 1 or not isinstance(scenarios, list) or not scenarios:
        raise PreflightError("invalid installed prompt catalog")
    names = {case.get("id") for case in scenarios}
    expected = {"success", "failure", "waiting_input", "cancel", "continuation", "reconnect_replay", "generation_supersession", "cleanup", "restart_loss"}
    if names != expected:
        raise PreflightError("catalog does not cover every required semantic boundary")
    result: dict[str, Any] = {"preflight": "passed", "live_execution": "not_started", "artifact_identities": identities,
                              "required_capabilities": sorted(required), "scenario_ids": sorted(names),
                              "probe": "not_requested"}
    if probe:
        # Read-only capability check. It intentionally does not start an execution.
        broker = unix_request(Path(runtime["broker_socket"]), "ping", {})
        pong = herdr_request(Path(runtime["herdr_socket"]), "ping", {})
        capabilities = (pong or {}).get("capabilities") or {}
        if int((pong or {}).get("protocol", 0)) < 20 or not capabilities.get("agent_turn_journal"):
            raise PreflightError("installed Herdr lacks protocol-20 agent_turn_journal")
        result["probe"] = {"broker": broker.get("status"), "herdr_protocol": pong.get("protocol"),
                           "agent_turn_journal": True, "native_xcsh_admit": False}
        # The existing manager intentionally lacks this admission API. Report it
        # as a concrete prerequisite instead of calling execution.start directly.
        raise PreflightError("installed broker lacks required native_xcsh_admit adapter; live prompt UAT cannot safely admit a correlated XCSH task")
    return result


def validate_journal(case: dict[str, Any], records: list[dict[str, Any]], task: dict[str, Any], consumed: dict[str, Any]) -> None:
    """Oracle for real journal observations; no prose/sentinel inference."""
    if not records:
        raise AssertionError("no real semantic journal records")
    expected_states = case["expected_states"]
    reports = [record.get("report", record) for record in records]
    revisions = [record.get("revision") for record in records]
    if revisions != sorted(set(revisions)):
        raise AssertionError("journal revisions are not strictly monotonic/deduplicated")
    if [report.get("state") for report in reports] != expected_states:
        raise AssertionError("semantic state trace differs from scenario oracle")
    for report in reports:
        if report.get("execution_id") != task["id"] or report.get("pane_id") != task.get("pane_id"):
            raise AssertionError("journal execution/pane provenance mismatch")
        if report.get("session_id") != task.get("agent_session_id"):
            raise AssertionError("journal session provenance mismatch")
    terminal = reports[-1]
    if terminal["state"] == "completed":
        result, digest = terminal.get("result"), terminal.get("result_digest")
        if not isinstance(result, str) or hashlib.sha256(result.encode()).hexdigest() != digest:
            raise AssertionError("completed result digest is invalid")
    if consumed.get("task_id") != task["id"] or consumed.get("stage") != "consumed":
        raise AssertionError("manager consumption receipt is missing or mismatched")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--catalog", type=Path, default=CATALOG)
    parser.add_argument("--probe-installed", action="store_true", help="read-only socket/capability probe")
    parser.add_argument("--execute", action="store_true", help="reserved for the future installed native_xcsh_admit adapter")
    args = parser.parse_args()
    if args.execute:
        raise SystemExit("refusing live execution: native_xcsh_admit adapter is not implemented; use preflight only")
    try:
        receipt = preflight(load_json(args.manifest), load_json(args.catalog), probe=args.probe_installed)
        print(json.dumps(receipt, indent=2, sort_keys=True))
        return 0
    except PreflightError as exc:
        print(json.dumps({"preflight": "blocked", "live_execution": "not_started", "blocker": str(exc)}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
