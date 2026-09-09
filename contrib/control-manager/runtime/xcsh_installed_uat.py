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
import os
import sys
import time
import tarfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


CATALOG = Path(__file__).with_name("xcsh-installed-uat") / "scenarios-v1.json"
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


def validate_xcsh_archive_provenance(runtime: dict[str, Any], artifact: dict[str, Any], *, verify_archive: bool) -> None:
    """Bind a launched executable to one regular member of the published asset."""
    archive_path, member = runtime.get("xcsh_archive_path"), runtime.get("xcsh_archive_member")
    member_digest, executable_digest = runtime.get("xcsh_archive_member_sha256"), runtime.get("xcsh_executable_sha256")
    if (not isinstance(archive_path, str) or not archive_path or not isinstance(member, str) or
            not isinstance(member_digest, str) or len(member_digest) != 64 or member_digest.lower() != executable_digest.lower()):
        raise PreflightError("XCSH runtime must bind archive path, exact regular member, and matching member/executable hash")
    if member.startswith("/") or ".." in Path(member).parts or member != "xcsh":
        raise PreflightError("XCSH archive member must be the exact safe regular member xcsh")
    if not verify_archive:
        return
    path = Path(archive_path)
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest().lower() != artifact["sha256"].lower():
        raise PreflightError("local XCSH archive does not match the declared published asset digest")
    try:
        with tarfile.open(path, "r:gz") as archive:
            members = archive.getmembers()
            if any(item.issym() or item.islnk() or item.name.startswith("/") or ".." in Path(item.name).parts for item in members):
                raise PreflightError("XCSH archive contains unsafe link or traversal member")
            matched = [item for item in members if item.name == member]
            if len(matched) != 1 or not matched[0].isreg():
                raise PreflightError("XCSH archive does not contain exactly one required regular xcsh member")
            stream = archive.extractfile(matched[0])
            if stream is None:
                raise PreflightError("XCSH archive member could not be read")
            digest = hashlib.sha256()
            while chunk := stream.read(1024 * 1024): digest.update(chunk)
            if digest.hexdigest().lower() != member_digest.lower():
                raise PreflightError("XCSH archive member hash differs from declared executable provenance")
    except (tarfile.TarError, OSError) as exc:
        raise PreflightError(f"XCSH archive provenance could not be verified: {exc}") from exc


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
    if not isinstance(runtime.get("workspace_id"), str) or not runtime["workspace_id"]:
        raise PreflightError("manifest must bind a dedicated Herdr workspace_id")
    if not isinstance(runtime.get("xcsh_session_dir"), str) or not Path(runtime["xcsh_session_dir"]).is_absolute():
        raise PreflightError("manifest must bind an isolated absolute xcsh_session_dir")
    if not isinstance(runtime.get("xcsh_executable"), str) or not runtime["xcsh_executable"]:
        raise PreflightError("manifest must bind the measured xcsh_executable")
    declared_executable_digest = runtime.get("xcsh_executable_sha256")
    if not isinstance(declared_executable_digest, str) or len(declared_executable_digest) != 64:
        raise PreflightError("manifest must bind measured xcsh_executable_sha256")
    validate_xcsh_archive_provenance(runtime, artifacts["xcsh"], verify_archive=probe)
    fixture = validate_fixture(runtime, verify_file=probe)
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
                              "fixture_sha256": fixture["sha256"],
                              "probe": "not_requested"}
    if probe:
        # Read-only capability check. It intentionally does not start an execution.
        broker = unix_request(Path(runtime["broker_socket"]), "ping", {})
        pong = herdr_request(Path(runtime["herdr_socket"]), "ping", {})
        capabilities = (pong or {}).get("capabilities") or {}
        if int((pong or {}).get("protocol", 0)) < 20 or not capabilities.get("agent_turn_journal"):
            raise PreflightError("installed Herdr lacks protocol-20 agent_turn_journal")
        broker_capabilities = broker.get("capabilities") or {}
        result["probe"] = {"broker": broker.get("status"), "herdr_protocol": pong.get("protocol"),
                           "agent_turn_journal": True, "native_xcsh_admit": bool(broker_capabilities.get(REQUIRED_CAPABILITY))}
        if not broker_capabilities.get(REQUIRED_CAPABILITY):
            raise PreflightError("installed broker lacks required native_xcsh_admit adapter; install the matching manager artifact before live prompt UAT")
        if not broker_capabilities.get("native_turn_consumer") or broker_capabilities.get("native_turn_producer") != "xcsh":
            raise PreflightError("installed broker does not have the XCSH native-turn consumer enabled with producer=xcsh")
        if not isinstance(broker_capabilities.get("native_turn_cursor"), int):
            raise PreflightError("installed broker did not expose a durable native-turn cursor")
        executable = Path(runtime["xcsh_executable"])
        if not executable.is_file() or hashlib.sha256(executable.read_bytes()).hexdigest() != declared_executable_digest:
            raise PreflightError("installed XCSH executable does not match the measured manifest hash")
    return result


def _journal_for_task(socket_path: Path, task_id: str, since: int) -> tuple[list[dict[str, Any]], int]:
    result = herdr_request(socket_path, "agent.turn.list", {"since_revision": since})
    records = (result or {}).get("turns", [])
    matched = [record for record in records if (record.get("report", record) or {}).get("execution_id") == task_id]
    newest = max([since] + [int(record.get("revision", since)) for record in records])
    return matched, newest


def controller_from_manifest(manifest: dict[str, Any]) -> Any:
    """Open only a local receipt made by the dedicated controller preparer."""
    from xcsh_isolated_uat_controller import ControllerError, DisposableHerdrController
    runtime = manifest["runtime"]
    fixture = validate_fixture(runtime, verify_file=True)
    receipt = runtime.get("controller_receipt")
    state_db = runtime.get("controller_state_db")
    if not isinstance(receipt, str) or not isinstance(state_db, str):
        raise PreflightError("execute requires a local authenticated controller receipt and action database")
    try:
        controller = DisposableHerdrController.open_receipt(Path(state_db), Path(receipt))
        herdr = manifest["artifacts"]["herdr"]["sha256"]
        if controller.binary_sha256 != herdr:
            raise ControllerError("controller binary does not match the declared Herdr artifact")
        if controller.ownership.get("herdr_version") != manifest["artifacts"]["herdr"].get("release_version"):
            raise ControllerError("controller runtime version does not match the declared Herdr artifact")
        if (controller.ownership.get("workspace_id") != runtime.get("workspace_id") or
                controller.ownership.get("server_socket") != runtime.get("herdr_socket")):
            raise ControllerError("manifest runtime does not match controller-created ownership receipt")
        controller.verify_live_ownership()
        return controller
    except ControllerError as exc:
        raise PreflightError(f"authenticated isolated controller unavailable: {exc}") from exc


def assert_controller_identity(receipt: dict[str, Any], task: dict[str, Any]) -> None:
    required = {"execution_id": task.get("id"), "workspace_id": task.get("workspace_id"),
                "tab_id": task.get("tab_id"), "pane_id": task.get("pane_id")}
    if any(not isinstance(value, str) or not value for value in required.values()):
        raise PreflightError("broker admission did not expose complete immutable execution provenance")
    if any(receipt.get(key) != value for key, value in required.items()):
        raise PreflightError("controller receipt is not bound to the exact admitted execution")


def execute_case(manifest: dict[str, Any], case: dict[str, Any], *, run_id: str, timeout: float = 90,
                 controller: Any | None = None) -> dict[str, Any]:
    """Drive one real installed XCSH prompt through the atomic broker adapter."""
    runtime = manifest["runtime"]
    controlled_cases = {"reconnect_replay", "generation_supersession", "cleanup", "restart_loss"}
    if case["id"] in controlled_cases and controller is None:
        raise PreflightError(f"{case['id']} requires an authenticated dedicated-runtime controller; shared/default targets are refused")
    broker_socket, herdr_socket = Path(runtime["broker_socket"]), Path(runtime["herdr_socket"])
    prompt = case["prompt"].replace("{fixture_path}", fixture["path"])
    if controller is None:
        raise PreflightError("installed execution requires an authenticated controller-owned XCSH session")
    try:
        session_receipt = controller.create_xcsh_session(
            Path(runtime["xcsh_executable"]), runtime["xcsh_executable_sha256"],
            Path(runtime.get("cwd", "/")),
            Path(runtime["xcsh_session_dir"]) / f"{run_id}-{case['id']}-{uuid.uuid4().hex}",
        )
        session_id = session_receipt["session_id"]
    except Exception as exc:
        raise PreflightError(f"controller could not create a measured XCSH session: {exc}") from exc
    task = unix_request(broker_socket, "native_xcsh_admit", {
        "target": "xcsh-native-uat", "cwd": runtime.get("cwd", "/isolated/xcsh-native-uat"),
        "priority": "routine", "prompt": case["prompt"], "text": prompt,
        "session_id": session_id, "workspace_id": runtime["workspace_id"],
        "runtime_identity": {f"{name}_artifact": artifact_id(manifest["artifacts"][name], name) for name in ("xcsh", "herdr", "manager")},
        "idempotency_key": f"installed-xcsh-uat:{run_id}:{case['id']}",
    })
    if task.get("state") == "unknown":
        raise PreflightError("native_xcsh_admit returned uncertain launch; reuse the same run_id after reconciliation, never create another task")
    if controller is not None:
        try:
            controller.register_execution(task)
        except Exception as exc:
            raise PreflightError(f"controller could not bind exact broker execution admission: {exc}") from exc
    records: list[dict[str, Any]] = []
    cursor, continued, cancelled, controlled = 0, False, False, False
    control_receipt: dict[str, Any] | None = None
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        fresh, cursor = _journal_for_task(herdr_socket, task["id"], cursor)
        records.extend(fresh)
        if fresh:
            applied = unix_request(broker_socket, "consume_native_turns", {}).get("applied", [])
            expected_state = fresh[-1].get("report", fresh[-1]).get("state")
            if not any(item.get("task_id") == task["id"] and item.get("state") in {expected_state, "waiting_human", "unknown"} for item in applied):
                raise PreflightError("native consumer did not apply the observed semantic transition")
        states = [record.get("report", record).get("state") for record in records]
        if states and states[-1] == "waiting_input" and case["id"] in {"continuation", "generation_supersession"} and not continued:
            def continuation() -> dict[str, Any]:
                return unix_request(broker_socket, "continue_task", {"task_id": task["id"], "text": "isolated UAT continuation label"})
            if case["id"] == "generation_supersession":
                control_receipt = controller.real_action("generation_supersession", task["pane_id"],
                    f"{run_id}:{case['id']}:continue", controller.token, continuation)
                assert_controller_identity(control_receipt, task)
                effect = control_receipt.get("effect", {})
                if effect.get("broker_task_id") != task["id"]:
                    raise PreflightError("supersession controller receipt is not bound to the admitted execution")
                duplicate = controller.real_action("generation_supersession", task["pane_id"],
                    f"{run_id}:{case['id']}:continue", controller.token,
                    lambda: (_ for _ in ()).throw(AssertionError("duplicate continuation escaped controller claim")))
                if duplicate != control_receipt:
                    raise PreflightError("duplicate supersession action did not replay its exact durable receipt")
            else:
                continuation()
            continued = True
        if states and states[-1] == "working" and case["id"] == "cancel" and not cancelled:
            unix_request(broker_socket, "request_stop", {"task_id": task["id"]})
            cancelled = True
        if states and states[-1] == "working" and case["id"] == "restart_loss" and not controlled:
            control_receipt = controller.real_action("restart_loss", task["pane_id"],
                f"{run_id}:{case['id']}:restart", controller.token)
            assert_controller_identity(control_receipt, task)
            controlled = True
            socket_after = control_receipt.get("effect", {}).get("after_socket")
            if socket_after != str(herdr_socket):
                herdr_socket = Path(socket_after) if isinstance(socket_after, str) else herdr_socket
            pong = herdr_request(herdr_socket, "ping", {})
            if int((pong or {}).get("protocol", 0)) < 20 or not ((pong or {}).get("capabilities") or {}).get("agent_turn_journal"):
                raise PreflightError("restart controller receipt did not reconnect to protocol-20 journal runtime")
        if states == case["expected_states"] and states[-1] == "waiting_input":
            observed = unix_request(broker_socket, "status", {"task_id": task["id"]})["tasks"][0]
            validate_journal(case, records, observed, None)
            return {"scenario_id": case["id"], "task_id": task["id"], "pass": True, "evidence_class": "installed_waiting_input"}
        if states and states[-1] in {"completed", "failed", "cancelled", "lost", "interrupted"}:
            break
        # agent.turn.wait is an observation-only long poll; it must never be
        # confused with a report injection.
        herdr_request(herdr_socket, "agent.turn.wait", {"after_revision": cursor, "timeout_ms": 1000})
    else:
        raise PreflightError(f"semantic journal timed out for {case['id']}")
    if case["id"] == "reconnect_replay":
        control_receipt = controller.real_action("reconnect_replay", task["pane_id"],
            f"{run_id}:{case['id']}:reconnect", controller.token)
        assert_controller_identity(control_receipt, task)
        if control_receipt.get("effect", {}).get("socket") != str(herdr_socket):
            raise PreflightError("controller reconnect receipt is not for the queried Herdr socket")
        replayed, _ = _journal_for_task(herdr_socket, task["id"], 0)
        if [item.get("revision") for item in replayed] != [item.get("revision") for item in records]:
            raise AssertionError("reconnect replay did not preserve the exact journal sequence")
    # Apply the real journal through the consumer, then wait for the manager's
    # own consumed receipt. The driver never acknowledges consumption itself.
    # No terminal state is inferred here: every journal record was applied
    # incrementally above, before any driver control action.
    consumed: dict[str, Any] | None = None
    observed_task = task
    while time.monotonic() < deadline:
        status = unix_request(broker_socket, "status", {"task_id": task["id"]})
        if status.get("tasks"):
            observed_task = status["tasks"][0]
        for event in status.get("pending_completions", []):
            if event.get("task_id") == task["id"] and event.get("delivery_state") in {"consumed", "response_produced", "client_delivered"}:
                consumed = {"task_id": task["id"], "stage": "consumed"}
                break
        if consumed: break
        time.sleep(.2)
    if not consumed:
        raise PreflightError("manager consumption receipt was not observed; do not self-acknowledge it")
    validate_journal(case, records, observed_task, consumed)
    if case["id"] == "cleanup":
        control_receipt = controller.real_action("cleanup", task["pane_id"],
            f"{run_id}:{case['id']}:cleanup", controller.token)
        assert_controller_identity(control_receipt, task)
        effect = control_receipt.get("effect", {})
        if effect.get("closed_execution_id") != task["id"] or effect.get("closed_tab_id") != task.get("tab_id"):
            raise PreflightError("cleanup receipt did not close the admitted execution tab")
    if case["id"] in controlled_cases and control_receipt is None:
        raise PreflightError("required isolated controller action produced no authoritative receipt")
    return {"scenario_id": case["id"], "task_id": task["id"], "pass": True,
            "controller_receipt": control_receipt, "evidence_class": "installed_runtime_pending_gate"}


def execute(manifest: dict[str, Any], catalog: dict[str, Any], *, run_id: str | None = None,
            controller: Any | None = None) -> dict[str, Any]:
    preflight(manifest, catalog, probe=True)
    controller = controller or controller_from_manifest(manifest)
    run_id = run_id or uuid.uuid4().hex
    cases = [execute_case(manifest, case, run_id=run_id, controller=controller) for case in catalog["scenarios"]]
    return {"run_id": run_id, "pass": all(item["pass"] for item in cases), "accepted": False,
            "cases": cases, "limitation": "A passing run requires separate authoritative installed_runtime and live_uat gate recording."}


def validate_journal(case: dict[str, Any], records: list[dict[str, Any]], task: dict[str, Any], consumed: dict[str, Any] | None) -> None:
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
    expected_generations = case.get("expected_generations")
    if expected_generations is not None and [report.get("generation") for report in reports] != expected_generations:
        raise AssertionError("semantic generation trace differs from scenario oracle")
    for report in reports:
        if report.get("producer") != "xcsh" or not report.get("turn_id") or not isinstance(report.get("event_revision"), int):
            raise AssertionError("journal producer/turn/event revision is invalid")
        if report.get("execution_id") != task["id"] or report.get("pane_id") != task.get("pane_id"):
            raise AssertionError("journal execution/pane provenance mismatch")
        if report.get("session_id") != task.get("agent_session_id"):
            raise AssertionError("journal session provenance mismatch")
    terminal = reports[-1]
    if terminal["state"] == "completed":
        result, digest = terminal.get("result"), terminal.get("result_digest")
        if not isinstance(result, str) or hashlib.sha256(result.encode()).hexdigest() != digest:
            raise AssertionError("completed result digest is invalid")
    elif terminal["state"] in {"failed", "cancelled", "lost", "interrupted"} and not terminal.get("reason"):
        raise AssertionError("non-completed terminal semantic reason is missing")
    if consumed is not None and (consumed.get("task_id") != task["id"] or consumed.get("stage") != "consumed"):
        raise AssertionError("manager consumption receipt is missing or mismatched")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--catalog", type=Path, default=CATALOG)
    parser.add_argument("--probe-installed", action="store_true", help="read-only socket/capability probe")
    parser.add_argument("--execute", action="store_true", help="run only against the approved dedicated installed runtime")
    parser.add_argument("--run-id", help="stable run identity; reuse after an uncertain adapter response")
    args = parser.parse_args()
    try:
        manifest, catalog = load_json(args.manifest), load_json(args.catalog)
        receipt = execute(manifest, catalog, run_id=args.run_id) if args.execute else preflight(manifest, catalog, probe=args.probe_installed)
        print(json.dumps(receipt, indent=2, sort_keys=True))
        return 0
    except PreflightError as exc:
        print(json.dumps({"preflight": "blocked", "live_execution": "not_started", "blocker": str(exc)}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
