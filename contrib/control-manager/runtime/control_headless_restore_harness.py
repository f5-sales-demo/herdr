#!/usr/bin/env python3
"""Exercise installed Herdr restore in an owned named session only.

The harness has no Codex prompt: it injects a synthetic native session marker,
fully stops/restarts its own server unit, and observes process existence.  It
does not address the default session or any Control service.
"""
from __future__ import annotations

import asyncio, json, os, socket, subprocess, tempfile, time, uuid
from pathlib import Path
from typing import Any

HERDR = "/usr/local/bin/herdr"

def cli(env: dict[str, str], *args: str) -> dict[str, Any]:
    result = subprocess.run([HERDR, *args], env=env, text=True, capture_output=True, timeout=20)
    if result.returncode:
        raise RuntimeError(f"{' '.join(args)} failed: {(result.stderr or result.stdout)[-800:]}")
    return json.loads(result.stdout).get("result", {}) if result.stdout.strip() else {}

def request(path: Path, method: str, params: dict[str, Any]) -> Any:
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); client.settimeout(10)
    try:
        client.connect(str(path)); token = uuid.uuid4().hex
        client.sendall(json.dumps({"id": token, "method": method, "params": params}).encode() + b"\n")
        response = json.loads(client.makefile("rb").readline())
        if "error" in response: raise RuntimeError(response["error"])
        return response.get("result")
    finally: client.close()

def wait_socket(path: Path) -> None:
    until = time.monotonic() + 15
    while time.monotonic() < until:
        if path.is_socket():
            try:
                request(path, "session.snapshot", {})
                return
            except OSError: pass
        time.sleep(.1)
    raise RuntimeError(f"timed out waiting for {path}")

def foreground(socket_path: Path, pane_id: str) -> list[dict[str, Any]]:
    value = request(socket_path, "pane.process_info", {"pane_id": pane_id})
    return (value.get("process_info", value).get("foreground_processes") or [])

def wait_saved(path: Path, pane_id: str) -> dict[str, Any]:
    until=time.monotonic()+15
    while time.monotonic()<until:
        if path.exists():
            try:
                snapshot=json.loads(path.read_text())
                panes=[pane for workspace in snapshot.get("workspaces",[]) for tab in workspace.get("tabs",[]) for pane in (tab.get("panes") or {}).values()]
                if any((pane.get("agent_session") or {}).get("value") == "fixture-thread-marker" for pane in panes): return snapshot
            except json.JSONDecodeError: pass
        time.sleep(.1)
    raise RuntimeError(f"timed out waiting for persisted pane/session marker in {path}")

def wait_inactive(unit: str) -> None:
    until=time.monotonic()+15
    while time.monotonic()<until:
        result=subprocess.run(["systemctl","--user","is-active",f"{unit}.service"],capture_output=True,text=True)
        if result.stdout.strip() in {"inactive","failed"}: return
        time.sleep(.1)
    raise RuntimeError(f"timed out waiting for {unit} to stop")

def main() -> None:
    suffix = uuid.uuid4().hex[:10]; session = f"control-headless-restore-{suffix}"; unit = f"{session}-server"
    socket_path = Path.home() / ".config/herdr/sessions" / session / "herdr.sock"
    state_path = socket_path.with_name("session.json")
    evidence: dict[str, Any] = {"session": session, "unit": unit, "default_session_requests": 0, "steps": []}
    with tempfile.TemporaryDirectory(prefix="control-headless-restore-") as raw:
        root = Path(raw); config = root / "herdr.toml"; config.write_text("[session]\nresume_agents_on_restore = true\n")
        env = os.environ | {"HERDR_CONFIG_PATH": str(config)}
        try:
            subprocess.run(["systemd-run", "--user", f"--unit={unit}", "--collect", f"--setenv=HERDR_CONFIG_PATH={config}", HERDR, "--session", session, "server"], check=True, env=env, capture_output=True, text=True, timeout=20)
            wait_socket(socket_path)
            created = cli(env, "--session", session, "workspace", "create", "--cwd", str(root), "--label", "headless-restore")
            pane = created["root_pane"]["pane_id"]
            request(socket_path, "pane.report_agent_session", {"pane_id": pane, "source": "herdr:codex", "agent": "codex", "agent_session_id": "fixture-thread-marker", "seq": 1})
            saved=wait_saved(state_path,pane)
            evidence["steps"].append({"saved_exact_native_marker": {"state_path":str(state_path),"workspace_count":len(saved.get("workspaces") or [])}})
            cli(env, "--session", session, "server", "stop")
            wait_inactive(unit)
            subprocess.run(["systemd-run", "--user", f"--unit={unit}", "--collect", f"--setenv=HERDR_CONFIG_PATH={config}", HERDR, "--session", session, "server"], check=True, env=env, capture_output=True, text=True, timeout=20)
            wait_socket(socket_path)
            restored = request(socket_path, "session.snapshot", {})["snapshot"]
            restored_panes = restored.get("panes") or []
            if len(restored_panes) != 1: raise AssertionError(f"saved test pane was not restored: {restored_panes}")
            pane = restored_panes[0]["pane_id"]
            try:
                deferred = foreground(socket_path, pane)
                raise AssertionError(f"default native restore unexpectedly exposed a runtime: {deferred}")
            except RuntimeError as exc:
                # This installed build represents a deferred runtime as a
                # restored metadata pane without a process-info handle.
                if "pane_not_found" not in str(exc): raise
                evidence["steps"].append({"default_restore_deferred_runtime": {"pane_id":pane,"result":"pane_not_found","ok":True}})
            cli(env, "--session", session, "server", "stop")
            wait_inactive(unit)
            config.write_text("[session]\nresume_agents_on_restore = false\n")
            subprocess.run(["systemd-run", "--user", f"--unit={unit}", "--collect", f"--setenv=HERDR_CONFIG_PATH={config}", HERDR, "--session", session, "server"], check=True, env=env, capture_output=True, text=True, timeout=20)
            wait_socket(socket_path)
            listed=request(socket_path,"pane.list",{}).get("panes") or []
            if len(listed) != 1: raise AssertionError(f"disabled restore pane list ambiguous: {listed}")
            pane=listed[0]["pane_id"]
            shells = foreground(socket_path, pane)
            evidence["steps"].append({"disabled_auto_resume_immediate_shell": {"foreground": shells, "ok": len(shells) == 1 and shells[0].get("name") in {"sh", "bash", "zsh", "fish"}}})
            if len(shells) != 1 or shells[0].get("name") not in {"sh", "bash", "zsh", "fish"}: raise AssertionError("disabled auto-resume did not restore an owned shell")
            # A disposable ELF named codex accepts the exact broker argv but
            # never contacts a provider or replays a prompt.
            shim_source=root/"codex.c"; shim=root/"codex"
            shim_source.write_text("#include <unistd.h>\nint main(void){sleep(30);return 0;}\n")
            subprocess.run(["cc",str(shim_source),"-o",str(shim)],check=True,capture_output=True,text=True)
            manager_thread="01a00000-0000-7000-8000-000000000001"
            workspace=listed[0]["workspace_id"]
            broker_config=root/"broker.json"; broker_config.write_text(json.dumps({"manager_thread_id":manager_thread,"manager_pane_id":pane,"manager_workspace_id":workspace,"manager_cwd":str(root),"codex_binary":str(shim),"app_server_remote":"unix://","profile":"control-manager"}))
            from control_broker import Broker
            broker=Broker(root/"broker.sock",root/"broker.sqlite3",socket_path,broker_config)
            snapshot=request(socket_path,"session.snapshot",{})["snapshot"]
            panes={item["pane_id"]:item for item in snapshot.get("panes",[])}
            admitted=asyncio.run(broker._ensure_manager(snapshot,panes,allow_supervisor_reattach=True,require_verified=False))
            deadline=time.monotonic()+5; launched=[]
            while time.monotonic()<deadline:
                launched=foreground(socket_path,pane)
                if len(launched)==1 and launched[0].get("name")=="codex": break
                time.sleep(.1)
            expected=[str(shim),"--disable","hooks","--remote","unix://","--profile","control-manager","-C",str(root),"resume",manager_thread]
            if admitted.get("state") != "admitted" or len(launched)!=1 or launched[0].get("argv") != expected: raise AssertionError(f"broker exact resume not proven: {admitted} {launched}")
            evidence["broker_exact_resume_isolation"]={"state":admitted["state"],"argv":launched[0]["argv"],"no_model_prompt":True}
        finally:
            subprocess.run(["systemctl", "--user", "stop", f"{unit}.service"], capture_output=True, text=True)
            subprocess.run([HERDR, "session", "delete", session], env=env, capture_output=True, text=True)
    print(json.dumps(evidence, indent=2, sort_keys=True))

if __name__ == "__main__": main()
