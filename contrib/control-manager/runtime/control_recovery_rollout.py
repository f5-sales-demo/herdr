#!/usr/bin/env python3
"""Plan or explicitly apply the recovery-supervisor ownership handoff.

This tool is inert without ``--apply``. The known competing owner is the
remote-control health timer; it must be disabled before guarded automatic
recovery becomes the exclusive owner. A receipt enables rollback.
"""
from __future__ import annotations
import argparse, base64, json, os, re, subprocess, tempfile, time, tomllib
from pathlib import Path
from typing import Any

NAME = re.compile(r"[A-Za-z0-9_.@-]{1,160}(?:\.(?:service|timer))?$")

def systemctl(*args: str) -> dict[str, Any]:
    argv=["systemctl","--user",*args]
    try: result=subprocess.run(argv,text=True,capture_output=True,check=False,timeout=15)
    except subprocess.TimeoutExpired as exc:
        return {"argv":argv,"returncode":124,"stdout":str(exc.stdout or "")[-2000:],"stderr":"systemctl exceeded 15-second deadline"}
    return {"argv":argv,"returncode":result.returncode,"stdout":result.stdout[-2000:],"stderr":result.stderr[-2000:]}

def unit_state(kind: str, unit: str) -> bool:
    result=systemctl(kind,unit); value=result["stdout"].strip().lower()
    if result["returncode"] == 0 and value in {"enabled","enabled-runtime","linked","linked-runtime","active"}: return True
    false_values={"disabled","masked","masked-runtime","static","indirect","generated","transient"} if kind == "is-enabled" else {"inactive","failed","activating","deactivating"}
    if value in false_values: return False
    raise RuntimeError(f"cannot determine {kind} state for {unit}: rc={result['returncode']} {result['stderr']}")
def enabled(unit: str) -> bool: return unit_state("is-enabled",unit)
def active(unit: str) -> bool: return unit_state("is-active",unit)

def write_config(path: Path, config: dict[str,Any]) -> None:
    write_receipt(path,config)

def herdr_config_path(config: dict[str, Any]) -> Path:
    """Return the explicit, portable machine binding for Herdr's TOML config."""
    value = config.get("herdr_config_path")
    if not isinstance(value, str) or not value:
        raise ValueError("guarded handoff requires explicit herdr_config_path")
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise ValueError("herdr_config_path must be absolute")
    return path

def capture_herdr_config(path: Path) -> dict[str, Any]:
    existed = path.exists()
    original = path.read_bytes() if existed else b""
    if existed:
        tomllib.loads(original.decode("utf-8"))
    return {"path": str(path), "existed": existed, "mode": path.stat().st_mode & 0o777 if existed else None,
            "original_b64": base64.b64encode(original).decode()}

def set_resume_agents_on_restore(path: Path, value: bool, prior: dict[str, Any] | None = None) -> dict[str, Any]:
    """Change only the documented [session] key, retaining all other TOML text."""
    captured = prior or capture_herdr_config(path)
    existed = bool(captured["existed"])
    original = base64.b64decode(str(captured["original_b64"]), validate=True)
    text = original.decode("utf-8")
    lines = text.splitlines(keepends=True)
    section = None; session_end = None
    changed = False
    for index, line in enumerate(lines):
        match = re.match(r"^\s*\[([^]]+)\]\s*(?:#.*)?$", line)
        if match:
            if section == "session" and session_end is None: session_end = index
            section = match.group(1).strip()
            continue
        if section == "session" and re.match(r"^\s*resume_agents_on_restore\s*=", line):
            newline = "\n" if line.endswith("\n") else ""
            lines[index] = f"resume_agents_on_restore = {'true' if value else 'false'}{newline}"
            changed = True
            break
    if not changed:
        if lines and not lines[-1].endswith("\n"): lines[-1] += "\n"
        if session_end is not None: lines.insert(session_end, f"resume_agents_on_restore = {'true' if value else 'false'}\n")
        elif section == "session": lines.append(f"resume_agents_on_restore = {'true' if value else 'false'}\n")
        else: lines.extend(["\n" if lines else "", "[session]\n", f"resume_agents_on_restore = {'true' if value else 'false'}\n"])
    rendered = "".join(lines)
    tomllib.loads(rendered)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(rendered); stream.flush(); os.fsync(stream.fileno())
        os.replace(name, path)
        if existed and captured.get("mode") is not None: os.chmod(path, int(captured["mode"]))
    finally:
        if os.path.exists(name): os.unlink(name)
    return captured

def restore_herdr_config(prior: dict[str, Any]) -> None:
    binding = prior.get("herdr_config")
    if not isinstance(binding, dict): return
    path = Path(str(binding.get("path") or ""))
    if not path.is_absolute(): raise ValueError("invalid prior Herdr config path")
    if binding.get("existed"):
        payload = base64.b64decode(str(binding.get("original_b64") or ""), validate=True)
        fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(payload); stream.flush(); os.fsync(stream.fileno())
            os.replace(name, path)
            if binding.get("mode") is not None: os.chmod(path, int(binding["mode"]))
        finally:
            if os.path.exists(name): os.unlink(name)
    elif path.exists():
        path.unlink()

def load_config(path: Path) -> dict[str, Any]:
    try: value=json.loads(path.read_text())
    except (OSError,json.JSONDecodeError) as exc: raise ValueError(f"invalid machine config: {exc}") from exc
    if not isinstance(value,dict): raise ValueError("machine config must be an object")
    return value

def safe_name(value: str, label: str) -> str:
    if not NAME.fullmatch(value): raise ValueError(f"invalid {label}")
    return value

def make_plan(config: dict[str, Any], supervisor: str, competing_timer: str) -> dict[str, Any]:
    mode=str(config.get("supervisor_mode","observation_only"))
    if mode not in {"observation_only","guarded_live"}: raise ValueError("supervisor_mode must be observation_only or guarded_live")
    return {"mode":mode,"supervisor_unit":supervisor,"competing_timer":competing_timer,
            "observation_install":["systemctl","--user","start",supervisor],
            "guarded_handoff":[["systemctl","--user","disable","--now",competing_timer],["systemctl","--user","restart",supervisor]],
            "rollback":[["systemctl","--user","stop",supervisor],["systemctl","--user","enable","--now",competing_timer]],
            "note":"No command is run unless --apply is supplied. Guarded handoff requires explicit config gates."}

def write_receipt(path: Path, receipt: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True,exist_ok=True)
    fd,name=tempfile.mkstemp(prefix=f".{path.name}.",dir=path.parent)
    try:
        with os.fdopen(fd,"w") as stream:
            stream.write(json.dumps(receipt,indent=2,sort_keys=True)+"\n"); stream.flush(); os.fsync(stream.fileno())
        os.chmod(name,0o600); os.replace(name,path)
        parent=os.open(path.parent,os.O_RDONLY); os.fsync(parent); os.close(parent)
    finally:
        if os.path.exists(name): os.unlink(name)

def apply_observation(config: dict[str, Any], supervisor: str) -> list[dict[str, Any]]:
    if config.get("supervisor_mode","observation_only") != "observation_only": raise ValueError("observation install requires supervisor_mode=observation_only")
    # Start only: startup enablement remains a separately authorized install gate.
    actions=[systemctl("start",supervisor)]
    if actions[-1]["returncode"] != 0: raise RuntimeError("observation supervisor failed to start")
    return actions

def apply_guarded(config: dict[str, Any], supervisor: str, timer: str, receipt_path: Path, config_path: Path | None = None) -> list[dict[str, Any]]:
    if config_path is None: raise ValueError("guarded handoff requires explicit runtime config path")
    desired=config.get("guarded_live_desired") or {}
    if desired != {"supervisor_mode":"guarded_live","recovery_live_enabled":True,"supervisor_owns_recovery":True}: raise ValueError("guarded handoff requires exact guarded_live_desired settings")
    herdr_path=herdr_config_path(config)
    prior_herdr=capture_herdr_config(herdr_path)
    receipt={"version":3,"created_at":time.time(),"supervisor_unit":supervisor,"competing_timer":timer,
             "config_path":str(config_path.resolve()),"prior":{"competing_timer_enabled":enabled(timer),"competing_timer_active":active(timer),"supervisor_active":active(supervisor),
             "runtime_config":{key:config.get(key) for key in ("supervisor_mode","recovery_live_enabled","supervisor_owns_recovery")},"herdr_config":prior_herdr},"actions":[],"state":"prepared"}
    write_receipt(receipt_path,receipt)
    receipt["actions"].append(systemctl("stop",supervisor))
    if receipt["actions"][-1]["returncode"] != 0:
        receipt["state"]="handoff_failed"; write_receipt(receipt_path,receipt)
        raise RuntimeError("supervisor could not be quiesced before ownership handoff")
    staged=dict(config); staged.update(desired); write_config(config_path,staged)
    set_resume_agents_on_restore(herdr_path, False, prior_herdr)
    write_receipt(receipt_path,receipt)
    receipt["actions"].append(systemctl("disable","--now",timer))
    if receipt["actions"][-1]["returncode"] != 0:
        receipt["state"]="rolling_back"; write_receipt(receipt_path,receipt)
        receipt["rollback_actions"]=restore_prior(receipt)
        receipt["state"]="rollback_failed" if any(item["returncode"] != 0 for item in receipt["rollback_actions"]) else "rolled_back"; write_receipt(receipt_path,receipt)
        if receipt["state"] == "rollback_failed": raise RuntimeError("timer disable and automatic rollback both failed; inspect receipt")
        raise RuntimeError("competing recovery timer could not be disabled; prior config and unit state were restored")
    receipt["actions"].append(systemctl("restart",supervisor)); write_receipt(receipt_path,receipt)
    if receipt["actions"][-1]["returncode"] != 0:
        receipt["state"]="rolling_back"; write_receipt(receipt_path,receipt)
        receipt["rollback_actions"]=restore_prior(receipt)
        receipt["state"]="rollback_failed" if any(item["returncode"] != 0 for item in receipt["rollback_actions"]) else "rolled_back"; write_receipt(receipt_path,receipt)
        if receipt["state"] == "rollback_failed": raise RuntimeError("supervisor restart and automatic rollback both failed; inspect receipt")
        raise RuntimeError("supervisor restart failed after timer handoff; prior unit state was restored")
    receipt["state"]="applied"; write_receipt(receipt_path,receipt)
    return receipt["actions"]

def restore_prior(receipt: dict[str,Any]) -> list[dict[str,Any]]:
    supervisor=safe_name(str(receipt.get("supervisor_unit","")),"supervisor unit"); timer=safe_name(str(receipt.get("competing_timer","")),"competing timer")
    prior=receipt.get("prior") or {}
    actions=[systemctl("stop",supervisor)]
    # Never create dual owners: until the current supervisor is conclusively
    # quiesced, runtime config and the prior timer remain untouched.
    if actions[0]["returncode"] != 0: return actions
    config_path=receipt.get("config_path"); prior_config=prior.get("runtime_config")
    if config_path and isinstance(prior_config,dict):
        current=load_config(Path(config_path)); current.update(prior_config); current.pop("guarded_live_desired",None); write_config(Path(config_path),current)
    restore_herdr_config(prior)
    actions.append(systemctl("enable" if prior.get("competing_timer_enabled") else "disable",timer))
    actions.append(systemctl("start" if prior.get("competing_timer_active") else "stop",timer))
    actions.append(systemctl("start" if prior.get("supervisor_active") else "stop",supervisor))
    return actions

def rollback(receipt_path: Path) -> list[dict[str, Any]]:
    try: receipt=json.loads(receipt_path.read_text())
    except (OSError,json.JSONDecodeError) as exc: raise ValueError(f"invalid rollback receipt: {exc}") from exc
    receipt["state"]="rolling_back"; write_receipt(receipt_path,receipt)
    actions=restore_prior(receipt); receipt["rollback_actions"]=actions
    if any(item["returncode"] != 0 for item in actions):
        receipt["state"]="rollback_failed"; write_receipt(receipt_path,receipt)
        raise RuntimeError("rollback did not restore every prior unit state")
    receipt["state"]="rolled_back"; write_receipt(receipt_path,receipt)
    return actions

def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__); parser.add_argument("command",choices=("plan","apply-observation","apply-guarded","rollback")); parser.add_argument("--config",type=Path,required=True); parser.add_argument("--supervisor-unit",default="control-supervisor.service"); parser.add_argument("--competing-timer",default="codex-remote-control-health.timer"); parser.add_argument("--receipt",type=Path,required=True); parser.add_argument("--apply",action="store_true")
    args=parser.parse_args(); config=load_config(args.config) if args.command != "rollback" else {}
    supervisor=safe_name(args.supervisor_unit,"supervisor unit"); timer=safe_name(args.competing_timer,"competing timer")
    if args.command == "plan": result=make_plan(config,supervisor,timer)
    elif not args.apply: result={"state":"waiting_human","reason":"--apply is required for systemd mutations","plan":make_plan(config,supervisor,timer)}
    elif args.command == "apply-observation": result={"state":"applied","actions":apply_observation(config,supervisor)}
    elif args.command == "apply-guarded": result={"state":"applied","actions":apply_guarded(config,supervisor,timer,args.receipt,args.config)}
    else: result={"state":"applied","actions":rollback(args.receipt)}
    print(json.dumps(result,indent=2,sort_keys=True)); return 0

if __name__ == "__main__": raise SystemExit(main())
