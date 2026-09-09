#!/usr/bin/env python3
"""Portable Control Manager layout, installation, and local bootstrap helpers.

The distributable tree is deliberately separate from machine bindings and
runtime state. This module never reads, exports, or serializes credentials.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path
from typing import Any


PACKAGE_VERSION = 20
STATE_SCHEMA_VERSION = 10


def package_root() -> Path:
    """Return the release-tree root for this installed runtime."""
    configured = os.environ.get("CODEX_CONTROL_ROOT")
    if configured:
        return Path(configured).expanduser().resolve()
    runtime = Path(__file__).resolve().parent
    return runtime.parent if (runtime.parent / "control-package.json").is_file() else runtime


def runtime_root(root: Path | None = None) -> Path:
    root = root or package_root()
    candidate = root / "runtime"
    return candidate if candidate.is_dir() else root


def state_root() -> Path:
    configured = os.environ.get("CODEX_CONTROL_STATE_DIR")
    if configured:
        return Path(configured).expanduser().resolve()
    xdg = Path(os.environ.get("XDG_STATE_HOME", str(Path.home() / ".local/state")))
    return (xdg / "codex-control").resolve()


def machine_config_path() -> Path:
    configured = os.environ.get("CODEX_CONTROL_CONFIG_PATH")
    return Path(configured).expanduser().resolve() if configured else state_root() / "machine.json"


def package_manifest(root: Path) -> dict[str, Any]:
    manifest = root / "control-package.json"
    try:
        value = json.loads(manifest.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"{root} has no valid control-package.json: {exc}") from exc
    if value.get("package_version") != PACKAGE_VERSION:
        raise ValueError("package manifest/version does not match this installer")
    files = value.get("files")
    if not isinstance(files, list) or not files or not all(isinstance(item, str) for item in files):
        raise ValueError("package manifest has no valid file list")
    return value


def bootstrap(root: Path, state: Path, config: Path) -> dict[str, Any]:
    """Write only relocatable package/version and machine-binding metadata."""
    root, state, config = root.resolve(), state.resolve(), config.resolve()
    package = package_manifest(root)
    if not (runtime_root(root) / "control_broker.py").is_file():
        raise ValueError(f"{root} is not a Control Manager package root")
    policy_path = root / str(package.get("shared_policy", ""))
    try:
        policy = json.loads(policy_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"{root} has no valid shared policy: {exc}") from exc
    if policy.get("commit_message_policy") != "autonomous_conventional":
        raise ValueError("shared policy does not enforce autonomous Conventional Commit messages")
    presentation = policy.get("presentation_policy")
    if not isinstance(presentation, dict) or presentation.get("voice_summary_only") is not True:
        raise ValueError("shared policy does not enforce voice-safe completion presentation")
    if config.exists():
        raise ValueError(f"refusing to overwrite existing machine config: {config}")
    state.mkdir(parents=True, exist_ok=True)
    config.parent.mkdir(parents=True, exist_ok=True)
    machine = {
        "package_version": PACKAGE_VERSION,
        "state_schema_version": STATE_SCHEMA_VERSION,
        "control_root": str(root),
        "manager_cwd": str(root),
        "state_dir": str(state),
        "broker_socket": str(state / "control.sock"),
        "database": str(state / "tasks.sqlite3"),
        "herdr_socket": os.environ.get("CODEX_CONTROL_HERDR_SOCKET", ""),
        "app_server_socket": os.environ.get("CODEX_APP_SERVER_SOCKET", ""),
        "codex_binary": os.environ.get("CODEX_BINARY", "codex"),
        "supervisor_socket": str(state / "recovery-supervisor.sock"),
        "supervisor_database": str(state / "recovery.sqlite3"),
        "supervisor_mode": "observation_only",
        "recovery_live_enabled": False,
        "supervisor_owns_recovery": False,
        "policy_file": str(root / str(package.get("policy", "POLICY.md"))),
        "shared_policy_file": str(policy_path),
        "commit_message_policy": policy["commit_message_policy"],
        "presentation_policy": presentation,
    }
    config.write_text(json.dumps(machine, indent=2, sort_keys=True) + "\n")
    os.chmod(config, 0o600)
    return machine


def install(source: Path, target: Path) -> dict[str, Any]:
    """Install manifest-listed package files only; state and secrets never copy."""
    source, target = source.resolve(), target.resolve()
    package = package_manifest(source)
    if target.exists() and any(target.iterdir()):
        raise ValueError(f"refusing to install into non-empty target: {target}")
    target.mkdir(parents=True, exist_ok=True)
    copied: list[str] = []
    for name in package["files"]:
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError(f"unsafe package file name: {name}")
        item = source / relative
        if not item.is_file() and not item.is_dir():
            raise ValueError(f"package is missing required file: {name}")
        destination = target / relative
        if item.is_dir():
            shutil.copytree(item, destination)
        else:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, destination)
        copied.append(name)
    return {"package_version": PACKAGE_VERSION, "source": str(source), "installed_root": str(target), "files": copied}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    boot = sub.add_parser("bootstrap", help="write a machine-local binding config")
    boot.add_argument("--root", type=Path, default=package_root())
    boot.add_argument("--state-dir", type=Path, default=state_root())
    boot.add_argument("--config", type=Path)
    installer = sub.add_parser("install", help="copy the manifest-listed package without state or secrets")
    installer.add_argument("--source", type=Path, default=package_root())
    installer.add_argument("--target", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "bootstrap":
        config = args.config or args.state_dir / "machine.json"
        print(json.dumps(bootstrap(args.root, args.state_dir, config), indent=2, sort_keys=True))
    else:
        print(json.dumps(install(args.source, args.target), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
