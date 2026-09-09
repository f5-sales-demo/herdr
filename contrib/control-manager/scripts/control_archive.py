#!/usr/bin/env python3
"""Build, verify, and test deterministic Control Manager release archives."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Any


FORBIDDEN_NAMES = {"config.json", "machine.json", ".env"}
FORBIDDEN_SUFFIXES = (".sqlite", ".sqlite3", ".sock", ".pyc")
FORBIDDEN_TEXT = (b"/home/" + b"robin", b"gh" + b"o_", b"github" + b"_pat_", b"BEGIN " + b"PRIVATE KEY")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_manifest(root: Path) -> dict[str, Any]:
    try:
        manifest = json.loads((root / "control-package.json").read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid package manifest: {exc}") from exc
    if manifest.get("name") != "control-manager" or not isinstance(manifest.get("files"), list):
        raise ValueError("package manifest must name control-manager and enumerate files")
    return manifest


def listed_paths(root: Path, manifest: dict[str, Any]) -> list[Path]:
    paths: list[Path] = []
    for name in manifest["files"]:
        relative = Path(name)
        if not isinstance(name, str) or relative.is_absolute() or ".." in relative.parts:
            raise ValueError(f"unsafe package manifest path: {name!r}")
        path = root / relative
        if not path.exists():
            raise ValueError(f"manifest path is missing: {name}")
        paths.append(path)
    return paths


def validate_tree(root: Path) -> dict[str, Any]:
    root = root.resolve()
    manifest = read_manifest(root)
    listed = listed_paths(root, manifest)
    files: list[Path] = []
    for path in listed:
        candidates = sorted(path.rglob("*")) if path.is_dir() else [path]
        files.extend(
            candidate for candidate in candidates
            if candidate.is_file() and candidate.suffix != ".pyc" and "__pycache__" not in candidate.parts
        )
    for path in files:
        relative = path.relative_to(root)
        if path.name in FORBIDDEN_NAMES or path.name.endswith(FORBIDDEN_SUFFIXES):
            raise ValueError(f"package contains machine-local runtime file: {relative}")
        data = path.read_bytes()
        if any(marker in data for marker in FORBIDDEN_TEXT):
            raise ValueError(f"package contains a prohibited personal path or credential marker: {relative}")
    return {"manifest": manifest, "files": files}


def archive_members(root: Path, top: str, files: list[Path]) -> list[tuple[Path, str]]:
    return [(path, f"{top}/{path.relative_to(root).as_posix()}") for path in sorted(files)]


def build(args: argparse.Namespace) -> None:
    root = args.source_root.resolve()
    checked = validate_tree(root)
    version = args.version.removeprefix("v")
    if not version:
        raise ValueError("version is required")
    top = f"control-manager-{version}"
    args.output_dir.mkdir(parents=True, exist_ok=True)
    archive = args.output_dir / f"{top}.tar.gz"
    members = archive_members(root, top, checked["files"])
    with archive.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as zipped:
            with tarfile.open(mode="w", fileobj=zipped, format=tarfile.PAX_FORMAT) as tar:
                for path, name in members:
                    info = tar.gettarinfo(str(path), arcname=name)
                    info.uid = info.gid = 0
                    info.uname = info.gname = ""
                    info.mtime = 0
                    with path.open("rb") as source:
                        tar.addfile(info, source)
    digest = sha256(archive)
    checksum = archive.with_suffix(archive.suffix + ".sha256")
    checksum.write_text(f"{digest}  {archive.name}\n")
    provenance = args.output_dir / f"{top}.provenance.json"
    provenance.write_text(json.dumps({
        "artifact": archive.name,
        "artifact_sha256": digest,
        "package_name": checked["manifest"]["name"],
        "package_version": checked["manifest"]["package_version"],
        "release_tag": args.release_tag,
        "source_sha": args.source_sha,
        "manifest_sha256": sha256(root / "control-package.json"),
        "file_count": len(members),
    }, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"archive": str(archive), "sha256": digest, "provenance": str(provenance)}, sort_keys=True))


def verify(args: argparse.Namespace) -> None:
    expected = args.checksum.read_text().strip().split()
    if len(expected) != 2 or expected[1] != args.archive.name or expected[0] != sha256(args.archive):
        raise ValueError("archive checksum sidecar does not verify")
    provenance = json.loads(args.provenance.read_text())
    if provenance.get("artifact") != args.archive.name or provenance.get("artifact_sha256") != expected[0]:
        raise ValueError("provenance does not bind this archive digest")
    if args.expected_tag and provenance.get("release_tag") != args.expected_tag:
        raise ValueError("provenance release tag does not match expected tag")
    if args.expected_source_sha and provenance.get("source_sha") != args.expected_source_sha:
        raise ValueError("provenance source SHA does not match expected source SHA")
    with tempfile.TemporaryDirectory() as raw:
        destination = Path(raw)
        with tarfile.open(args.archive, "r:gz") as tar:
            names = tar.getnames()
            if any(name.startswith("/") or ".." in Path(name).parts for name in names):
                raise ValueError("archive contains unsafe member path")
            tar.extractall(destination, filter="data")
        roots = [path for path in destination.iterdir() if path.is_dir()]
        if len(roots) != 1:
            raise ValueError("archive must contain exactly one package root")
        checked = validate_tree(roots[0])
        if sha256(roots[0] / "control-package.json") != provenance.get("manifest_sha256"):
            raise ValueError("provenance manifest digest does not verify")
    print(json.dumps({"verified": True, "archive": args.archive.name, "files": len(checked["files"])}, sort_keys=True))


def attest(args: argparse.Namespace) -> None:
    """Verify an archive, then prove its installer/bootstrap remain relocatable."""
    verify(args)
    with tempfile.TemporaryDirectory() as raw:
        destination = Path(raw)
        with tarfile.open(args.archive, "r:gz") as tar:
            tar.extractall(destination, filter="data")
        source = next(path for path in destination.iterdir() if path.is_dir())
        installed = destination / "installed"
        state = destination / "state"
        installer = source / "runtime" / "control_portable.py"
        environment = dict(os.environ, CODEX_CONTROL_ROOT=str(installed), CODEX_CONTROL_STATE_DIR=str(state))
        subprocess.run(
            [sys.executable, str(installer), "install", "--source", str(source), "--target", str(installed)],
            env=environment, check=True, stdout=subprocess.PIPE, text=True,
        )
        subprocess.run(
            [sys.executable, str(installed / "runtime" / "control_portable.py"), "bootstrap"],
            env=environment, check=True, stdout=subprocess.PIPE, text=True,
        )
        config = json.loads((state / "machine.json").read_text())
        if config.get("control_root") != str(installed) or config.get("state_dir") != str(state):
            raise ValueError("installed bootstrap did not preserve the new package/state roots")
    print(json.dumps({"attested": True, "archive": args.archive.name}, sort_keys=True))


def test(args: argparse.Namespace) -> None:
    root = args.source_root.resolve()
    validate_tree(root)
    environment = dict(os.environ, PYTHONPATH=str(root / "runtime"), PYTHONDONTWRITEBYTECODE="1")
    command = [sys.executable, "-m", "unittest", "discover", "-s", str(root / "tests"), "-p", "test_*.py", "-v"]
    completed = subprocess.run(command, env=environment, check=False)
    if completed.returncode:
        raise SystemExit(completed.returncode)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    build_parser = sub.add_parser("build")
    build_parser.add_argument("--source-root", type=Path, required=True)
    build_parser.add_argument("--output-dir", type=Path, required=True)
    build_parser.add_argument("--version", required=True)
    build_parser.add_argument("--release-tag", required=True)
    build_parser.add_argument("--source-sha", required=True)
    verify_parser = sub.add_parser("verify")
    verify_parser.add_argument("--archive", type=Path, required=True)
    verify_parser.add_argument("--checksum", type=Path, required=True)
    verify_parser.add_argument("--provenance", type=Path, required=True)
    verify_parser.add_argument("--expected-tag")
    verify_parser.add_argument("--expected-source-sha")
    attest_parser = sub.add_parser("attest")
    attest_parser.add_argument("--archive", type=Path, required=True)
    attest_parser.add_argument("--checksum", type=Path, required=True)
    attest_parser.add_argument("--provenance", type=Path, required=True)
    attest_parser.add_argument("--expected-tag")
    attest_parser.add_argument("--expected-source-sha")
    test_parser = sub.add_parser("test")
    test_parser.add_argument("--source-root", type=Path, required=True)
    args = parser.parse_args()
    {"build": build, "verify": verify, "attest": attest, "test": test}[args.command](args)


if __name__ == "__main__":
    main()
