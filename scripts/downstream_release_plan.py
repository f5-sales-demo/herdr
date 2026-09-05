#!/usr/bin/env python3
"""Derive a downstream SemVer release from conventional commits.

This intentionally has no knowledge of the upstream release process.  The
fork-owned workflow calls it after CI succeeds on build-xcsh.
"""

from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path

HEADER = re.compile(
    r"^(?P<kind>feat|fix|perf|docs|ci|test|refactor|chore|release)"
    r"(?:\([^)]+\))?(?P<breaking>!)?:\s+\S",
    re.MULTILINE,
)
VERSION = re.compile(r"^(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)$")


def release_level(messages: list[str]) -> str | None:
    """Return the highest SemVer level justified by conventional commits."""
    level: str | None = None
    for message in messages:
        match = HEADER.search(message)
        if not match:
            continue
        if match.group("breaking") or re.search(r"^BREAKING[ -]CHANGE:", message, re.MULTILINE | re.IGNORECASE):
            return "major"
        if match.group("kind") == "feat":
            level = "minor"
        elif level is None:
            level = "patch"
    return level


def bump(version: str, level: str) -> str:
    match = VERSION.fullmatch(version)
    if not match:
        raise ValueError(f"expected a stable SemVer version, got {version!r}")
    major, minor, patch = (int(match.group(name)) for name in ("major", "minor", "patch"))
    if level == "major":
        return f"{major + 1}.0.0"
    if level == "minor":
        return f"{major}.{minor + 1}.0"
    if level == "patch":
        return f"{major}.{minor}.{patch + 1}"
    raise ValueError(f"unknown release level {level!r}")


def messages_from_git(base: str, head: str) -> list[str]:
    result = subprocess.run(
        ["git", "log", "--format=%B%x1e", f"{base}..{head}"],
        check=True,
        capture_output=True,
        text=True,
    )
    return [message for message in result.stdout.split("\x1e") if message.strip()]


def replace_package_version(path: Path, version: str) -> None:
    content = path.read_text(encoding="utf-8")
    updated, count = re.subn(
        r'(?m)^(version\s*=\s*")[^"]+("\s*)$',
        rf"\g<1>{version}\g<2>",
        content,
        count=1,
    )
    if count != 1:
        raise ValueError(f"could not update package version in {path}")
    path.write_text(updated, encoding="utf-8")


def replace_lockfile_version(path: Path, version: str) -> None:
    content = path.read_text(encoding="utf-8")
    updated, count = re.subn(
        r'(?ms)(\[\[package\]\]\nname = "herdr"\nversion = ")[^"]+("\n)',
        rf"\g<1>{version}\g<2>",
        content,
        count=1,
    )
    if count != 1:
        raise ValueError(f"could not update Herdr package version in {path}")
    path.write_text(updated, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", help="inclusive base tag or commit for the commit range")
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--current", help="current stable Cargo package version")
    parser.add_argument("--write-version", metavar="VERSION", help="update Cargo.toml after a release plan is approved")
    args = parser.parse_args()

    if args.write_version:
        replace_package_version(Path("Cargo.toml"), args.write_version)
        replace_lockfile_version(Path("Cargo.lock"), args.write_version)
        return 0
    if not args.base or not args.current:
        parser.error("--base and --current are required unless --write-version is used")
    level = release_level(messages_from_git(args.base, args.head))
    if level is None:
        print("release=false")
        return 0
    print("release=true")
    print(f"level={level}")
    print(f"version={bump(args.current, level)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
