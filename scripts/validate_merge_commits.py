#!/usr/bin/env python3
"""Validate merge subjects without exempting direct merges from CI policy."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.request
from collections.abc import Callable
from pathlib import Path

try:
    from .conventional_commits import valid_subject
except ImportError:  # Direct script invocation keeps scripts/ on sys.path.
    from conventional_commits import valid_subject


def merge_subjects(rev_range: str, *, cwd: Path | None = None) -> list[tuple[str, str]]:
    output = subprocess.check_output(
        ["git", "log", "--merges", "--pretty=format:%H%x1f%s", rev_range], text=True, cwd=cwd
    ).strip()
    return [tuple(line.split("\x1f", maxsplit=1)) for line in output.splitlines() if "\x1f" in line]


def github_pr_evidence(repository: str, sha: str, token: str) -> list[dict[str, object]]:
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repository}/commits/{sha}/pulls",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(request) as response:  # noqa: S310 -- fixed GitHub API origin
        return json.load(response)


def is_recorded_pr_merge(
    subject: str,
    sha: str,
    repository: str,
    base_ref: str,
    token: str,
    evidence: Callable[[str, str, str], list[dict[str, object]]] = github_pr_evidence,
) -> bool:
    for pull in evidence(repository, sha, token):
        base = pull.get("base")
        if not isinstance(base, dict):
            continue
        if (
            pull.get("state") == "closed"
            and pull.get("merged_at")
            and pull.get("merge_commit_sha") == sha
            and base.get("ref") == base_ref
            and valid_subject(str(pull.get("title", "")))
        ):
            return True
    return False


def invalid_merge_subjects(
    rev_range: str,
    repository: str,
    base_ref: str,
    token: str,
    evidence: Callable[[str, str, str], list[dict[str, object]]] = github_pr_evidence,
    cwd: Path | None = None,
) -> list[tuple[str, str]]:
    return [
        (sha, subject)
        for sha, subject in merge_subjects(rev_range, cwd=cwd)
        if not valid_subject(subject) and not is_recorded_pr_merge(subject, sha, repository, base_ref, token, evidence)
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--range", required=True, dest="rev_range")
    parser.add_argument("--repository", required=True)
    parser.add_argument("--base-ref", required=True)
    parser.add_argument("--token-env", default="GITHUB_TOKEN")
    args = parser.parse_args()
    token = os.environ.get(args.token_env)
    if not token:
        parser.error(f"{args.token_env} is required to verify GitHub PR merge provenance")

    invalid = invalid_merge_subjects(args.rev_range, args.repository, args.base_ref, token)
    if invalid:
        print("invalid merge subject(s) without recorded conventional PR provenance:")
        for sha, subject in invalid:
            print(f"  {sha[:12]} {subject}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
