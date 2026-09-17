#!/usr/bin/env python3
"""Regression tests for the trusted shared-runner workflow routing contract."""

from __future__ import annotations

import re
import unittest
from pathlib import Path


WORKFLOWS = Path(__file__).resolve().parents[1] / ".github" / "workflows"
TRUSTED_RUNNER = "managed-socketless"
UNTRUSTED_PULL_REQUEST_RUNNER = "ubuntu-latest"


def job(workflow: str, name: str) -> str:
    content = (WORKFLOWS / workflow).read_text(encoding="utf-8")
    match = re.search(
        rf"(?ms)^  {re.escape(name)}:\n(?P<body>.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)",
        content,
    )
    if match is None:
        raise AssertionError(f"{workflow} does not define the {name} job")
    return match.group("body")


class SharedRunnerWorkflowTests(unittest.TestCase):
    def test_ci_routes_only_trusted_pull_requests_to_arc(self) -> None:
        for name in ("control-manager-package", "conventional-commits"):
            body = job("ci.yml", name)
            self.assertIn(TRUSTED_RUNNER, body)
            self.assertIn("github.event.pull_request.head.repo.full_name", body)
            self.assertIn(UNTRUSTED_PULL_REQUEST_RUNNER, body)

    def test_release_assembly_uses_the_socketless_profile(self) -> None:
        for name in ("control-manager-package", "publish", "publish-manifest"):
            self.assertIn(f"runs-on: {TRUSTED_RUNNER}", job("downstream-release.yml", name))

    def test_trusted_push_housekeeping_uses_the_socketless_profile(self) -> None:
        self.assertIn(f"runs-on: {TRUSTED_RUNNER}", job("label-next-release-issues.yml", "close"))

    def test_privileged_and_platform_specific_jobs_remain_hosted(self) -> None:
        for workflow, name, runner in (
            ("approve-contributor.yml", "approve", "ubuntu-latest"),
            ("approve-merged-contributor.yml", "approve", "ubuntu-latest"),
            ("issue-gate.yml", "check-template", "ubuntu-latest"),
            ("pr-gate.yml", "link-fork-issue", "ubuntu-latest"),
            ("ci.yml", "windows-conpty-package", "windows-2022"),
            ("windows-arm64.yml", "installer", "windows-11-arm"),
        ):
            with self.subTest(workflow=workflow, job=name):
                self.assertIn(f"runs-on: {runner}", job(workflow, name))


if __name__ == "__main__":
    unittest.main()
