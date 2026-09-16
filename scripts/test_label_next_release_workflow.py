#!/usr/bin/env python3
"""Regression tests for the pending-release issue workflow."""

import unittest
from pathlib import Path


WORKFLOW = (
    Path(__file__).resolve().parents[1]
    / ".github"
    / "workflows"
    / "label-next-release-issues.yml"
)


class LabelNextReleaseWorkflowTests(unittest.TestCase):
    def test_same_repository_issue_writes_use_the_workflow_token(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")

        self.assertIn("issues: write", workflow)
        self.assertIn("GH_TOKEN: ${{ github.token }}", workflow)
        self.assertNotIn("KANGAL_GITHUB_TOKEN", workflow)


if __name__ == "__main__":
    unittest.main()
