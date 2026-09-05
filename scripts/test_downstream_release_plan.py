#!/usr/bin/env python3
"""Focused tests for the fork-owned conventional-commit release planner."""

import tempfile
import unittest
from pathlib import Path

from downstream_release_plan import bump, release_level, replace_lockfile_version, replace_package_version


class ReleasePlanTests(unittest.TestCase):
    def test_patch_for_non_feature_conventional_commits(self) -> None:
        self.assertEqual(release_level(["fix: repair lifecycle\n", "docs: clarify runbook\n"]), "patch")
        self.assertEqual(bump("0.7.5", "patch"), "0.7.6")

    def test_feature_is_a_minor_release(self) -> None:
        self.assertEqual(release_level(["fix: repair lifecycle", "feat(queue): admit prompts"]), "minor")
        self.assertEqual(bump("0.7.5", "minor"), "0.8.0")

    def test_breaking_change_is_a_major_release(self) -> None:
        self.assertEqual(release_level(["feat(api)!: replace protocol"]), "major")
        self.assertEqual(release_level(["feat: replace protocol\n\nBREAKING CHANGE: old clients stop working"]), "major")
        self.assertEqual(bump("0.7.5", "major"), "1.0.0")

    def test_non_conventional_commit_does_not_release(self) -> None:
        self.assertIsNone(release_level(["miscellaneous cleanup"]))

    def test_automation_only_commits_do_not_release(self) -> None:
        self.assertIsNone(release_level([
            "ci: update fork workflow",
            "test: cover release planner",
            "docs: clarify runbook",
            "fix(release): repair build policy",
        ]))

    def test_version_write_updates_only_the_root_package_entries(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "Cargo.toml"
            lockfile = root / "Cargo.lock"
            manifest.write_text('[package]\nname = "herdr"\nversion = "0.7.5"\n', encoding="utf-8")
            lockfile.write_text(
                '[[package]]\nname = "other"\nversion = "1.0.0"\n\n[[package]]\nname = "herdr"\nversion = "0.7.5"\n',
                encoding="utf-8",
            )
            replace_package_version(manifest, "0.8.0")
            replace_lockfile_version(lockfile, "0.8.0")
            self.assertIn('version = "0.8.0"', manifest.read_text(encoding="utf-8"))
            self.assertEqual(lockfile.read_text(encoding="utf-8").count('version = "0.8.0"'), 1)


if __name__ == "__main__":
    unittest.main()
