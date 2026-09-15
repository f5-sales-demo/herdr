#!/usr/bin/env python3
"""Regression tests for the downstream release workflow contract."""

import re
import unittest
from pathlib import Path


WORKFLOW = (
    Path(__file__).resolve().parents[1]
    / ".github"
    / "workflows"
    / "downstream-release.yml"
)


class DownstreamReleaseWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = WORKFLOW.read_text(encoding="utf-8")

    def matrix_entry(self, target: str) -> str:
        match = re.search(
            rf"(?ms)^          - target: {re.escape(target)}\n"
            r"(?P<entry>(?:            .+\n)+)",
            self.workflow,
        )
        self.assertIsNotNone(match, f"missing release matrix entry for {target}")
        return match.group("entry")

    def test_macos_matrix_uses_canonical_macho_architectures(self) -> None:
        expectations = {
            "aarch64-apple-darwin": "arm64",
            "x86_64-apple-darwin": "x86_64",
        }
        for target, architecture in expectations.items():
            with self.subTest(target=target):
                self.assertIn(f"macho_arch: {architecture}\n", self.matrix_entry(target))

    def test_architecture_verification_survives_tagged_checkout(self) -> None:
        self.assertIn('expected_arch="${{ matrix.macho_arch }}"', self.workflow)
        self.assertIn('actual_arch="$(lipo -archs "${{ matrix.artifact }}")"', self.workflow)
        self.assertNotIn("scripts/macos_arch.py", self.workflow)


if __name__ == "__main__":
    unittest.main()
