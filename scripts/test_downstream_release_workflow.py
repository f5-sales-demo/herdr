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

    def test_only_manual_ci_completion_admits_an_automatic_release(self) -> None:
        prepare_guard = re.search(
            r"(?ms)^  prepare:\n    if: >-\n(?P<guard>.+?)^    runs-on:",
            self.workflow,
        )
        self.assertIsNotNone(prepare_guard)
        guard = prepare_guard.group("guard")
        self.assertIn("github.event.workflow_run.event == 'workflow_dispatch'", guard)
        self.assertNotIn("github.event.workflow_run.event == 'push'", guard)

    def test_existing_tag_recovery_separates_payload_and_tooling_pins(self) -> None:
        self.assertIn(
            "release_sha: ${{ steps.publish.outputs.release_sha || steps.retry.outputs.release_sha }}",
            self.workflow,
        )
        self.assertIn("tooling_sha: ${{ steps.tooling.outputs.tooling_sha }}", self.workflow)
        self.assertIn(
            "TOOLING_SHA: ${{ github.event_name == 'workflow_dispatch' && github.sha || github.event.workflow_run.head_sha }}",
            self.workflow,
        )
        self.assertIn("ref: ${{ needs.prepare.outputs.release_sha }}", self.workflow)
        self.assertIn("ref: ${{ needs.prepare.outputs.tooling_sha }}", self.workflow)
        self.assertIn(
            'run: test "$(git -C herdr-source rev-parse HEAD)" = "$TOOLING_SHA"',
            self.workflow,
        )


if __name__ == "__main__":
    unittest.main()
