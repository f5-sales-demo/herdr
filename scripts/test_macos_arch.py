#!/usr/bin/env python3
"""Tests for Rust target to Mach-O architecture translation."""

import unittest

if __package__:
    from scripts.macos_arch import macho_arch
else:
    from macos_arch import macho_arch


class MacosArchTests(unittest.TestCase):
    def test_aarch64_uses_macho_arm64_name(self) -> None:
        self.assertEqual(macho_arch("aarch64-apple-darwin"), "arm64")

    def test_x86_64_name_is_unchanged(self) -> None:
        self.assertEqual(macho_arch("x86_64-apple-darwin"), "x86_64")

    def test_unknown_target_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "unsupported macOS target"):
            macho_arch("universal2-apple-darwin")


if __name__ == "__main__":
    unittest.main()
