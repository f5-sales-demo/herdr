#!/usr/bin/env python3
"""Tests for the downstream prebuilt-binary Homebrew formula renderer."""

import tempfile
import unittest
from pathlib import Path

if __package__:
    from scripts.render_downstream_homebrew_formula import TARGETS, load_checksums, render_cask, render_formula
else:
    from render_downstream_homebrew_formula import TARGETS, load_checksums, render_cask, render_formula


class RenderDownstreamHomebrewFormulaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.checksums = {target: f"{index:064x}" for index, target in enumerate(TARGETS, start=1)}

    def test_formula_selects_one_immutable_asset_per_os_and_architecture(self) -> None:
        formula = render_formula("0.15.6", self.checksums)

        self.assertIn('version "0.15.6"', formula)
        self.assertEqual(formula.count("using: :nounzip"), 4)
        self.assertIn("on_macos do", formula)
        self.assertIn("on_linux do", formula)
        self.assertNotIn("depends_on", formula)
        for target, checksum in self.checksums.items():
            if target.endswith(".pkg"):
                continue
            self.assertIn(f"releases/download/v#{{version}}/herdr-{target}", formula)
            self.assertIn(f'sha256 "{checksum}"', formula)

    def test_cask_selects_the_stapled_installer_for_each_mac_architecture(self) -> None:
        cask = render_cask("0.15.6", self.checksums)

        self.assertIn('arch arm: "aarch64", intel: "x86_64"', cask)
        self.assertIn('pkg "herdr-macos-#{arch}.pkg"', cask)
        self.assertIn('pkgutil: "com.f5.herdr"', cask)
        self.assertIn(self.checksums["macos-aarch64.pkg"], cask)
        self.assertIn(self.checksums["macos-x86_64.pkg"], cask)
        self.assertIn("releases/download/v#{version}/herdr-macos-#{arch}.pkg", cask)

    def test_checksum_files_must_name_the_expected_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for target, checksum in self.checksums.items():
                artifact = f"herdr-{target}"
                (root / f"{artifact}.sha256").write_text(f"{checksum}  {artifact}\n", encoding="utf-8")
            self.assertEqual(load_checksums(root), self.checksums)

            bad_artifact = "herdr-macos-aarch64"
            (root / f"{bad_artifact}.sha256").write_text(
                f"{self.checksums['macos-aarch64']}  another-file\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(ValueError, "invalid checksum file"):
                load_checksums(root)

    def test_invalid_version_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "stable SemVer"):
            render_formula("v0.15.6", self.checksums)
        with self.assertRaisesRegex(ValueError, "stable SemVer"):
            render_cask("v0.15.6", self.checksums)


if __name__ == "__main__":
    unittest.main()
