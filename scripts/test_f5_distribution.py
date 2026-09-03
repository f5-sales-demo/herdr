import hashlib
import json
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path

from scripts.f5_distribution import build_manifest, package_binary, parse_f5_tag, render_formula


class F5DistributionTests(unittest.TestCase):
    def test_tag_requires_cargo_base_and_maps_revision(self) -> None:
        parsed = parse_f5_tag("v0.7.5-xcsh12", "0.7.5")
        self.assertEqual(parsed.base_version, "0.7.5")
        self.assertEqual(parsed.binary_version, "0.7.5")
        self.assertEqual(parsed.revision, 12)
        with self.assertRaises(ValueError):
            parse_f5_tag("v0.7.4-xcsh1", "0.7.5")
        with self.assertRaises(ValueError):
            parse_f5_tag("v0.7.5", "0.7.5")

    def test_packaging_is_deterministic_and_complete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            binary = root / "herdr"
            binary.write_bytes(b"binary\n")
            binary.chmod(0o755)
            first = package_binary(binary, "linux-x86_64", root / "one")
            second = package_binary(binary, "linux-x86_64", root / "two")
            self.assertEqual(hashlib.sha256(first.archive.read_bytes()).digest(), hashlib.sha256(second.archive.read_bytes()).digest())
            with tarfile.open(first.archive, "r:gz") as archive:
                member = archive.getmember("herdr-linux-x86_64")
                self.assertEqual(member.mtime, 0)
                self.assertEqual(member.mode, 0o755)

    def test_manifest_lists_raw_and_archive_assets_with_checksums(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            artifacts = []
            for platform in ("linux-x86_64", "linux-aarch64", "macos-x86_64", "macos-aarch64"):
                binary = root / f"input-{platform}"
                binary.write_bytes(platform.encode())
                artifacts.append(package_binary(binary, platform, root / platform))
            manifest = build_manifest("v0.7.5-xcsh2", "0.7.5", artifacts, "https://github.com/f5-sales-demo/herdr/releases/download")
            self.assertEqual(manifest["revision"], 2)
            self.assertEqual(set(manifest["platforms"]), {"linux-x86_64", "linux-aarch64", "macos-x86_64", "macos-aarch64"})
            for entry in manifest["platforms"].values():
                self.assertEqual(set(entry), {"binary", "archive"})
                self.assertRegex(entry["binary"]["sha256"], r"^[0-9a-f]{64}$")

    def test_formula_uses_immutable_architecture_urls_and_revision(self) -> None:
        fixture = json.loads((Path(__file__).parent / "fixtures" / "f5-latest.json").read_text())
        formula = render_formula(fixture)
        self.assertIn('version "0.7.5"', formula)
        self.assertIn("revision 3", formula)
        self.assertIn("v0.7.5-xcsh3/herdr-macos-aarch64.tar.gz", formula)
        self.assertIn("v0.7.5-xcsh3/herdr-macos-x86_64.tar.gz", formula)
        self.assertNotIn("cargo install", formula)


if __name__ == "__main__":
    unittest.main()
