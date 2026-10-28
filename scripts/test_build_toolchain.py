"""Exercise the actual native build resolver on Unix and Windows."""
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]

class BuildToolchainTests(unittest.TestCase):
    def test_resolver(self):
        with tempfile.TemporaryDirectory(prefix="herdr-zig-test-") as directory:
            binary = Path(directory) / "resolver-test.exe"
            subprocess.run(["rustc", "--edition", "2024", "--test",
                            str(ROOT / "crates/ghostty-vt/build.rs"), "-o", str(binary)], check=True)
            subprocess.run([str(binary)], check=True)
