import unittest
from pathlib import Path


class ReleaseWorkflowContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workflow = (Path(__file__).parents[1] / ".github" / "workflows" / "release.yml").read_text()

    def test_f5_tag_and_native_macos_runners(self) -> None:
        self.assertIn('"v*-xcsh*"', self.workflow)
        self.assertIn("macos-15-intel", self.workflow)
        self.assertIn("macos-15", self.workflow)

    def test_ephemeral_signing_and_notarization_are_hard_gates(self) -> None:
        for name in ("APPLE_CERTIFICATE_BASE64", "APPLE_CERTIFICATE_PASSWORD", "APPLE_ID", "APPLE_PASSWORD", "APPLE_TEAM_ID"):
            self.assertIn(name, self.workflow)
        self.assertIn("security create-keychain", self.workflow)
        self.assertIn("codesign --force --options runtime --timestamp", self.workflow)
        self.assertIn("xcrun notarytool submit", self.workflow)
        self.assertIn("status: Accepted", self.workflow)
        self.assertIn("spctl --assess", self.workflow)

    def test_verification_precedes_artifact_upload_and_release(self) -> None:
        macos = self.workflow.index("  build-macos:")
        verify = self.workflow.index("Verify signed binary", macos)
        upload = self.workflow.index("Upload verified binary", macos)
        release = self.workflow.index("Create F5 release")
        self.assertLess(verify, upload)
        self.assertLess(upload, release)
        self.assertIn("needs: [build-linux, build-macos]", self.workflow)


if __name__ == "__main__":
    unittest.main()
