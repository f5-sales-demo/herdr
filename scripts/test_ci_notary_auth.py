"""Credential selection, Apple validation and temporary-key privacy contract."""

import base64
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(os.name == "posix", "macOS notarization helper requires POSIX Bash")
class NotaryAuthTests(unittest.TestCase):
    def invoke(self, credentials, reject=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            mock = root / "xcrun"
            mock.write_text("""#!/usr/bin/env python3
import json,os,pathlib,sys
args=sys.argv[1:]
record={"args":args}
if "--key" in args:
 p=pathlib.Path(args[args.index("--key")+1]);record.update(keyPath=str(p),mode=p.stat().st_mode & 0o777,key=p.read_text())
pathlib.Path(os.environ["NOTARY_TEST_RECORD"]).write_text(json.dumps(record))
sys.exit(int(os.environ["NOTARY_TEST_REJECT"]))
""")
            mock.chmod(0o755)
            environment = {
                k: v for k, v in os.environ.items() if not k.startswith("APPLE_")
            }
            environment.update(credentials)
            environment.update(
                PATH=str(root) + ":" + os.environ["PATH"],
                TMPDIR=str(root),
                NOTARY_TEST_RECORD=str(root / "record.json"),
                NOTARY_TEST_REJECT="1" if reject else "0",
            )
            result = subprocess.run(  # noqa: S603 -- fixed trusted helper and task-owned mock directory
                [
                    "/bin/bash",
                    str(ROOT / "scripts/ci-notary-auth.sh"),
                    str(root / "signing.keychain-db"),
                    "test-profile",
                ],
                env=environment,
                capture_output=True,
                text=True,
                check=False,
            )
            record = (
                json.loads((root / "record.json").read_text())
                if (root / "record.json").exists()
                else None
            )
            leftovers = list(root.glob("herdr-notary-key.*"))
            assert not leftovers, "Private key must be removed on every exit"
            for value in [
                "FAKE_PRIVATE_KEY",
                "FAKE_APP_PASSWORD",
                base64.b64encode(b"FAKE_PRIVATE_KEY").decode(),
            ]:
                assert value not in result.stdout + result.stderr
            return result, record

    def api(self):
        return {
            "APPLE_NOTARY_KEY_BASE64": base64.b64encode(b"FAKE_PRIVATE_KEY").decode(),
            "APPLE_NOTARY_KEY_ID": "FAKE_KEY_ID",
            "APPLE_NOTARY_ISSUER": "FAKE_ISSUER",
        }

    def legacy(self):
        return {
            "APPLE_ID": "fake@example.invalid",
            "APPLE_PASSWORD": "FAKE_APP_PASSWORD",
            "APPLE_TEAM_ID": "FAKE_TEAM",
        }

    def test_api_precedes_legacy_and_validates_private_key(self):
        result, record = self.invoke({**self.legacy(), **self.api()})
        assert result.returncode == 0, result.stderr
        assert record["key"] == "FAKE_PRIVATE_KEY"
        assert record["mode"] == 0o600
        assert record["args"][:3] == ["notarytool", "store-credentials", "test-profile"]
        assert "--keychain" in record["args"]
        assert "--issuer" in record["args"]
        assert "--apple-id" not in record["args"]
        assert "--no-validate" not in record["args"]

    def test_individual_key_omits_issuer(self):
        credentials = self.api()
        credentials.pop("APPLE_NOTARY_ISSUER")
        result, record = self.invoke(credentials)
        assert result.returncode == 0, result.stderr
        assert "--issuer" not in record["args"]

    def test_legacy_fallback_is_validated(self):
        result, record = self.invoke(self.legacy())
        assert result.returncode == 0, result.stderr
        assert "--apple-id" in record["args"]
        assert "--password" in record["args"]
        assert "--team-id" in record["args"]
        assert "--key" not in record["args"]
        assert "--no-validate" not in record["args"]

    def test_partial_api_never_falls_back(self):
        for name, value in self.api().items():
            with self.subTest(name=name):
                result, record = self.invoke({**self.legacy(), name: value})
                assert result.returncode != 0
                assert record is None

    def test_missing_credentials_fail_before_apple(self):
        result, record = self.invoke({})
        assert result.returncode != 0
        assert record is None

    def test_partial_legacy_fails_before_apple(self):
        result, record = self.invoke({"APPLE_ID": "fake@example.invalid"})
        assert result.returncode != 0
        assert record is None

    def test_rejected_api_validation_cleans_key(self):
        result, record = self.invoke(self.api(), reject=True)
        assert result.returncode != 0
        assert record is not None

    def test_rejected_legacy_validation_fails(self):
        result, record = self.invoke(self.legacy(), reject=True)
        assert result.returncode != 0
        assert record is not None

    def test_invalid_base64_fails_before_apple(self):
        credentials = self.api()
        credentials["APPLE_NOTARY_KEY_BASE64"] = "invalid!"
        result, record = self.invoke(credentials)
        assert result.returncode != 0
        assert record is None


if __name__ == "__main__":
    unittest.main()
